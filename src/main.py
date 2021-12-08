############################################################################################
# Setup code
############################################################################################

import os
import tweepy
import requests
import time

TWITTER_CLIENT_RAW = tweepy.Client(bearer_token=os.environ['BEARER_TOKEN'], consumer_key=os.environ['API_KEY'], consumer_secret=os.environ['API_KEY_SECRET'], return_type=requests.Response)

TWEET_FIELDS = ["author_id", "created_at", "entities", "in_reply_to_user_id", "public_metrics", "referenced_tweets"]
USER_FIELDS = ["username", "id"]

from google.cloud import firestore

# The `project` parameter is optional and represents which project the client
# will act on behalf of. If not supplied, the client falls back to the default
# project inferred from the environment.
FIRESTORE_DB = firestore.Client(project='web3twitterdata')

from google.cloud import bigquery

# Construct a BigQuery client object.
BIGQUERY_CLIENT = bigquery.Client(project='web3twitterdata')

# Configure logging so that it goes into GCP
# Imports the Cloud Logging client library
import google.cloud.logging

# Instantiates a client
LOGGING_CLIENT = google.cloud.logging.Client()

# Retrieves a Cloud Logging handler based on the environment
# you're running in and integrates the handler with the
# Python logging module. By default this captures all logs
# at INFO level and higher
LOGGING_CLIENT.setup_logging()

# Now we can use standard Python logging
import logging

##############################################################################################
# End of setup code
##############################################################################################

def get_influencer_watermarks():
    """
    Query BigQuery to get the latest seen tweet ids and liked tweets for each influencer.

    Returns dataframe with user_id, username, latest_tweet_id, latest_like_at fields
    """
    query = """
    WITH influencers AS (
        SELECT id, username
        FROM TwitterData.users
        WHERE is_influencer
    ),
    tweet_watermarks AS (
        SELECT
            author_id,
            MAX(id) AS latest_tweet_id
        FROM TwitterData.tweets
        WHERE is_by_influencer
        GROUP BY author_id
    ),
    like_watermarks AS (
        SELECT
            liked_by_user_id,
            MAX(tweet_created_at) AS latest_like_at
        FROM TwitterData.likes
        GROUP BY liked_by_user_id
    )
    SELECT
        users.id AS user_id,
        users.username,
        COALESCE(latest_tweet_id, 1) AS  latest_tweet_id,
        COALESCE(FORMAT_TIMESTAMP('%FT%X.000Z', latest_like_at), '2001-01-01') AS latest_like_at
    FROM influencers AS users
    LEFT JOIN tweet_watermarks
    ON users.id = tweet_watermarks.author_id
    LEFT JOIN like_watermarks
    ON users.id = like_watermarks.liked_by_user_id
    """
    return BIGQUERY_CLIENT.query(query).to_dataframe()

def compute_influencer_watermarks(event, context):
    """
    Compute influencer watermarks and upload them to Firestore "influencer_watermarks" collection

    Background Cloud Function to be triggered by Pub/Sub.
    """
    logging.info("compute_influencer_watermarks called.")

    df = get_influencer_watermarks()
    logging.info("Successfully obtained influencer watermarks. Saving to Firestore...")
    for _, row in df.iterrows():
        data_ref = FIRESTORE_DB.collection(u"influencer_watermarks").document(str(row.user_id))
        data_ref.set(row.to_dict())
    logging.info("Successfully uploaded watermarks to Firestore. Exiting now....")
