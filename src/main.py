############################################################################################
# Setup code
############################################################################################

import os
import tweepy
import requests
import datetime
import time
import pandas as pd
import numpy as np
import json
from raw_paginator import RawPaginator

if 'BEARER_TOKEN' in os.environ:
    TWITTER_CLIENT_RAW = tweepy.Client(bearer_token=os.environ['BEARER_TOKEN'], consumer_key=os.environ['API_KEY'], consumer_secret=os.environ['API_KEY_SECRET'], return_type=requests.Response)

    TWEET_FIELDS = ["author_id", "created_at", "entities", "in_reply_to_user_id", "public_metrics", "referenced_tweets"]
    USER_FIELDS = ["username", "id"]
else:
    TWITTER_CLIENT_RAW = None

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

# Standard response headers for HTTP Cloud functions
RESPONSE_HEADERS = {"Content-Type": "application/json"}

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

def compute_influencer_watermarks(request):
    """
    Compute influencer watermarks and upload them to Firestore "influencer_watermarks" collection

    HTTP Cloud Function. Responds both to GET and POST with json:
    {
        "watermarks": [
            {
                "user_id": 123,
                "username": "test",
                "latest_tweet_id": 456,
                "latest_like_at": "2021-12-09T12:34:00.000Z"
            },
            ....
        ]
    }
    """
    logging.info("compute_influencer_watermarks called.")

    df = get_influencer_watermarks()
    logging.info("Successfully obtained influencer watermarks. Saving to Firestore...")
    watermarks = []
    for _, row in df.iterrows():
        data_ref = FIRESTORE_DB.collection(u"influencer_watermarks").document(str(row.user_id))
        data_ref.set(row.to_dict())
        watermarks.append(row.to_dict())
    logging.info("Successfully uploaded watermarks to Firestore. Exiting now....")
    return (json.dumps({"watermarks": watermarks}), 200, RESPONSE_HEADERS)

def set_fetched_at_field(tweets):
    """
    Sets fetched-at field for all tweets in the list.

    The list is modified inplace.
    Fetched at is set to the current time, which is fine for our purposes.
    Returns the original list of tweets.
    """
    fetched_at = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())
    for tweet in tweets:
        tweet["fetched_at"] = fetched_at
    return tweets

def get_tweets_by_ids(tweet_ids):
    """
    Queries Twitter API for tweets info by their ids.

    Returns a list of tweet dictionaries. Fields in the dictionaries match TWEET_FIELDS constant
    """
    if len(tweet_ids) == 0:
        return []

    BATCH_SIZE = 100
    tweets = []
    for n in range(int((len(tweet_ids) - 1)/BATCH_SIZE) + 1):
        batch = tweet_ids[n*BATCH_SIZE:min(len(tweet_ids), (n+1)*BATCH_SIZE)]
        response = TWITTER_CLIENT_RAW.get_tweets(ids=batch, tweet_fields=TWEET_FIELDS)
        response_json = response.json()
        if "data" in response_json:
            tweets.extend(set_fetched_at_field(response_json["data"]))
        else:
            logging.warning("No data returned for request %s", response.request.url)
        time.sleep(3) # to adhere to 300 req/15 minutes rate limit
        if (n+1) % 10 == 0:
            logging.info("Fetched %d tweets", (n+1)*BATCH_SIZE)
    time.sleep(3)
    return tweets

def add_liked_by_user_id_field(likes, user_id):
    """
    Sets "liked_by_user_id" field to user_id for likes in the list.

    Returns the original list
    """
    for like in likes:
        like["liked_by_user_id"] = user_id
    return likes

def get_tweets_and_likes_for_user(user_id, latest_seen_tweet_id, latest_seen_like_timestamp):
    """
    Queries Tweeter API for tweets and likes for a given user. Queries for referenced and liked tweets as well.

    Returns a dictionary:
    {
        "tweets": [list of retrieved tweets],
        "likes": [list of retrieved likes]
    }
    """
    tweets = []
    # let's download user's tweets
    logging.info("Fetching tweets...")
    for response in RawPaginator(TWITTER_CLIENT_RAW.get_users_tweets, user_id, max_results=100, limit=3200, tweet_fields=TWEET_FIELDS, since_id=latest_seen_tweet_id):
        response_json = response.json()
        if "data" in response_json:
            tweets.extend(set_fetched_at_field(response_json["data"]))
        else:
            logging.warning("No data returned for request %s", response.request.url)
        time.sleep(0.8)
    logging.info("Fetched %d tweets", len(tweets))

    # now let's download likes
    likes = []
    logging.info("Fetching likes...")
    for response in RawPaginator(TWITTER_CLIENT_RAW.get_liked_tweets, user_id, max_results=100, limit=7500, tweet_fields=["id", "created_at"]):
        response_json = response.json()
        if "data" in response_json:
            likes.extend(add_liked_by_user_id_field(response_json["data"], user_id))
            # We should stop querying if we see liked tweet that is too old
            time_to_break = False
            for tweet in response_json["data"]:
                if tweet["created_at"] <= latest_seen_like_timestamp:
                    time_to_break = True
                    break
            if time_to_break:
                break
        else:
            logging.warning("No data returned for request %s", response.request.url)
        time.sleep(12) # to meet 75 requests per 15 minutes limit
    time.sleep(12) # To make sure successive calls respect rate limit
    logging.info("Fetched %d likes", len(likes))

    # Collect all the liked and referenced tweet ids and query the information about them
    referenced_and_liked_tweet_ids = set()
    influencer_tweet_ids = set([tweet["id"] for tweet in tweets])

    for tweet in tweets:
        if "referenced_tweets" in tweet:
            for t in tweet["referenced_tweets"]:
                referenced_and_liked_tweet_ids.add(t["id"])

    for like in likes:
        referenced_and_liked_tweet_ids.add(like["id"])

    tweet_ids_to_fetch = list(referenced_and_liked_tweet_ids - influencer_tweet_ids)

    logging.info("Fetching %d referenced and liked tweets ...", len(tweet_ids_to_fetch))
    tweets.extend(get_tweets_by_ids(tweet_ids_to_fetch))

    return {
        "tweets": tweets,
        "likes": likes,
    }

def store_tweets_in_firestore(tweets):
    """
    Saves an array of tweets to Firestore db "tweets" collection.

    If a tweet already exists in the collection, it's overwritten, which is fine because we'll get fresher engagement metrics
    """
    for tweet in tweets:
        data_ref = FIRESTORE_DB.collection(u"tweets").document(tweet["id"])
        data_ref.set(tweet)

def store_likes_in_firestore(likes):
    """
    Saves an array of liked tweets to Firestore db "likes" collection.

    We use tweet_id + liked_by_user as a key
    """
    for like in likes:
        data_ref = FIRESTORE_DB.collection(u"likes").document(like["id"] + "|" + str(like["liked_by_user_id"])) # compound key because the same tweet can be liked by multiple users
        data_ref.set(like)

def download_new_tweets_and_likes_for_user(request):
    """
    Query Twitter API for fresh tweets and likes for a specific user and store them in Firestore

    HTTP cloud function that accepts POST with json body in format:
        {
            "user_id": 123,
            "username": "test",
            "latest_tweet_id": 456,
            "latest_like_at": "2021-12-09T12:34:00.000Z"
        }

    Responds with json body:
    {
        "status": "SUCCESS"
    }
    """
    if request.method != "POST" or request.headers["content-type"] != "application/json":
        logging.error("Incorrect method or content type: %s, %s", request.method, request.headers["content-type"])
        return (json.dumps({"status": "INVALID_REQUEST"}), 400, RESPONSE_HEADERS)
    watermarks = request.get_json(silent=False)
    logging.info("Fetching tweets and likes for user %s. Watermarks: %s", watermarks["username"], watermarks)
    tweets_and_likes = get_tweets_and_likes_for_user(watermarks["user_id"], watermarks["latest_tweet_id"], watermarks["latest_like_at"])
    store_tweets_in_firestore(tweets_and_likes["tweets"])
    store_likes_in_firestore(tweets_and_likes["likes"])
    return (json.dumps({"status": "SUCCESS"}), 200, RESPONSE_HEADERS)


def get_existing_user_ids():
    """
    Returns a set of user ids that already exist in our BigTable db
    """
    query = """
    SELECT DISTINCT id FROM TwitterData.users
    """
    df = BIGQUERY_CLIENT.query(query).to_dataframe()
    return set(df.id.values)

def get_user_ids_to_download(existing_user_ids):
    """
    Returns a set of user ids that need to be looked up via Twitter API.

    existing_user_ids is a set of user ids that already exist in our database
    """
    tweets = FIRESTORE_DB.collection(u"tweets").stream()
    all_user_ids = set([tweet.to_dict()["author_id"] for tweet in tweets])
    return all_user_ids - existing_user_ids

def get_users_by_ids(user_ids):
    """
    Queries Twitter API for user info for a list of user ids.

    Returns a list of dictionaries with user info.
    """
    BATCH_SIZE = 100
    users = []
    for n in range(int((len(user_ids) - 1)/BATCH_SIZE) + 1):
        batch = user_ids[n*BATCH_SIZE:min(len(user_ids), (n+1)*BATCH_SIZE)]
        response = TWITTER_CLIENT_RAW.get_users(ids=batch)
        response_json = response.json()
        if "data" in response_json:
            users.extend(response_json["data"])
        else:
            logging.warn("No data returned for request %s", response.request.url)
        time.sleep(3) # to adhere to 300 req/15 minutes rate limit
        if (n+1) % 10 == 0:
            logging.info("Fetched %d users", (n+1)*BATCH_SIZE)
    time.sleep(3)
    return users

def download_new_users(request):
    """
    Look at the fresh tweets and download user information about tweet authors that are not yet in our database.

    The records are saved into "users" collection in Firestore.

    HTTP Cloud function that accepts POST requests (no body needed).
    Responds with json body:
    {
        "status": "SUCCESS"
    }
    """
    if TWITTER_CLIENT_RAW is None:
        logging.error("Twitter client hasn't been initialized. Make sure environment variables are set. Exiting ...")
    if request.method != "POST":
        logging.error("Incorrect method: %s", request.method)
        return (json.dumps({"status": "INVALID_REQUEST"}), 400, RESPONSE_HEADERS)
    logging.info("Downloading new users. Looking up existing user ids")
    existing_ids = get_existing_user_ids()
    logging.info("Computing the list of new users")
    user_ids_to_download = get_user_ids_to_download(existing_ids)
    logging.info("About to query Twitter for %d user records", len(user_ids_to_download))
    users = get_users_by_ids(list(user_ids_to_download))
    logging.info("Got %d records from Twitter. Uploading to Firestore ...", len(users))
    for user in users:
        data_ref = FIRESTORE_DB.collection(u"users").document(str(user["id"]))
        data_ref.set(user)
    logging.info("Done uploading users to Firestore")
    return (json.dumps({"status": "SUCCESS"}), 200, RESPONSE_HEADERS)

def convert_to_tweets_table_row(tweet):
    """
    Generate a flattened dict from Twitter API data representing one tweet.

    This should match the schema of raw data tables in BigQuery
    """
    result = { key: tweet.get(key) for key in [
        "id", "text", "author_id", "created_at", "fetched_at", "in_reply_to_user_id"
    ]}
    result.update(tweet["public_metrics"])
    if "entities" in tweet:
        if "urls" in tweet["entities"]:
            result["mentioned_urls"] = [url["expanded_url"] for url in tweet["entities"]["urls"]]
        if "hashtags" in tweet["entities"]:
            result["mentioned_hashtags"] = [hashtag["tag"] for hashtag in tweet["entities"]["hashtags"]]
        if "mentions" in tweet["entities"]:
            result["mentioned_users"] = [mention["username"] for mention in tweet["entities"]["mentions"]]
    return result

def convert_nullable_to_int(n):
    if n is None:
        return None
    else:
        return int(n)

def create_new_tweets_and_references_dataframes():
    """
    Read new tweets and referenced tweets from Firestore and convert them to a dataframe that is ready to be uploaded to BigQuery.

    Returns a tuple with two dataframes - one for tweets and the other one for referenced tweets.
    """
    tweets = FIRESTORE_DB.collection(u"tweets").stream()
    tweets_list = []
    referenced_tweets = []
    for tweet in tweets:
        td = tweet.to_dict()
        tweets_list.append(convert_to_tweets_table_row(td))
        if "referenced_tweets" in td:
            for t in td["referenced_tweets"]:
                referenced_tweets.append({
                    "tweet_id": int(td["id"]),
                    "referenced_tweet_id": int(t["id"]),
                    "type": t["type"],
                })
    tweets_df = pd.DataFrame(tweets_list)
    # do necessary type conversions
    for field in ["id", "author_id"]:
        tweets_df[field] = pd.to_numeric(tweets_df[field])
    for field in ["created_at", "fetched_at"]:
        tweets_df[field] = pd.to_datetime(tweets_df[field])
    tweets_df["in_reply_to_user_id"] = tweets_df["in_reply_to_user_id"].apply(convert_nullable_to_int)

    ref_tweets_df = pd.DataFrame(referenced_tweets)

    return (tweets_df, ref_tweets_df)

def create_new_likes_dataframe():
    """
    Create a dataframe with all the likes stored in Firestore

    Returns dataframe.
    """
    likes = FIRESTORE_DB.collection(u"likes").stream()
    likes_df = pd.DataFrame([like.to_dict() for like in likes])
    likes_df.drop(columns=["text"], inplace=True)
    likes_df["id"] = likes_df["id"].astype(np.int64)
    likes_df["created_at"] = pd.to_datetime(likes_df["created_at"])
    return likes_df

def create_new_users_dataframe():
    """
    Create a dataframe with all the users stored in Firestore

    Returns dataframe.
    """
    users = FIRESTORE_DB.collection(u"users").stream()
    users_df = pd.DataFrame([user.to_dict() for user in users])
    users_df["id"] = pd.to_numeric(users_df["id"])
    return users_df

def upload_df_to_big_query_with_ds_partition(df, table_id):
    """
    Uploads dataframe to BigQuery table partitioned by ds field
    """
    table = BIGQUERY_CLIENT.get_table(table_id)
    # let's add ds field to our dataframe
    date_str = str(datetime.date.today())
    df_copy = df.copy()
    df_copy["ds"] = pd.to_datetime(date_str)
    BIGQUERY_CLIENT.query(f"DELETE {table_id} WHERE ds='{date_str}'")
    job = BIGQUERY_CLIENT.load_table_from_dataframe(df_copy, table)

    return job.result()

def upload_tweets_from_firestore_to_big_query(request):
    """
    Get all the tweets stored in Firestore, upload them to Big Query raw data tables.

    HTTP Cloud Function. Accepts only POST requests, no body is needed.
    Responds with json body:
    {
        "status": "SUCCESS"
    }
    """
    if request.method != "POST":
        logging.error("Incorrect method: %s", request.method)
        return (json.dumps({"status": "INVALID_REQUEST"}), 400, RESPONSE_HEADERS)
    logging.info("Getting tweets data from Firestore...")
    (tweets_df, referenced_tweets_df) = create_new_tweets_and_references_dataframes()
    logging.info("Got %d tweets and %d referenced tweets", len(tweets_df), len(referenced_tweets_df))
    logging.info("Uploading tweets to Big Query...")
    result = upload_df_to_big_query_with_ds_partition(tweets_df, "TwitterDataRaw.tweets")
    logging.info("Uploaded %d rows to Big Query. Now on to uploading referenced tweets", result.output_rows)
    result = upload_df_to_big_query_with_ds_partition(referenced_tweets_df, "TwitterDataRaw.referenced_tweets")
    logging.info("Uploaded %d rows to Big Query. We're done!", result.output_rows)
    return (json.dumps({"status": "SUCCESS"}), 200, RESPONSE_HEADERS)


def upload_likes_from_firestore_to_big_query(request):
    """
    Get all the likes stored in Firestore, upload them to Big Query raw data tables.

    HTTP Cloud Function. Accepts only POST requests, no body is needed.
    Responds with json body:
    {
        "status": "SUCCESS"
    }
    """
    if request.method != "POST":
        logging.error("Incorrect method: %s", request.method)
        return (json.dumps({"status": "INVALID_REQUEST"}), 400, RESPONSE_HEADERS)
    logging.info("Getting likes data from Firestore...")
    likes_df = create_new_likes_dataframe()
    logging.info("Got %d likes", len(likes_df))
    logging.info("Uploading likes to Big Query...")
    result = upload_df_to_big_query_with_ds_partition(likes_df, "TwitterDataRaw.likes")
    logging.info("Uploaded %d rows to Big Query. We're done!", result.output_rows)
    return (json.dumps({"status": "SUCCESS"}), 200, RESPONSE_HEADERS)


def upload_users_from_firestore_to_big_query(request):
    """
    Get all the users stored in Firestore, upload them to Big Query raw data tables.

    HTTP Cloud Function. Accepts only POST requests, no body is needed.
    Responds with json body:
    {
        "status": "SUCCESS"
    }
    """
    if request.method != "POST":
        logging.error("Incorrect method: %s", request.method)
        return (json.dumps({"status": "INVALID_REQUEST"}), 400, RESPONSE_HEADERS)
    logging.info("Getting users data from Firestore...")
    likes_df = create_new_users_dataframe()
    logging.info("Got %d users", len(likes_df))
    logging.info("Uploading users to Big Query...")
    result = upload_df_to_big_query_with_ds_partition(likes_df, "TwitterDataRaw.users")
    logging.info("Uploaded %d rows to Big Query. We're done!", result.output_rows)
    return (json.dumps({"status": "SUCCESS"}), 200, RESPONSE_HEADERS)

def delete_collection(coll_ref, batch_size, collection_name):
    """
    Recursively delete all documents from Firestore collection
    """
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0

    for doc in docs:
        doc.reference.delete()
        deleted = deleted + 1
    logging.info("Deleted %d docs from collection %s", deleted, collection_name)

    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size, collection_name)
    else:
        logging.info("Done deleting from collection %s", collection_name)

def cleanup_firestore_data(request):
    """
    Remove all temporary collections from the Firestore

    HTTP Cloud Function. Accepts only POST requests, no body is needed.
    Responds with json body:
    {
        "status": "SUCCESS"
    }
    """
    if request.method != "POST":
        logging.error("Incorrect method: %s", request.method)
        return (json.dumps({"status": "INVALID_REQUEST"}), 400, RESPONSE_HEADERS)

    BATCH_SIZE = 100
    logging.info("Deleting tweets collection")
    delete_collection(FIRESTORE_DB.collection(u"tweets"), BATCH_SIZE, "tweets")
    logging.info("Deleting likes collection")
    delete_collection(FIRESTORE_DB.collection(u"likes"), BATCH_SIZE, "likes")
    logging.info("Deleting users collection")
    delete_collection(FIRESTORE_DB.collection(u"users"), BATCH_SIZE, "users")
    logging.info("Deleting influencer_watermarks collection")
    delete_collection(FIRESTORE_DB.collection(u"influencer_watermarks"), BATCH_SIZE, "influencer_watermarks")
    return (json.dumps({"status": "SUCCESS"}), 200, RESPONSE_HEADERS)

import urllib.request as urllib
from bs4 import BeautifulSoup

PAGE_TITLE_HASH = {}

def fetch_page_title(url):
    """
    Fetch title of a webpage by its url.

    This requires fetching the whole webpage, which might be slow and data intensive, and can fail for various reasons.

    Returns fetched title, or the page url if we failed to get the title.
    """
    if url in PAGE_TITLE_HASH:
        return PAGE_TITLE_HASH[url]
    title = url
    try:
        # headers that make it more likely that the website won't respond with 403. Kind of random, found on SO :-(
        hdr = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive'
        }

        req = urllib.Request(url, headers=hdr)
        soup = BeautifulSoup(urllib.urlopen(req))
        if soup.title.string is not None:
            title = soup.title.string
    except BaseException as ex:
        logging.warning('error getting title for url %s, %s', url, ex)
        return url # let's not store it in cache
    PAGE_TITLE_HASH[url] = title
    return title

def get_popular_urls(days_in_range=30):
    """
    Run a BigQuery query to get stats about mentioned urls and select top 50 most popular.
    days_in_range is positive integer determining how far back in the past we look.
    E.g. days_in_range=7 will return data about urls mentioned in the past 7 days.

    Returns a dataframe with urls.
    """

    if not (days_in_range >= 0 and days_in_range <= 1000):
        # it's super unlikely that someone will attempt SQL injection, and the better way to deal with this is query parameters,
        # but let's add this check just in case
        logging.error("get_popular_urls was called with invalid days_in_range parameter value: %s", str(days_in_range))
        return pd.DataFrame()

    query = f"""
    WITH url_mentions AS (
        SELECT
            *
        FROM TwitterData.tweets
        CROSS JOIN UNNEST(mentioned_urls) AS mentioned_url
        WHERE created_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL {days_in_range} DAY)) AND mentioned_url NOT LIKE '%twitter.com%'
    )
    SELECT
        mentioned_url,
        COUNT(1) AS mentions_count,
        COUNTIF(is_by_influencer) AS influencer_mentions_count,
        SUM(retweet_count) AS retweet_count,
        SUM(IF(is_by_influencer, retweet_count, 0)) AS influencer_retweet_count,
        SUM(quote_count) AS quote_count,
        SUM(IF(is_by_influencer, quote_count, 0)) AS influencer_quote_count,
        ARRAY_AGG(DISTINCT IF(is_by_influencer, author_username, NULL) IGNORE NULLS) AS mentioned_by_influencers,
        COALESCE(ARRAY_LENGTH(ARRAY_AGG(DISTINCT IF(is_by_influencer, author_username, NULL) IGNORE NULLS)), 0) AS influencer_count,
        ARRAY_LENGTH(ARRAY_AGG(DISTINCT author_username)) AS user_count,
        ARRAY_AGG(CONCAT('https://twitter.com/', author_username, '/status/', id)) AS tweet_urls
    FROM url_mentions
    GROUP BY mentioned_url
    ORDER BY mentions_count DESC, quote_count DESC
    LIMIT 50
    """
    df = BIGQUERY_CLIENT.query(query).to_dataframe()
    df["mentioned_by_influencers"] = df["mentioned_by_influencers"].apply(lambda a: list(a))
    df["tweet_urls"] = df["tweet_urls"].apply(lambda a: list(a))
    # fetch page titles, where possible
    df["title"] = df["mentioned_url"].apply(fetch_page_title)
    return df

def save_popular_urls_to_firestore(urls_df, key):
    """
    Store most popular urls for a given timerange into Firestore.

    urls_df - dataframe with urls stats
    key - string id corresponding to that time range. E.g. lastMonth, lastWeek or last2days

    Doesn't return anything
    """
    urls = [row.to_dict() for _, row in urls_df.iterrows()]
    doc = {
        "urls": urls,
    }
    FIRESTORE_DB.collection("urlsData").document(key).set(doc)

def refresh_trending_urls_data(request):
    """
    Run BigQuery queries to get most popular urls for various time ranges. Upload the results into Firestore for use by website.

    HTTP Cloud Function. Accepts only POST requests, no body is needed.
    Responds with json body:
    {
        "status": "SUCCESS"
    }
    """
    if request.method != "POST":
        logging.error("Incorrect method: %s", request.method)
        return (json.dumps({"status": "INVALID_REQUEST"}), 400, RESPONSE_HEADERS)

    ranges = {
        "lastMonth": 30,
        "lastWeek": 7,
        "last2days": 2,
    }

    for key, days in ranges.items():
        logging.info("Fetching popular urls in the %s range", key)
        save_popular_urls_to_firestore(get_popular_urls(days), key)

    return (json.dumps({"status": "SUCCESS"}), 200, RESPONSE_HEADERS)
