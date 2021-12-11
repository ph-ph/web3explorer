# What is this?
Collection of GCP cloud functions and workflow scripts to download and analyze Twitter data about "web3 influencers": their tweets, referenced tweets and likes.

# High-level overview of the project
We query data using Twitter API and store it in BigQuery for further analysis.

The code in this project handles incremental updates:
- There's Google Workflow configuration that gets executed daily: `src/workflows/download_fresh_twitter_data.yaml`.
- The workflow first queries BigQuery to get the stats about the latest tweets and likes we've already seen. This is done via Cloud Function `compute_influencer_watermarks`. This and other Cloud Functions live in `src/main.py` .
- We then call Cloud Function `download_new_tweets_and_likes_for_user` that queries Twitter API for tweets and likes for each influencer and stores the data in Firestore.
- After we fetched fresh tweets, we use `download_new_users` to fetch profile information for users that we haven't seen before.
- After that we call cloud functions that copy tweets, likes, referenced_tweets and users data from Firestore to BigQuery.
- Once we have tables with the fresh data in BigQuery (in TwitterDataRaw dataset) we run queries that incrementally update tables in the main dataset TwitterData to incorporate our new data.
- We then run a couple of BigQuery queries to compute word statistics and store them in `word_mentions` and `word_mention_stats` tables.
- After that we delete all temporary data in Firestore. We're done!

There are a bunch of shell scripts in the root folder that deploy various artifacts to GCP.

# Caveats
- Folder structure in the project is suboptimal, to say the least - we deploy table schemas and workflow config with every CloudFunction. I would've fixed it, but the amount of retesting that will have to be done is non-trivial.
- The way we store data in Firestore is less than optimal from the pricing perspective. Each tweet and like are stored as a separate document. Since we download thousands of them every day, that easily pushes us towards free 20k writes threshold. If I had to do that again, I'd store batches of tweets in documents, to reduce the amount of reads and writes. Or maybe I should've skipped Firestore and simply stored everything in GCP's S3 equivalent. Idk, Firestore is convenient, though. Other than that, the project is well within free tier of GCP.
- A lot of artifacts in the project have been created manually. If I had to do that again, I'd probably use Terraform to automate all of this.

# If you had to replicate that in your own GCP project
- Enable Cloud Functions, Workflows, Firestore, BigQuery in your project.
- Create TwitterData and TwitterDataRaw datasets in BigQuery.
- Create tweets, likes, referenced_tweets and users tables in TwitterData dataset - they can be empty, and you can guess the schema based on the queries in my workflow config.
- Deploy all the Cloud Functions and the workflow config. This will require creating service account. You'll also need to create a secret using Secret Manager and make sure that Twitter API's credentials are passed to Cloud Functions via env variables.
- Schedule the workflow to run once a day
