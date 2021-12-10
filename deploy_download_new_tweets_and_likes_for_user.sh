gcloud functions deploy download_new_tweets_and_likes_for_user \
        --region=us-west1 \
        --memory=512MB \
        --runtime=python39 \
        --service-account=service-account@web3twitterdata.iam.gserviceaccount.com \
        --source=./src \
        --timeout=540s \
        --trigger-http