gcloud functions deploy download_new_users \
        --region=us-west1 \
        --memory=256MB \
        --runtime=python39 \
        --service-account=service-account@web3twitterdata.iam.gserviceaccount.com \
        --source=./src \
        --timeout=540s \
        --trigger-topic=download_new_users