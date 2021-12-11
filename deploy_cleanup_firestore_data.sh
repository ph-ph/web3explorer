gcloud functions deploy cleanup_firestore_data \
        --region=us-west1 \
        --memory=512MB \
        --runtime=python39 \
        --service-account=service-account@web3twitterdata.iam.gserviceaccount.com \
        --source=./src \
        --timeout=540s \
        --trigger-http