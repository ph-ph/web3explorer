gcloud functions deploy compute_influencer_watermarks \
        --region=us-west1 \
        --memory=256MB \
        --runtime=python39 \
        --service-account=service-account@web3twitterdata.iam.gserviceaccount.com \
        --source=./src \
        --timeout=60s \
        --trigger-topic=compute_influencer_watermarks