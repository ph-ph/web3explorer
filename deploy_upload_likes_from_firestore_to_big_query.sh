gcloud functions deploy upload_likes_from_firestore_to_big_query \
        --region=us-west1 \
        --memory=512MB \
        --runtime=python39 \
        --service-account=service-account@web3twitterdata.iam.gserviceaccount.com \
        --source=./src \
        --timeout=540s \
        --trigger-topic=upload_likes_from_firestore_to_big_query