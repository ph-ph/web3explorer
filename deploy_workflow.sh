gcloud workflows deploy download_fresh_twitter_data \
    --source=./src/workflows/download_fresh_twitter_data.yaml \
    --description="Workflow that queries Twitter API for fresh data and uploads it to BigQuery"