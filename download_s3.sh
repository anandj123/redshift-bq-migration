export S3_FOLDER_NAME=s3://<bucket_name>/<folder_name>/
export GCS_FOLDER_NAME=gs://<bucket_name>/<folder_name>/
gcloud storage cp -R $S3_FOLDER_NAME $GCS_FOLDER_NAME 