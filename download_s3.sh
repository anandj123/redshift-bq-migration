export S3_FOLDER_NAME=s3://a206447-deal-redshift-bq-migration-poc/sort_key_test_5_3_2023_3/
export GCS_FOLDER_NAME=gs://tr-redshift-bigquery-load/sort_key_test_5_3_2023_3_test2/
gcloud storage cp -R $S3_FOLDER_NAME $GCS_FOLDER_NAME 