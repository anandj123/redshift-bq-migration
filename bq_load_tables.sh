#!/bin/bash

# folder structure assumption:
#
#     parent_folder (GCS_FOLDER_NAME)
#            dataset_1
#               table_1
#                  table_splits_0
#                  table_splits_1
#                  table_splits_2
#               table_2
#                  table_splits_0
#                  table_splits_1
#                  table_splits_2
#            dataset_2
#               table_3
#                  table_splits_0
#                  table_splits_1
#                  table_splits_2
#               table_4
#                  table_splits_0
#                  table_splits_1
#                  table_splits_2

export GCS_FOLDER_NAME=gs://tr-redshift-bigquery-load/sort_key_test_5_3_2023_3_test2/sort_key_test_5_3_2023_3/

for dataset in $(echo "$(gcloud storage ls $GCS_FOLDER_NAME)" | awk '{n=split($0, array, "/"); print array[n-1]}')
do
        # change the dataset name if it is different from the original schema name in GCS bucket

        target_dataset=$dataset'_2'

        echo "Dataset Name: "$dataset
        echo "Target dataset Name: "$target_dataset

        for table in $(echo "$(gcloud storage ls $GCS_FOLDER_NAME$dataset'/')" | awk '{n=split($0, array, "/"); print array[n-1]} ')
        do
            echo "Table Name: "$table
            bq load --noautodetect --source_format=PARQUET -decimal_target_types="BIGNUMERIC" $target_dataset.$table "$GCS_FOLDER_NAME$dataset/$table/*"
        done
done