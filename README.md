# redshift-bq-migration
Redshift to BigQuery migration require the following steps.

![Architecture diagram](img/redshif_bq_arch.png)

```sh
cd ~/redshift_poc
nohup python3 unload_sortkey_v3.py > idt_rpt_common_downloads_6_7_2023.out &
```

# Script for copy from S3 to Google Cloud Storage

```sh
cd ~/redshift_poc
./download_s3.sh 
```

