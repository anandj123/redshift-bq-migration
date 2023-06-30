# Redshift to BigQuery migration using optimized Redshift UNLOAD
The following diagram describes the approach we propose to migrate historical data from Redshift to BigQuery. 

For bulk unload of Redshift tables it is required to UNLOAD the data using multi-threaded application otherwise the UNLOAD througput we observed is quite slow. So the following python program can leverage the natural sort order of table (if one defined) to UNLOAD multiple sort keys in parallel and you can get a good throughput.

![Architecture diagram](img/redshif_bq_arch.png)

# Scripts for Triggering AWS Redshift UNLOAD command

Parameter | Description | 
---|---|
dryrun |True - If no UNLOAD is needed. Just for testing and debugging purposes. False - Run the UNLOAD commands. 
port | Redshift port number 
username | User name of the Redshift cluster  
database_name | Database name of the redshift cluster 
cluster_id | Cluster id of the Redshift cluster 
url | URL of the Redshift cluster 
region | Region of the Redshift cluster 
bucket_name | Name of the S3 bucket where the Redshift UNLOAD will write the output 
parallel_threads | Number of threads to run UNLOAD commands. 
conn_string | Connection string for Redshift cluster 
schemas | List of Redshift schemas that need to be unloaded. This is a YAML list. 
&ensp;schema_name | Name of the schema to unload 
&ensp;tables | YAML list of tables in the schema that needs to be unloaded. 
&emsp;table_name | Name of the table in Redshift 
&emsp;sort_key_col | Sort key column name of the Redshift table. 

```yaml
---
redshift_config:
  dryrun: False  
  port: 5439
  username: <user_name>
  database_name: <db_name>
  cluster_id: <cluster_id>
  url: <url> 
  region: <region>
  bucket_name: <s3_bucket_name>
  parallel_threads: <Number of parallel threads e.g. 30> 
  conn_string: <connection string>
schemas:
- schema_name: <schema_name> 
  tables:
  - table_name: <table_name>
    sort_key_col: <sort_key_name>
- schema_name: <schema_name_2>
  tables:
  - table_name: <table_name_2>
  - table_name: <table_name_3>

```

```sh
nohup python3 unload_sortkey_v3.py > idt_rpt_common_downloads_6_7_2023.out &
```

# Script for copy from S3 to Google Cloud Storage

```sh
./download_s3.sh 
```

# Script for loading from Google Cloud Storage to Google BigQuery

```sh
./bq_load_tables.sh
```