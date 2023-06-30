# redshift-bq-migration
Redshift to BigQuery migration require the following steps.

[Architecture diagram](img/redshif_bq_arch.png)
```
cd ~/redshift_poc
nohup python3 unload_sortkey_v3.py > idt_rpt_common_downloads_6_7_2023.out &
```


