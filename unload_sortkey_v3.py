import yaml
import hashlib
import psycopg2
import time
import uuid
import math
import configparser
import threading
import sys
import queue
import os.path
from datetime import datetime
from datetime import timedelta 
from pathlib import Path 
from google.cloud.exceptions import NotFound
from google.cloud.exceptions import BadRequest
from google.cloud import bigquery

# Global variables for storing the config file parameters

config_file = "config_v3.yaml"
mapping = {}
conn_string = ""
dryrun = True 
bucket_name = ""
parallel_threads = ""
iam_role = ""
select_query = 'select * from {}.{}'
unload_query = "unload ('{}') to '{}' {} ALLOWOVERWRITE PARALLEL ON PARQUET;"

##################################################################################
#
# WorkQueu holds all the work queies. A work item consists of the following items
#   schema:             Name of schema
#   table:              Name of table
#   low:                Lower key value for the unload WHERE clause
#   high:               Higher key value for the unload WHERE clause
#   split_num:          Split number (1 to n) for each of the splits
#   total_splits:       Total number of splits based on the sort_key
#   sort_key_col_name:  Name of the sort_key for the particular table
#   key_count:          Number of records for particular split
#
# This class has the following methods
#   add_task:   Add a task with the above parameters to the work queue.
#   schedule:   Schedule the work as a thread
#   get_key:    Create a unique key from the parameters
#   print:      Used for debug print of the workque items
#
##################################################################################
class WorkQueue:

    que = []
    concurrent = 0

    def __init__(self, concurrent):
        self.concurrent = concurrent

    def add_task(self,schema,table,low,high, split_num, total_splits,sortkey_col_name,key_count):
        self.que.append([schema,table,low,high,split_num,total_splits,sortkey_col_name,key_count])
        
    def schedule(self):

        threads = {} 

        while (len(self.que) > 0) :
            if (len(threads) < self.concurrent):
                work_item = self.que.pop()
                t = threading.Thread(target=task,args=([work_item]))
                threads[t] = "INIT"
                print("Scheduleing work: ", work_item, flush=True)
                continue

            for thread in threads:
                if threads[thread] == "INIT": 
                    thread.start()
                    threads[thread] = "RUNNING"

            for thread in list(threads):
                thread.join(2)
                if (not thread.is_alive()):
                    threads.pop(thread)

        while (len(threads) > 0):
            for thread in threads:
                if threads[thread] == "INIT": 
                    thread.start()
                    threads[thread] = "RUNNING"

            for thread in list(threads):
                thread.join(2)
                if (not thread.is_alive()):
                    threads.pop(thread)

    def get_key(self, work_item):
        schema = work_item[0] 
        table = work_item[1] 
        low = work_item[2]
        high = work_item[3]
        split_num = work_item[4] 
        total_splits = work_item[5] 
        sortkey_col_name = work_item[6] 

        return ("{}_{}_{}_{}_{}_{}_{}".format(schema,
            table, 
            str(split_num), 
            str(total_splits), 
            str(low),str(high),sortkey_col_name))

    def print(self):
        while len(self.que) > 0:
            print(self.que.pop(0))

##################################################################################
#
# This is the thread task that fires the UNLOAD command. 
#
##################################################################################
def task(work_item):

    global conn_string
    
    schema = work_item[0] 
    table = work_item[1] 
    low = work_item[2]
    high = work_item[3]
    split_num = work_item[4] 
    total_splits = work_item[5] 
    sortkey_col_name = work_item[6] 
    key_count = work_item[7] 

    key = "{}.{}_(split)_{}_(key_value)_{}_(sortkey_col_name)_{}".format(schema,
            table, 
            str(split_num), 
            #str(total_splits), 
            str(low),sortkey_col_name)

    Path("./locks/"+key).touch()

    start_time = datetime.now()
    print("Starting download [{}] Time: [{}]".format(key, start_time), flush=True)

    hash_object = hashlib.sha1(key.encode('utf-8'))
    hex_dig = hash_object.hexdigest()

    unload_bucket_name = "{}{}/{}/{}/".format(bucket_name, schema, table, hex_dig)

    # Simulating an error
    #if (table == "idt_rpt_countries_dim"):
    #    exit()

    if dryrun:
        time.sleep(1)
    else:
        con=psycopg2.connect(conn_string)

        cursor = con.cursor()
        if (len(sortkey_col_name) > 0):
            unload_query_2 = unload_query.format(
                select_query.format(schema, table) + " where {} = \\'{}\\'  ".format(sortkey_col_name,low.strftime("%Y-%m-%d")),
                unload_bucket_name, iam_role)
            print(unload_query_2, flush=True)
            cursor.execute(unload_query_2)
        else: 
            unload_query_2 = unload_query.format(
                select_query.format(schema, table),
                unload_bucket_name, iam_role)
            print(unload_query_2, flush=True)
            cursor.execute(unload_query_2)


        con.close()

    end_time = datetime.now()

    print("Done {} Time:{} Pulled {} records in {} seconds".format(
        key,
        end_time, str(key_count),
        (end_time-start_time).total_seconds()), flush=True)
    
    Path("./locks/"+key).unlink()

##################################################################################
#
# The following function
#   [find_range] finds range of values for the sorkkey_col_name so that we can 
#   create task items in work queue so that they can be run in parallel.
#
##################################################################################
def find_range(schema, table, sortkey_col_name):
    global conn_string
    print(conn_string, flush=True)
    con=psycopg2.connect(conn_string)
    cursor = con.cursor()

    cursor.execute("select min({}), max({}) from {}.{}".format(\
            sortkey_col_name,\
            sortkey_col_name,\
            schema,table))
    records = cursor.fetchall()
    
    return (records[0][0],records[0][1])

##################################################################################
#
# The following function
#   [find_count] finds the count of records for the sortkey_value so that we can  
#   ignore splits that have no (0) records. 
#
##################################################################################
def find_count(schema, table, sort_key_name,sort_key_val):
    global conn_string
    con=psycopg2.connect(conn_string)
    cursor = con.cursor()

    cursor.execute("select count(*) from {}.{} where {} = '{}'".format(\
            schema,table,sort_key_name,sort_key_val))
    records = cursor.fetchall()
    
    return records[0][0]

##################################################################################
#
# The following function
#   [read_mapping] reads the config file and stores them as global variables.
#       For each of the schema finds table and their sort_keys and schedules
#       work items for each of the sortkey splits.
#
##################################################################################
def read_mapping():
    with open(config_file,'r') as f:
        config_data = yaml.safe_load(f)
        global conn_string, bucket_name, parallel_threads, iam_role, dryrun

        conn_string = config_data['redshift_config']['conn_string']
        bucket_name = config_data['redshift_config']['bucket_name']
        parallel_threads = config_data['redshift_config']['parallel_threads']
        iam_role = config_data['redshift_config']['iam_role']
        dryrun = config_data['redshift_config']['dryrun']

        print(conn_string, bucket_name, parallel_threads, flush=True)

        work = WorkQueue(parallel_threads)

        # Read each schema
        for schema in config_data['schemas']:
            schema_name = schema['schema_name']
            print("Schema: " + schema_name, flush=True)

            # Read each table
            for table in schema['tables']:

                table_name = table['table_name']
                print("Table Name: ", table_name, flush=True)
                number_of_splits = 1
                if 'sort_key_col' in table.keys(): 

                    sort_key_col = table['sort_key_col']

                    print("sort key name {}".format(sort_key_col), flush=True)
            
                    # Find lower and high range for the sort column
                    sort_key_range = find_range(schema_name, table_name, sort_key_col)

                    low = sort_key_range[0]
                    high = sort_key_range[1]
                
                    print(low,high,high-low, flush=True)

                    while( (high-low) >= timedelta(days=0) ):
                        key_count = find_count(schema_name, table_name, sort_key_col, low.strftime("%Y-%m-%d"))

                        # If we have records for this sort column then add the task
                        if (key_count >0):
                            print(schema_name, table_name, sort_key_col, low, key_count, flush=True)
                            work.add_task(schema_name, table_name, low, low, 1, 1, sort_key_col,key_count)

                        low = low + timedelta(days=1) 
                else: 
                    print(schema_name, table_name, "1", "1", "1",1, flush=True)
                    work.add_task(schema_name, table_name, 0, 0, 1, 1, "",1)

        work.schedule()

read_mapping()