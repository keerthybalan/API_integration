# Import necessary packages
import os
from google.cloud import bigquery
import pandas as pd
from bigquery_functions import (query,
                                create_table,
                                batch_upload, 
                                delete_table)

# Initialize google environment and other variables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "qntq-recruitment-3ce2719bea9c.json"
project_name = "qntq-recruitment"
dataset_name = "data"

# DEMO: To create a table
table_name = "testtablename"
table= f"{project_name}.{dataset_name}.{table_name}"
schema = [
    bigquery.SchemaField("TESTCOLNAMESTRING", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("TESTCOLNAMEINT", "INTEGER", mode="REQUIRED"),
]
create_table(table, schema)

# DEMO: to fill a table
dataset = pd.DataFrame([{"TESTCOLNAMESTRING": "Name1", 
              "TESTCOLNAMEINT": 2}, 
             {"TESTCOLNAMESTRING": "Name2", 
              "TESTCOLNAMEINT": 2}] )
batch_upload(table, dataset.to_dict(orient='records'))

# DEMO: to query a table
query_string= f"SELECT * FROM {table}"
query(query_string)

# DEMO: delete the table
delete_table(table)