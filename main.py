# TODO: Develop your code
import os
from datetime import date
from time import sleep

import requests
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
# For api layer
payload = {}
headers = {
    "apikey": "75KqFpjG8FAmPxhmN73oYJhUfryNmjN9"
}


def get_rates_url(base, start_date, end_date):
    return "https://api.apilayer.com/exchangerates_data/timeseries?base=%s&start_date=%s&end_date=%s" % (
    base, start_date, end_date)


def api_request(api_url):
    response = requests.request("GET", api_url, headers=headers,
                                data=payload)
    status_code = response.status_code
    print(status_code)
    result = response.text
    return result


def get_rates(base, start_date, end_date):
    results = api_request(get_rates_url(base, start_date, end_date))
    df_results = pd.read_json(results)
    df_rates = pd.json_normalize(df_results.rates)
    df_rates.insert(0, 'date', df_results.index)
    df_rates.insert(1, 'base', df_results['base'].values)
    df_rates['date'] = df_rates['date'].astype(str)
    return df_rates


def create_table_gcp(table_name, data_fields):
    table = f"{project_name}.{dataset_name}.{table_name}"
    print(table)
    schema = []
    for field in data_fields:
        if field == 'date':
            schema.append(bigquery.SchemaField("DATE", "DATE", mode="REQUIRED"))
        elif field == 'base':
            schema.append(bigquery.SchemaField("BASE", "STRING", mode="REQUIRED"))
        else:
            schema.append(bigquery.SchemaField(field, "FLOAT", mode="REQUIRED"))
    create_table(table, schema)
    return table


def query_print_table(table):
    query_string = f"SELECT * FROM {table}"
    print(query(query_string))


def find_last_date_table(table):
    query_string = f"SELECT max(DATE) FROM {table}"
    return query(query_string)['f0_'][0]


def date_today():
    return date.today().strftime('%Y-%m-%d')


rates_to_upload = get_rates(base='EUR', start_date='2022-01-01', end_date='2022-05-31')
delete_table('qntq-recruitment.data.de_currency_rates')
rates_table = create_table_gcp("de_currency_rates", rates_to_upload)
sleep(10)  # Sleep to prevent 404 error from batch_upload
batch_upload(rates_table, rates_to_upload.to_dict(orient='records'))
last_date = find_last_date_table(rates_table)
print("Rates uploaded until %s" % (last_date))
new_rates_to_upload = get_rates(base='EUR', start_date=last_date, end_date=date_today())
sleep(10)  # Sleep to prevent 404 error from batch_upload
batch_upload(rates_table, new_rates_to_upload.to_dict(orient='records'))
last_date = find_last_date_table(rates_table)
print("Rates uploaded until %s" % (last_date))
print(query_print_table(rates_table))
