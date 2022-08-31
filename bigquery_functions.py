from google.cloud import bigquery

def create_table(table_name, schema):

    # create a table in bigquery
    client = bigquery.Client()


    table = bigquery.Table(table_name, schema=schema)
    table = client.create_table(table) # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def delete_table(table_name):

    # create a table in bigquery
    client = bigquery.Client()


    table = bigquery.Table(table_name)
    table = client.delete_table(table) # Make an API request.
    print(
        "Deleted table {}".format(table_name)
    )

def fill_table(table, rows_to_insert):
    
    # fill a bigquery table with rows
    client = bigquery.Client()
    errors = client.insert_rows_json(table, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


def batch_upload(table, rows_to_insert):

    # upload rows in batches of 1000
    for c in range(int(len(rows_to_insert)/1000)):

        c+=c*1000
        fill_table(table, rows_to_insert[c:int(c+1000)])

    fill_table(table, rows_to_insert[(int(len(rows_to_insert)/1000))*1000:])


def query(query):

    # make a query in bigquery using python
    client = bigquery.Client()
    query_job = client.query(
               query
    )
    return query_job.result().to_dataframe()