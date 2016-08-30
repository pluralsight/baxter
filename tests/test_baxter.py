import sys
import json
import pyodbc
#import baxter
from baxter import googlecloud
from baxter import toolbox
from baxter import relationaldb
from apiclient.discovery import build
from test_config import (GC_SERVICE_ACCOUNT_EMAIL, GC_SECRET_KEY_PATH, BQ_PROJECT, BQ_DATASET_PROD, GS_BUCKET, DB_SERVER,
                        DB_NAME, DB_USER, DB_PASSWORD, LOCAL_TEST_DATA_PATH, BQ_TEST_DATA_PATH)

run_bq_test = 1
run_mssql_test = 0

def bq_query(bqservice, project_id, dataset_id, target_table):
    bq_query = "Select count(1) From " + dataset_id + "." + target_table
    r = googlecloud.query_table(bqservice,project_id,bq_query)
    bq_count = r[0][0]
    print "BQ Record Count:    " + str(bq_count)
    return bq_count
    
def sql_query(server, database, table, username, password, table_schema='dbo'):
    try:
        
        connect_string = 'DRIVER={ODBC Driver 11 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
        connection = pyodbc.connect(connect_string)
    except (ValueError) as e:
        print "Error creating database connection", e

    mssql_query = "Select count(1) From " + table
    cursor = relationaldb.sql_get_query_data(connection,mssql_query)
    mssql_count = cursor.fetchone()[0]
    print "MSSQL Record Count: " + str(mssql_count)
    connection.close()
    return mssql_count

def test_mssql_1():
    if run_mssql_test:
        server = DB_SERVER
        db = DB_NAME
        username = DB_USER
        password = DB_PASSWORD
        table = 'test_table'

        mssql_count = sql_query(server, db, table, username, password)
        assert bq_count > 0

def test_bq_list_datasets_1():
    bq_token = googlecloud.gcloud_connect(GC_SERVICE_ACCOUNT_EMAIL, GC_SECRET_KEY_PATH, 'https://www.googleapis.com/auth/bigquery')
    bqservice = build('bigquery', 'v2', http=bq_token)
    project_id = BQ_PROJECT
    googlecloud.list_datasets(bqservice, project_id)
    assert True


def test_gc_upload_1():
    if run_bq_test:
        gs_token = googlecloud.gcloud_connect(GC_SERVICE_ACCOUNT_EMAIL, GC_SECRET_KEY_PATH,
                                              'https://www.googleapis.com/auth/devstorage.read_write')
        gsservice = build('storage', 'v1', http=gs_token)
        project_id = BQ_PROJECT
        bucket = GS_BUCKET
        source_file = LOCAL_TEST_DATA_PATH + 'test_table_data.csv'
        dest_file = BQ_TEST_DATA_PATH + 'test_table_data.csv'
    googlecloud.cloudstorage_upload(gsservice, project_id, bucket, source_file, dest_file,
                                    show_status_messages=False)


def test_gc_upload_2():
    if run_bq_test:
        gs_token = googlecloud.gcloud_connect(GC_SERVICE_ACCOUNT_EMAIL, GC_SECRET_KEY_PATH,
                                              'https://www.googleapis.com/auth/devstorage.read_write')
        gsservice = build('storage', 'v1', http=gs_token)
        project_id = BQ_PROJECT
        bucket = GS_BUCKET
        source_file = LOCAL_TEST_DATA_PATH + 'test_table_data.json'
        dest_file = BQ_TEST_DATA_PATH + 'test_table_data.json'
    googlecloud.cloudstorage_upload(gsservice, project_id, bucket, source_file, dest_file,
                                    show_status_messages=False)


def test_bq_load_table_from_file_1():
    bq_token = googlecloud.gcloud_connect(GC_SERVICE_ACCOUNT_EMAIL, GC_SECRET_KEY_PATH, 'https://www.googleapis.com/auth/bigquery')
    bqservice = build('bigquery', 'v2', http=bq_token)
    project_id = BQ_PROJECT
    dataset_id = BQ_DATASET_PROD
    target_table = 'test_table'
    sourceCSV = BQ_TEST_DATA_PATH + 'test_table_data.csv'
    googlecloud.load_table_from_file(bqservice, project_id, dataset_id, target_table, sourceCSV,field_list=None,delimiter='\t',skipLeadingRows=0, overwrite=False)
    assert True

def test_bq_load_table_from_json_1():
    bq_token = googlecloud.gcloud_connect(GC_SERVICE_ACCOUNT_EMAIL, GC_SECRET_KEY_PATH, 'https://www.googleapis.com/auth/bigquery')
    bqservice = build('bigquery', 'v2', http=bq_token)
    project_id = BQ_PROJECT
    dataset_id = BQ_DATASET_PROD
    target_table = 'test_table'
    source_file = BQ_TEST_DATA_PATH + 'test_table_data.json'
    googlecloud.load_table_from_json(bqservice, project_id, dataset_id, target_table, source_file, field_list=None, overwrite=False)
    assert True

# def test_bq_load_table_1():
#     googlecloud.load_table(service, project_id, job_data)

# def test_bq_load_from_query_1():
#     googlecloud.load_from_query(service, project_id, dataset_id, target_table, source_query,overwrite = False)

def test_bq_export_table_1():
    bq_token = googlecloud.gcloud_connect(GC_SERVICE_ACCOUNT_EMAIL, GC_SECRET_KEY_PATH, 'https://www.googleapis.com/auth/bigquery')
    bqservice = build('bigquery', 'v2', http=bq_token)
    project_id = BQ_PROJECT
    dataset_id = BQ_DATASET_PROD
    source_table = 'test_table'
    destination_uris = ["gs://" + GS_BUCKET + "/test_table_export.csv"]
    googlecloud.export_table(bqservice, project_id, dataset_id, source_table, destination_uris, compress = False, delimiter = ',', print_header = True)
    assert True
    
def test_bq_query_1():
    if run_bq_test:
        bq_token = googlecloud.gcloud_connect(GC_SERVICE_ACCOUNT_EMAIL, GC_SECRET_KEY_PATH, 'https://www.googleapis.com/auth/bigquery')
        bqservice = build('bigquery', 'v2', http=bq_token)
        project_id = BQ_PROJECT
        dataset_id = BQ_DATASET_PROD
        target_table = 'test_table'
        bq_count = bq_query(bqservice,project_id,dataset_id,target_table)
        assert bq_count > 0

# def test_bq_delete_table_1():
#     googlecloud.delete_table(service, project_id,dataset_id,table)


# def test_gc_download_1():
#     googlecloud.cloudstorage_download(cloudstorage_download(service, project_id, bucket, source_file, dest_file, show_status_messages=False)

# def test_cloudstorage_delete_1():
#     googlecloud.cloudstorage_delete(service, project_id, bucket, filename, show_status_messages=False)

if __name__ == "__main__":
    test_gc_upload_1()
    test_gc_upload_2()
    test_bq_load_table_from_file_1()
    test_bq_load_table_from_json_1()
    test_bq_export_table_1()
    test_bq_query_1()
