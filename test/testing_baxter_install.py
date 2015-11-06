import sys
import json
import pyodbc
#import baxter
from baxter import googlecloud
from baxter import toolbox
from baxter import relationaldb
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import SignedJwtAssertionCredentials
from test_config import GC_SERVICE_ACCOUNT_EMAIL, GC_SECRET_KEY_PATH, BQ_PROJECT, BQ_DATASET_PROD, DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD 



def test_bq_query(bqservice, project_id, dataset_id, target_table):
    bq_query = "Select count(1) From " + dataset_id + "." + target_table
    r = googlecloud.query_table(bqservice,project_id,bq_query)
    bq_count = r[0][0]
    print "BQ Record Count:    " + str(bq_count)
    return bq_count
    
def test_sql_query(server, database, table, username, password, table_schema='dbo'):
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


bq_token = googlecloud.gcloud_connect(GC_SERVICE_ACCOUNT_EMAIL, GC_SECRET_KEY_PATH, 'https://www.googleapis.com/auth/bigquery')
bqservice = build('bigquery', 'v2', http=bq_token)
project_id = BQ_PROJECT
dataset_id = BQ_DATASET_PROD
target_table = 'test_table'

bq_count = test_bq_query(bqservice,project_id,dataset_id,target_table)


server = DB_SERVER
db = DB_NAME
username = DB_USER
password = DB_PASSWORD
table = 'test_table'

mssql_count = test_sql_query(server, db, table, username, password)

if str(mssql_count) != str(bq_count):
    print "AUDIT FAIL: BigQuery count not equal to SQL Server count"
    print "BQ:    " + str(bq_count)
    print "MSSQL: " + str(mssql_count)
else:
    print "Audit Passed!"
    print "BQ:    " + str(bq_count)
    print "MSSQL: " + str(mssql_count)

