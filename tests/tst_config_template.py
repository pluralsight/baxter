import json

LOGIN_CREDENTIALS_PATH = 'config/credentials.json'

GC_SERVICE_ACCOUNT_EMAIL = '<add-google-service-account@developer.gserviceaccount.com>'
GC_SECRET_KEY_PATH = 'config/client_secrets.p12'
BQ_PROJECT = '<BigQuery Project Id>'
BQ_DATASET_STAGE = '<BigQuery Dataset (Stage)>'
BQ_DATASET_PROD = '<BigQuery Dataset (Prod)>'
GS_BUCKET = '<Google Cloud Storage Bucket name>'

DB_CRED_FILE = 'config/databaselogin.config'
DB_SERVER = '0.0.0.0'  #Add your SQL Server IP
DB_NAME = '<database name>'  #Add the database/schema name

with open(DB_CRED_FILE,'rb') as cred:
    db_info = json.loads(cred.read())
    DB_USER = db_info['local_sql_server']['username']
    DB_PASSWORD = db_info['local_sql_server']['password']
