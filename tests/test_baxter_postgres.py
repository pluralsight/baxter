import unittest
import sys
sys.path.append("/Users/dustinvannoy/dev/")
from baxter import googlecloud
from baxter import postgres
from apiclient.discovery import build
from test_config import (POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, GC_SERVICE_ACCOUNT_EMAIL,
                         GC_SECRET_KEY_PATH, BQ_PROJECT, BQ_DATASET_PROD, GS_BUCKET, BQ_TEST_DATA_PATH,
                         LOCAL_TEST_DATA_PATH)

run_bq_test = 0
run_postgres_test = 1


# class TestPostgresFunctions(unittest.TestCase):
#     def setUp(self):
#         pass


def test_postgres_cursor_to_json():
    if run_postgres_test:
        table = 'test_table'
        dest_file = LOCAL_TEST_DATA_PATH + table + '.json'
        with postgres.connect(POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD) as connection:
            sql = "Select from_plan_name, to_plan_name, insert_date insert_date, event_timestamp, clipviews_merged from etl.plan_merged where clipviews_merged = FALSE"
            cursor = postgres.run_sql(connection, sql)
            postgres.cursor_to_json(cursor, dest_file, dest_schema_file=None, source_schema_file=None)
        assert True

def sql_query(table):
    with postgres.connect(POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD) as connection:
        sql_query = "Select count(1) From " + table
        cursor = postgres.sql_get_query_data(connection, sql_query)
        sql_count = cursor.fetchone()[0]
        print "SQL Record Count: " + str(sql_count)
        connection.close()
        return sql_count

# def test_postgres_1():
#     if run_postgres_test:
#         table = 'sandbox.test_table'
#         count = sql_query(table)
#         assert count > 0

def test_bq_load_table_from_json_1():
    bq_token = googlecloud.gcloud_connect(GC_SERVICE_ACCOUNT_EMAIL, GC_SECRET_KEY_PATH, 'https://www.googleapis.com/auth/bigquery')
    bqservice = build('bigquery', 'v2', http=bq_token)
    project_id = BQ_PROJECT
    dataset_id = BQ_DATASET_PROD
    target_table = 'test_table'
    source_file = BQ_TEST_DATA_PATH + 'test_table_data.json'
    googlecloud.load_table_from_json(bqservice, project_id, dataset_id, target_table, source_file, field_list=None, overwrite=False)
    assert True

if __name__ == "__main__":
    test_postgres_cursor_to_json()
    # test_bq_load_table_from_json_1()

# if __name__ == '__main__':
#     unittest.main()