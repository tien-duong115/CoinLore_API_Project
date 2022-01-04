import unittest
import psycopg2
from data_result_output import *
import pandas as pd
from db_main import DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, S3_BUCKET_NAME, HOST, DWH_ROLE_ARB, localhost, local_dbname, local_username, local_password
from create_table_sql import validation_queries


my_list = []

# conn = psycopg2.connect(host=f'{localhost}',database=f'{local_dbname}',user=f'{local_username}',password=f'{local_password}')

conn = psycopg2.connect(f"host={HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}")
cur = conn.cursor()
for query in validation_queries:
    cur.execute(query)
    row = cur.fetchall()
    my_list.append(row)
    conn.commit()

conn.close()
coin_data, exchange_data, top_coins_data, historical_data, redshift_demo_query = my_list
coin_data = pd.DataFrame(coin_data).shape
exchange_data = pd.DataFrame(exchange_data).shape
top_coins_data = pd.DataFrame(top_coins_data).shape
historical_data = pd.DataFrame(historical_data).shape

redshift_demo_query = pd.DataFrame(redshift_demo_query)

print("\n\n>>> Redshift Demo's query of top coins table and exchange table for analysis: \n".upper(), redshift_demo_query, "\n\n")


print("\n\n>>> Redshift DataBase staging table's shape result: ".upper(), coin_data, exchange_data,top_coins_data, historical_data, "\n\n")


class my_test(unittest.TestCase):    
    
    def test_1(self):
        self.assertEqual(coin_requests_s3_result, coin_data, f"\n\n>>> Test coin_data_result_s3 result should be:{coin_requests_s3_result}")
    
    def test_2(self):
        self.assertEqual(exchange_data_s3_result, exchange_data, f"\n\n>>> Test exchange_data_result_s3 result should be:{exchange_data_s3_result}")

    def test_3(self):
        self.assertEqual(historical_s3_result, historical_data,f"\n\n>>> Test historical_data_result_s3 result should be:{historical_s3_result}")
    
    def test_4(self):
        self.assertEqual(top_coins_s3_result, top_coins_data, f"\n\n>>> Test top_coins_data_result_s3 result should be: {top_coins_s3_result}")


if __name__ == '__main__':
    unittest.main()
    