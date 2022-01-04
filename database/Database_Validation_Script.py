import unittest
import psycopg2
from pyspark.sql.functions import row_number
from data_result_output import *
import pandas as pd
from db_main import DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, S3_BUCKET_NAME, HOST, DWH_ROLE_ARB, localhost, local_dbname, local_username, local_password
from create_table_sql import validation_queries


col_names = []
my_list = []
# conn = psycopg2.connect(host=f'{localhost}',database=f'{local_dbname}',user=f'{local_username}',password=f'{local_password}')

conn = psycopg2.connect(f"host={HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}")
cur = conn.cursor()
for query in validation_queries:
    cur.execute(query)
    colnames = [desc[0] for desc in cur.description]
    row_num = cur.fetchall()
    col_names.append(colnames)
    my_list.append(row_num)
    conn.commit()
conn.close()

coin_row, exchange_row, top_coin_row, historical_row, redshift_demo_query_row = my_list
coin_col, exchange_col, top_coin_col, historical_col, redshift_demo_query_col = col_names

coin_shape = (len(coin_row), len(coin_col))
exchange_shape = (len(exchange_row), len(exchange_col))
top_coin_shape = (len(top_coin_row), len(top_coin_col))
historical_shape = (len(historical_row), len(historical_col))

redshift_demo_query = pd.DataFrame(redshift_demo_query_row, columns=redshift_demo_query_col)
print("\n\n>>> Redshift Demo's query of top coins table and exchange table for analysis: \n".upper(), redshift_demo_query, "\n\n")
print("\n\n>>> Redshift DataBase staging table's shape result: ".upper(),coin_shape, exchange_shape, top_coin_shape, historical_shape, "\n\n")

class my_test(unittest.TestCase):    
    
    def test_1(self):
        self.assertEqual(coin_requests_s3_result, coin_shape, f"\n\n>>> Test coin_data_result_s3 result should be:{coin_requests_s3_result}")
    
    def test_2(self):
        self.assertEqual(exchange_data_s3_result, exchange_shape, f"\n\n>>> Test exchange_data_result_s3 result should be:{exchange_data_s3_result}")

    def test_3(self):
        self.assertEqual(historical_s3_result, historical_shape,f"\n\n>>> Test historical_data_result_s3 result should be:{historical_s3_result}")
    
    def test_4(self):
        self.assertEqual(top_coins_s3_result, top_coin_shape, f"\n\n>>> Test top_coins_data_result_s3 result should be: {top_coins_s3_result}")


if __name__ == '__main__':
    unittest.main()
    