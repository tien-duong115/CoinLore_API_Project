from dotenv import load_dotenv
from pathlib import Path
import os

dotenv_path = Path('.env_db')
load_dotenv(dotenv_path)


BUCKET_NAME = os.getenv('S3_BUDKET_NAME')
DWH_ROLE_ARN = os.getenv('DWH_ROLE_ARN')
DWH_ENDPOINT = os.getenv('DWH_ENDPOINT')


coins_stage_table_drop = 'DROP TABLE IF EXISTS coins_data_table'
coins_stage_table_create = ("""
CREATE TABLE IF NOT EXISTS coins_data_table (
id FLOAT
,symbol VARCHAR 
,name VARCHAR
,nameid VARCHAR
,rank FLOAT
,price_usd FLOAT
,percent_change_24h FLOAT
,percent_change_1h FLOAT
,percent_change_7d FLOAT
,price_btc FLOAT
,market_cap_usd VARCHAR
,volume24 FLOAT
,volume24a FLOAT
,csupply VARCHAR
,tsupply FLOAT
,msupply FLOAT
);
""")


insert_coin_data_table = """
COPY coins_data_table FROM '{BUCKET_NAME}' 
CREDENTIALS '{DWH_ROLE_ARN}'
IGNOREHEADER 1
delimiter ',' 
blanksasnull
;
"""


print(f'\n\n{coins_stage_table_drop}\n\n')

print(f'{coins_stage_table_create}\n\n')

print(f'{insert_coin_data_table}\n\n')