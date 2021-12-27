from dotenv import load_dotenv
from pathlib import Path
import os

dotenv_path = Path('.env_db')
load_dotenv(dotenv_path)


BUCKET_NAME = os.getenv('S3_BUDKET_NAME')
DWH_ROLE_ARN = os.getenv('DWH_ROLE_ARN')
DWH_ENDPOINT = os.getenv('DWH_ENDPOINT')


coins_stage_table_drop = 'DROP TABLE IF EXISTS coins_data_table'
market_stage_table_drop = 'DROP TABLE IF EXISTS market_data_table'
exchange_stage_table_drop = 'DROP TABLE IF EXISTS exchange_data_table'
binance_stage_table_drop = 'DROP TABLE IF EXISTS binance_data_table'


coins_stage_table_create = ("""
CREATE TABLE IF NOT EXISTS coins_data_table 
    (
        id INTEGER PRIMARY KEY sortkey,
        ,symbol VARCHAR NOT NULL
        ,name VARCHAR NOT NULL
        ,nameid VARCHAR NOT NULL
        ,rank FLOAT NOT NULL
        ,price_usd FLOAT NOT NULL
        ,percent_change_24h FLOAT NOT NULL
        ,percent_change_1h FLOAT NOT NULL
        ,percent_change_7d FLOAT NOT NULL
        ,price_btc FLOAT NOT NULL
        ,market_cap_usd VARCHAR NOT NULL
        ,volume24 FLOAT NOT NULL
        ,volume24a FLOAT NOT NULL
        ,csupply VARCHAR NOT NULL
        ,tsupply FLOAT NOT NULL
        ,msupply FLOAT NOT NULL
    );
""")


exchange_stage_table_create = ("""
CREATE TABLE IF NOT EXISTS exchange_data_table
    (
        id INTEGER PRIMARY KEY sortkey
        ,name VARCHAR NOT NULL
        ,name_id VARCHAR NOT NULL
        ,url VARCHAR NOT NULL
        ,country VARCHAR NOT NULL
        ,date_live VARCHAR NOT NULL
        ,date_added VARCHAR NOT NULL
        ,usdt INTEGER NOT NULL
        ,fiat INTEGER NOT NULL
        ,auto VARCHAR NOT NULL
        ,volume_usd FLOAT NOT NULL
        ,udate VARCHAR
        ,volume_usd_adj FLOAT NOT NULL
    );
""")



market_stage_table_create = ("""
    CREATE TABLE IF NOT EXISTS market_data_table
    (
        name VARCHAR PRIMARY KEY SORTKEY
        ,base VARCHAR NOT NULL
        ,quote VARCHAR NOT NULL
        ,price FLOAT NOT NULL
        ,price_usd FLOAT NOT NULL
        ,volume FLOAT NOT NULL
        ,volume_usd FLOAT NOT NULL
        ,time VARCHAR NOT NULL
    );
""")
binance_stage_table_create= ''


# insert_coin_data_table = """
# COPY coins_data_table FROM '{BUCKET_NAME}' 
# CREDENTIALS '{DWH_ROLE_ARN}'
# IGNOREHEADER 1
# delimiter ',' 
# blanksasnull
# ;
# """




print(f'\n\n{coins_stage_table_create}\n\n')

print(f'{exchange_stage_table_create}\n\n')

print(f'{market_stage_table_create}\n\n')