from dotenv import load_dotenv
from pathlib import Path
import os

dotenv_path = Path('.env_db')
load_dotenv(dotenv_path)


BUCKET_NAME = os.getenv('S3_BUDKET_NAME')
DWH_ROLE_ARN = os.getenv('DWH_ROLE_ARN')
DWH_ENDPOINT = os.getenv('DWH_ENDPOINT')


coins_stage_table_drop = 'DROP TABLE IF EXISTS coins_data_table_stage'
market_stage_table_drop = 'DROP TABLE IF EXISTS market_data_table_stage'
exchange_stage_table_drop = 'DROP TABLE IF EXISTS exchange_data_table_stage'
binance_stage_table_drop = 'DROP TABLE IF EXISTS binance_data_table_stage'


coins_stage_table_create = ("""
CREATE TABLE IF NOT EXISTS coins_data_table_stage
    (
        id INTEGER 
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


exchange_stage_table_create = ("""
CREATE TABLE IF NOT EXISTS exchange_data_table_stage
    (
        id INTEGER 
        ,name VARCHAR
        ,name_id VARCHAR
        ,url VARCHAR
        ,country VARCHAR
        ,date_live VARCHAR
        ,date_added VARCHAR
        ,usdt INTEGER
        ,fiat INTEGER
        ,auto VARCHAR
        ,volume_usd FLOAT
        ,udate VARCHAR
        ,volume_usd_adj FLOAT
    );
""")


market_stage_table_create = ("""
    CREATE TABLE IF NOT EXISTS market_data_table_stage
    (
        name VARCHAR 
        ,base VARCHAR
        ,quote VARCHAR
        ,price FLOAT
        ,price_usd FLOAT
        ,volume FLOAT
        ,volume_usd FLOAT
        ,time VARCHAR
    );
""")

binance_stage_table_create= ("""
    CREATE TABLE IF NOT EXISTS binanca_data_table_stage
    (
        unix VARCHAR
        ,date VARCHAR
        ,symbol VARCHAR
        ,open VARCHAR
        ,high
        ,low
        ,Volume
    );

""")


# insert_coin_data_table = """
# COPY coins_data_table FROM '{BUCKET_NAME}' 
# CREDENTIALS '{DWH_ROLE_ARN}'
# IGNOREHEADER 1
# delimiter ',' 
# blanksasnull
# ;
# """




# print(f'\n\n{coins_stage_table_create}\n\n')

# print(f'{exchange_stage_table_create}\n\n')

# print(f'{market_stage_table_create}\n\n')

drop_table_queries = [coins_stage_table_drop, exchange_stage_table_drop, market_stage_table_drop, binance_stage_table_drop]
create_table_queries = [coins_stage_table_create, exchange_stage_table_create, market_stage_table_create, coins_stage_table_create]
for i in drop_table_queries:
    print(i)
for e in create_table_queries:    
    print(e)


