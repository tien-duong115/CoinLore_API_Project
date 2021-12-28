#/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/capstone_venv/bin/python3.8
from dotenv import load_dotenv
from pathlib import Path
import os
os.chdir("/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/database")
db_env_path = Path('.env')
load_dotenv(db_env_path)


BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
DWH_ROLE_ARN = os.getenv('DWH_ROLE_ARN')
DWH_ENDPOINT = os.getenv('DWH_ENDPOINT')
COINS_DATA=os.getenv('COINS_DATA')

coins_stage_table_drop = 'DROP TABLE IF EXISTS coins_data_table_stage'
market_stage_table_drop = 'DROP TABLE IF EXISTS market_data_table_stage'
exchange_stage_table_drop = 'DROP TABLE IF EXISTS exchange_data_table_stage'
binance_stage_table_drop = 'DROP TABLE IF EXISTS binance_data_table_stage'


coins_stage_table_create = ("""
CREATE TABLE IF NOT EXISTS coins_data_table_stage
    (
        id  INTEGER
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


binance_stage_table_create = ("""
    CREATE TABLE IF NOT EXISTS binance_data_table_stage
    (
        Unix VARCHAR 
        ,Date timestamp
        ,Symbol VARCHAR
        ,Open_market VARCHAR
        ,High FLOAT
        ,Low FLOAT
        ,Close FLOAT
        ,Volume_btc FLOAT
        ,Volume_usdt FLOAT
        ,Tradecount INTEGER
    );
""")



copy_coins_data_to_redshift = f"""
COPY coins_data_table_stage
FROM '{DWH_ROLE_ARN}'
CREDENTIALS '{COINS_DATA}'
COMPUPDATE OFF
DELIMITER ','
REGION 'us-west-2'
"""

# copy_coin_data_table = """
# COPY coins_data_table FROM '{BUCKET_NAME}' 
# CREDENTIALS '{DWH_ROLE_ARN}'
# IGNOREHEADER 1
# delimiter ',' 
# blanksasnull
# ;
# """

drop_table_queries = [coins_stage_table_drop, exchange_stage_table_drop, market_stage_table_drop, binance_stage_table_drop]

create_table_queries = [coins_stage_table_create, exchange_stage_table_create, market_stage_table_create, binance_stage_table_create]


# print(f'\n\n{coins_stage_table_create}\n\n')

# print(f'{exchange_stage_table_create}\n\n')

# print(f'{market_stage_table_create}\n\n')


# for i in drop_table_queries:
#     print(i)
# for e in create_table_queries:    
#     print(e)


