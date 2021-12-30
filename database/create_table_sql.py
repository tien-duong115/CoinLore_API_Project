#/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/capstone_venv/bin/python3.8
from dotenv import load_dotenv
from pathlib import Path
import os
my_path = Path('/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/.env')

load_dotenv()
load_dotenv(my_path)

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
DWH_ROLE_ARN = os.getenv('DWH_ROLE_ARN')
DWH_ENDPOINT = os.getenv('DWH_ENDPOINT')
COINS_DATA=os.getenv('COINS_DATA')
EXCHANGE_DATA=os.getenv('EXCHANGE_DATA')
MARKET_DATA=os.getenv('MARKET_DATA')
BINANCE_DATA=os.getenv('BINANCE_DATA')

# print('\n\n')
# print(DWH_ENDPOINT)
# print(DWH_ROLE_ARN)
# print(COINS_DATA)
# print(EXCHANGE_DATA)
# print(MARKET_DATA)
# print(BINANCE_DATA)
# print('\n\n')

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
        id VARCHAR 
        ,name VARCHAR
        ,url VARCHAR
        ,country VARCHAR
        ,date_live VARCHAR
        ,usdt VARCHAR
        ,fiat VARCHAR
        ,auto VARCHAR
        ,volume_usd VARCHAR
        ,udate VARCHAR
        ,volume_usd_adj VARCHAR
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
FROM '{COINS_DATA}'
CREDENTIALS 'aws_iam_role={DWH_ROLE_ARN}'
IGNOREHEADER 1
COMPUPDATE OFF
DELIMITER ','
REGION 'us-west-2'
"""

copy_market_data_to_redshift = f"""
COPY market_data_table_stage
FROM '{MARKET_DATA}'
CREDENTIALS 'aws_iam_role={DWH_ROLE_ARN}'
IGNOREHEADER 1
COMPUPDATE OFF
DELIMITER ','
REGION 'us-west-2'
"""

copy_binance_data_to_redshift = f"""
COPY binance_data_table_stage
FROM '{BINANCE_DATA}'
CREDENTIALS 'aws_iam_role={DWH_ROLE_ARN}'
IGNOREHEADER 1
COMPUPDATE OFF
DELIMITER ','
REGION 'us-west-2'
"""

copy_exchange_data_to_redshift = f"""
COPY exchange_data_table_stage(id, name, url, country, date_live, usdt, fiat,auto, volume_usd, udate, volume_usd_adj)
FROM '{EXCHANGE_DATA}'
CREDENTIALS 'aws_iam_role={DWH_ROLE_ARN}'
IGNOREHEADER 1
FILLRECORD
EMPTYASNULL
REMOVEQUOTES
DELIMITER ','
REGION 'us-west-2'
ESCAPE
"""

alter_exchange_table="""
ALTER table exchange_data_table_stage
ALTER COLUMN date_live TYPE DATE USING(date_live::date),
ALTER COLUMN id TYPE integer using(id::integer),
ALTER COLUMN volume_usd TYPE integer using(volume_usd::double precision),
ALTER COLUMN volume_usd_adj TYPE integer using(volume_usd_adj::double precision)
"""

drop_table_queries = [coins_stage_table_drop, exchange_stage_table_drop, market_stage_table_drop, binance_stage_table_drop]

create_table_queries = [coins_stage_table_create, exchange_stage_table_create, market_stage_table_create, binance_stage_table_create]

copy_table_queries=[copy_coins_data_to_redshift, copy_exchange_data_to_redshift, copy_market_data_to_redshift, copy_binance_data_to_redshift]


# print(f'\n\n{coins_stage_table_create}\n\n')

# print(f'{exchange_stage_table_create}\n\n')

# print(f'{market_stage_table_create}\n\n')


# for i in drop_table_queries:
#     print(i)
# for e in copy_table_queries:    
#     print(e)


