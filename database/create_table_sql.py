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
TOP_COINS=os.getenv('TOP_COINS')
HISTORICAL_DATA=os.getenv('HISTORICAL_DATA')

# print('\n\n')
# print(DWH_ENDPOINT)
# print(DWH_ROLE_ARN)
# print(COINS_DATA)
# print(EXCHANGE_DATA)
# print(top_coins)
# print(HISTORICAL_DATA)
# print('\n\n')

coins_stage_table_drop = 'DROP TABLE IF EXISTS coins_data_table_stage'
top_coins_stage_table_drop = 'DROP TABLE IF EXISTS top_coins_table_stage'
exchange_stage_table_drop = 'DROP TABLE IF EXISTS exchange_data_table_stage'
historical_data_table_drop = 'DROP TABLE IF EXISTS historical_data_table_stage'


coins_stage_table_create = ("""
CREATE TABLE IF NOT EXISTS coins_data_table_stage
    (
        id INTEGER
        ,symbol VARCHAR
        ,name VARCHAR
        ,rank VARCHAR
        ,price_usd VARCHAR
        ,percent_change_24h VARCHAR
        ,percent_change_1h VARCHAR
        ,percent_change_7d VARCHAR
        ,price_btc VARCHAR
        ,market_cap_usd VARCHAR
        ,volume24 VARCHAR
        ,volume24a VARCHAR
        ,csupply VARCHAR
        ,tsupply VARCHAR
        ,msupply VARCHAR
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


top_coins_table_create = ("""
    CREATE TABLE IF NOT EXISTS top_coins_table_stage
    (
        name VARCHAR 
        ,base VARCHAR
        ,quote VARCHAR
        ,price VARCHAR
        ,price_usd VARCHAR
        ,volume VARCHAR
        ,volume_usd VARCHAR
        ,time VARCHAR
    );
""")


historical_data_table_create = ("""
    CREATE TABLE IF NOT EXISTS historical_data_table_stage
    (
        Unix VARCHAR 
        ,Date VARCHAR
        ,Symbol VARCHAR
        ,Open_price VARCHAR
        ,High VARCHAR
        ,Low VARCHAR
        ,Close VARCHAR
        ,Volume_bnb VARCHAR
        ,Volume_usdt VARCHAR
        ,Tradecount VARCHAR
    );
""")

copy_coins_data_to_redshift = f"""
COPY coins_data_table_stage
FROM '{COINS_DATA}'
CREDENTIALS 'aws_iam_role={DWH_ROLE_ARN}'
IGNOREHEADER 1
COMPUPDATE OFF
FILLRECORD
EMPTYASNULL
REMOVEQUOTES
DELIMITER ','
REGION 'us-west-2'
ESCAPE
"""

copy_top_coins_to_redshift = f"""
COPY top_coins_table_stage
FROM '{TOP_COINS}'
CREDENTIALS 'aws_iam_role={DWH_ROLE_ARN}'
IGNOREHEADER 1
COMPUPDATE OFF
REMOVEQUOTES
DELIMITER ','
REGION 'us-west-2'
ESCAPE
"""

copy_historical_data_to_redshift = f"""
COPY historical_data_table_stage
FROM '{HISTORICAL_DATA}'
CREDENTIALS 'aws_iam_role={DWH_ROLE_ARN}'
IGNOREHEADER 1
COMPUPDATE OFF
REMOVEQUOTES
DELIMITER ','
REGION 'us-west-2'
ESCAPE
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

drop_table_queries = [coins_stage_table_drop, exchange_stage_table_drop, top_coins_stage_table_drop, historical_data_table_drop]

create_table_queries = [coins_stage_table_create, exchange_stage_table_create, top_coins_table_create, historical_data_table_create]

copy_table_queries=[copy_coins_data_to_redshift, copy_exchange_data_to_redshift, copy_top_coins_to_redshift, copy_historical_data_to_redshift]


# print(f'\n\n{coins_stage_table_create}\n\n')

# print(f'{exchange_stage_table_create}\n\n')

# print(f'{top_coins_table_create}\n\n')


# for i in drop_table_queries:
#     print(i)
# for e in copy_table_queries:    
#     print(e)


