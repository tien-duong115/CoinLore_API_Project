# /mnt/c/Users/tienl/Udacity_Courses/DE_capstone/capstone_venv/bin/python3.8
from dotenv import load_dotenv
from pathlib import Path
import os
my_path = Path('/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/.env')

load_dotenv()
load_dotenv(my_path)

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
DWH_ROLE_ARN = os.getenv('DWH_ROLE_ARN')
DWH_ENDPOINT = os.getenv('DWH_ENDPOINT')
COINS_DATA = os.getenv('COINS_DATA')
EXCHANGE_DATA = os.getenv('EXCHANGE_DATA')
TOP_COINS = os.getenv('TOP_COINS')
HISTORICAL_DATA = os.getenv('HISTORICAL_DATA')

coins_stage_table_drop = 'DROP TABLE IF EXISTS coins_data_table_stage'
top_coins_stage_table_drop = 'DROP TABLE IF EXISTS top_coins_table_stage'
exchange_stage_table_drop = 'DROP TABLE IF EXISTS exchange_data_table_stage'
historical_data_table_stage_drop = 'DROP TABLE IF EXISTS historical_data_table_stage'

drop_exchange_table = """DROP TABLE IF EXISTS exchange_data_table"""
drop_coins_table = """DROP TABLE IF EXISTS coins_data_table"""
drop_historical_table = """ DROP TABLE IF EXISTS historical_data_table"""
drop_top_coins_table = """ DROP TABLE IF EXISTS top_coins_data_table"""
drop_bridge_table = """ DROP TABLE IF EXISTS bridge_table"""


coins_stage_table_create = ("""
CREATE TABLE IF NOT EXISTS coins_data_table_stage
    (
        id VARCHAR
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


top_coins_stage_table_create = ("""
    CREATE TABLE IF NOT EXISTS top_coins_table_stage
    (   id  integer 
        ,name VARCHAR
        ,base VARCHAR
        ,quote VARCHAR
        ,price varchar
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
FILLRECORD
EMPTYASNULL
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
COPY exchange_data_table_stage
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

"""
>>> POSTGRRES SQL LOCAL DATABASE QUERY
- Uncommented to test within local
"""
# copy_coins_data_to_redshift = f"""
# COPY coins_data_table_stage
# FROM '/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/data/coins_data.csv'
# DELIMITER ','
# HEADER
# CSV ;
# """
# copy_top_coins_to_redshift = f"""
# COPY top_coins_table_stage
# FROM '/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/data/top_coins.csv'
# HEADER
# DELIMITER ','
# CSV ;
# """

# copy_historical_data_to_redshift = f"""
# COPY historical_data_table_stage
# FROM '/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/data/final_historical_data.csv'
# HEADER
# DELIMITER ','
# CSV ;
# """

# copy_exchange_data_to_redshift = f"""
# COPY exchange_data_table_stage
# FROM '/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/data/coin_exchange_info.csv'
# HEADER
# DELIMITER ','
# CSV ;
# """


create_exchange_table = """CREATE TABLE if not exists exchange_data_table(id integer primary key sortkey distkey, name varchar, url varchar, country varchar, date_live date, volume_usd numeric, volume_usd_adj numeric);"""

insert_exchange_table = """INSERT INTO exchange_data_table( id, name, url, country, date_live, volume_usd, volume_usd_adj)
SELECT  id::numeric
       ,name
       ,url
       ,country
       ,replace(date_live,'00-00','01-01')::date AS date_live
       ,volume_usd::numeric
       ,volume_usd_adj::numeric
FROM exchange_data_table_stage;"""


create_coins_table = """CREATE TABLE IF NOT EXISTS coins_data_table( id integer primary key sortkey distkey, symbol varchar, name varchar, rank float, market_cap_usd float, price_usd float, price_btc float);"""

insert_coins_table = """INSERT INTO coins_data_table(id, symbol, name, rank , market_cap_usd, price_usd, price_btc)
SELECT  id::integer
       ,symbol
       ,name
       ,rank::float
       ,replace(market_cap_usd,'0?', '0.0')::float
       ,price_usd::float
       ,price_btc::float
FROM coins_data_table_stage;"""


create_historical_table = """ CREATE TABLE IF NOT EXISTS historical_data_table(unix varchar, date date distkey sortkey, symbol varchar, open_price float, high float, low float, close float, volume_bnb float, volume_usdt float, tradecount integer)
"""

insert_historical_table = """ INSERT INTO historical_data_table(unix, date, symbol, open_price, high, low, close, volume_bnb, volume_usdt, tradecount)
SELECT  unix::varchar
       ,date::date 
       ,split_part(symbol,'/',1) AS symbol
       ,open_price::float
       ,high::float
       ,low::float
       ,close::float
       ,volume_bnb::float
       ,volume_usdt::float
       ,tradecount::integer
FROM historical_data_table_stage
"""

create_top_coins_table = """CREATE TABLE IF NOT EXISTS top_coins_data_table( id integer primary key distkey sortkey, name varchar, base varchar, quote varchar, price float, price_usd float, volume float, volume_usd float, time date)
"""

insert_top_coins_table = """ INSERT INTO top_coins_data_table( id, name, base, quote, price, price_usd, volume, volume_usd, time)
SELECT  id::integer
       ,name
       ,base
       ,quote
       ,price::float
       ,price_usd::float
       ,volume::float
       ,volume_usd::float
       ,time::date
FROM top_coins_table_stage
"""


create_bridge_table = """
CREATE TABLE if not exists bridge_table(coin_names varchar primary key sortkey distkey)"""

insert_bridge_table = """
INSERT INTO bridge_table(coin_names)
SELECT  distinct symbol
FROM historical_data_table"""

### Use this for Redshift database
redshift_Validation_query = """
SELECT  t.name
       ,t.base AS coin_symbol
       ,t.price
       ,t.price_usd
       ,e.url
       ,e.country
       ,e.date_live
       ,t.volume
FROM top_coins_data_table AS t
JOIN exchange_data_table_stage AS e
ON t.name = e.name
"""

### Use this if working with local database
local_Validation_query = """
SELECT  t.name
       ,t.base AS coin_symbol
       ,t.price
       ,t.price_usd
       ,e.url
       ,e.country
       ,e.date_live
       ,t.volume
FROM top_coins_table_stage AS t
JOIN exchange_data_table_stage AS e
ON t.name = e.name
"""

validation_coins_stage_table = """
select * from coins_data_table_stage
"""

validation_exchange_stage_table = """
select * from exchange_data_table_stage
"""

validation_historical_table_stage = """
select * from historical_data_table_stage
"""

validation_top_coins_table_stage = """
select * from top_coins_table_stage
"""


coins_stage_table_drop = 'DROP TABLE IF EXISTS coins_data_table_stage'
top_coins_stage_table_drop = 'DROP TABLE IF EXISTS top_coins_table_stage'
exchange_stage_table_drop = 'DROP TABLE IF EXISTS exchange_data_table_stage'
historical_data_table_stage_drop = 'DROP TABLE IF EXISTS historical_data_table_stage'

# validation_queries = [validation_coins_stage_table, validation_exchange_stage_table, validation_top_coins_table_stage, validation_historical_table_stage, redshift_Validation_query]
validation_queries = [validation_coins_stage_table, validation_exchange_stage_table, validation_top_coins_table_stage, validation_historical_table_stage, redshift_Validation_query]


drop_table_queries = [coins_stage_table_drop, exchange_stage_table_drop, top_coins_stage_table_drop, historical_data_table_stage_drop,
                      drop_exchange_table, drop_coins_table, drop_historical_table, drop_top_coins_table, drop_bridge_table]
create_table_queries = [coins_stage_table_create, exchange_stage_table_create, top_coins_stage_table_create, historical_data_table_create,
                        create_coins_table, create_exchange_table, create_historical_table, create_top_coins_table, create_bridge_table]
copy_table_queries = [copy_coins_data_to_redshift, copy_exchange_data_to_redshift,
                      copy_top_coins_to_redshift, copy_historical_data_to_redshift]
insert_table_queries = [insert_top_coins_table, insert_coins_table,
                        insert_historical_table, insert_exchange_table, insert_bridge_table]
