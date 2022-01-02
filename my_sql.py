drop_exchange_table = DROP TABLE IF EXISTS exchange_data_table

create_exchange_table = CREATE TABLE if not exists exchange_data_table(name varchar, url varchar, country varchar, date_live date, volume_usd numeric, volume_usd_adj numeric);

insert_exchange_table = INSERT INTO exchange_data_table( name, url, country, date_live, volume_usd, volume_usd_adj)
SELECT  name
       ,url
       ,country
       ,replace(date_live,'00-00','01-01')::date AS date_live
       ,volume_usd::numeric
       ,volume_usd_adj::numeric
FROM exchange_data_table_stage;


drop_coins_table =  DROP TABLE IF EXISTS coins_data_table

create_coins_table = CREATE TABLE IF EXISTS coins_data_table(id integer, symbol varchar, name varchar, rank real, price_usd real, market_cap_usd real);

insert_coins_table = INSERT INTO demo(name, rank, market_cap_usd, price_usd)
SELECT  name
       ,rank
       ,market_cap_usd::numeric
       ,price_usd::numeric
FROM coins_data_table_stage;