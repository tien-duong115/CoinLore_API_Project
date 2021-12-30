SELECT  round(cast(volume_usd_adj AS numeric),2) AS volume_usd_adj
       ,round(cast(volume_usd AS numeric),2)     AS volume_usd
FROM exchange_data_table_stage;




ALTER TABLE exchange_data_table_stage ALTER COLUMN date_live TYPE DATE USING(date_live::date)
,ALTER COLUMN id TYPE integer using(id::integer)
,ALTER COLUMN volume_usd TYPE integer using(volume_usd::double precision)
,ALTER COLUMN volume_usd_adj TYPE integer using(volume_usd_adj::double precision);

