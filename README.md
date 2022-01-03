                                # coinlore_api_project
## README Project Overview

As part of Data-Engineer Udacity-CapStone project to apply concept learn within the course such as AWS services, relational database systems, and scheduling services. With the current popularity crypto-currencie. To help user keeping up to date of new coins being created and different exchanges services. I created an data pipeline from coinlore api service and hosted it on AWS services including dataware house storage system.Coinlore is an open public API that let user request for cryptocurrencies and exchanges information. This project current lack many feature in supporting real-time situation data. Intergrating features such as data migration tools to keeping track of different versions and migration control. Furthermore, the project can also leverages of scheduling services to refresh the database with the new data inserted. If the current project requires larger space and higher compute power, we can use HDFS on top of Redshift cluster and process the data with spark.

## How to use

                    #### Requirement: AWS KEY and AWS SECRET KEY Access
Download historical_data_temp.csv and set into data DIR.

   1. close repo
   2. create virtual environment 
   3. pip install all dependencies within `requirements.txt`
   4. run start_redshift.py
   5. run main.py
   6. run db_main.py
   7. run stop_redshift.py (close cluster)

## Pipeline Steps
- Build ETL pipeline from coinlore open API of cryptopcurrencies
- Transform requested data into CSV format and manipulate the data with pyspark, pandas, and SQL
- Stores the requested data into S3 bucket as temperorary file's storage
- Create a pipeline to transfer CSV file format into RedShift serves as dataware
- Stages data into Redshift AWS as staging table
- Use staging tables to create database as relational Star Schema concept

#### Tool used
- *Python*
- *Power bi*
- *Jupyter Notebook*
- *Postgres sql*
- *AWS S3*
- *AWS Redshift*

## Data Information

**Coins data: s3://tien-duong1151/data/coins_data.csv**
- Retreived from coinlore API, consisted of coin symbols, full name, rank, market cap and price in USD

**Exchange data: s3://tien-duong1151/data/coin_exchange_info.csv**
- Retreived from coinlore API, information on exchanges of cryptocurrencies broker. Consisted of exchange full name, URL, country exchange base at, and date exchange went live

Top coins: s3://ten-duong1151/data/top_coins.csv
- Retreived from coinlore API, use rank base on `exchange data` and request as user's desire number of top coins to be returned. Consisted of coin full name, ability to trade with, price, and volume

Historical data: s3://tien-duong1151/data/final_historical_data.csv
- Large dataset retrieved from `https://www.cryptodatadownload.com/`. Under Binance cryptocurrencies broker. Consited of historical data of ETH, BTC and LTC by minutes along with tradecount, volume, and price.


### Current Project Files
 
 1. notebook.ipynb  ==> Combination of scripts along with output and data model schema structure.
 
 2. start_redshift.py ==> Using Pyhton boto3 to create AWS redshift, IAM role, and Port connection. Will check until Cluster OPEN

 3. stop_redshift.py ==> Stop Redshift Cluster script, will check until cluster CLOSE

 4. database DIR --
        -- create_table_sql.py  ==> contain SQL queries to DROP, CREATE, INSERT data into S3 and Redshift
        -- db_main.py ==> pipeline script to run SQL queries

 5. main.py ==> Pipeline for data cleaning, and export into S3

 6. requirements.txt ==> libraries installed require for the project

 7. config.py   ==> configuration variable references from .env which NOT included along with repo

 8. spark_functions.py ==>   Functions in pysaprk to import and transform data

 9. pandas_functions.py ==> Function in pandas to import, transform and export data

 10. data_schema.pbix    ==> visualization of datasets along with table schema


 References:
 `https://www.cryptodatadownload.com/`
 'https://www.coinlore.com/cryptocurrency-data-api'