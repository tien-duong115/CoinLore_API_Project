from os import write
import requests
from requests.api import request
import pandas as pd
import csv, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F



def get_request(url):
    """[
        - Generate Request from input Url
        ]
    Args:
        url ([type]): [description]
    Returns:
        [type]: [description]
    """
    payload = requests.get(url)
    return payload.json()


def get_coin_request(start=0, limit=100):
    """[
        - Generate requests from API
        - return out as Pandas DataFrame
        ]
    Args:
        start (int, optional): [description]. Defaults to 0.
        limit (int, optional): [description]. Defaults to 100.

    Returns:
        [type]: [description]
    """
    payload = pd.DataFrame()
    for i in range(start,limit,100):
        r = requests.get(f'https://api.coinlore.net/api/tickers/?start={i}').json()
        entry = r['data']
        for line in entry:
            payload = payload.append(line, ignore_index=True)
            print(f"Successfully request number {i}")
    return payload


def get_coin_market_request(HowMany=10, DataPath=''):
    """[
        - GET market of top coins
        - Return Pandas DataFrame
        ]   
    Args:
        HowMany (int, optional): [description]. Defaults to 10.
        DataPath (str, optional): [description]. Defaults to ''.
    """
    payload=pd.read_csv(DataPath)
    finalPayload = pd.DataFrame()
    
    df = payload.copy()
    
    df['rank'] = df['rank'].astype(int)
    sort_by_rank = df.sort_values(by='rank')
    top_coins = sort_by_rank.head(HowMany)
    list_of_top_coins = list(top_coins.id)
    
    for i in list_of_top_coins:
        r = requests.get(f'https://api.coinlore.net/api/coin/markets/?id={i}').json()
        for line in r:
            finalPayload = finalPayload.append(line, ignore_index=True)
    finalPayload['time'] = pd.to_datetime(finalPayload['time'], unit='s')
    
    return finalPayload
       
        
def export_csv(payload,path):
    """[Used Pandas to export out as CSV format]

    Args:
        payload ([dataframe]): [data to be export]
        path ([None]): [path to be export]
    """
    payload.to_csv(path)
    print(f'exported to {path}!')


def exchange_data_filter(data):
    """[Expect incoming data as Pandas dataframe and return DF as output]
    Args:
        data ([json]): [return exchange data records with country filtered]
    """
    payload = pd.DataFrame.from_dict(data.values())
    payload = payload.astype({'id':int}).copy()
    payload = df = payload[['id','name', 'name_id', 'url','country','date_live', 'date_added', 'usdt','fiat','auto','volume_usd','udate','volume_usd_adj']].sort_values(by=['id']).set_index('id').query("country != '' ")
    return payload


def create_SparkSession():
    """[summary]

    Returns:
        [type]: [description]
    """
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()
    return spark



def binance_BtcUSDT_minute(data):
    """[
        - Take data CSV format and manipulate the spark dataframe
        - Capitalize first letter of each column
        - Round float to ceil and single digit
        ]

    Args:
        data ([type]): [description]

    Returns:
        [type]: [description]
    """
    spark = create_SparkSession()
    payload = spark.read.option('header', True).csv(data)
    payload = payload.select([F.col(col).alias(col.replace(' ','_')) for col in payload.columns])
    payload = payload.toDF(*[i.capitalize() for i in payload.columns])
    
    collect_columns=['Open', 'High', 'Low','Close', 'Volume_btc', 'Tradecount', 'Volume_usdt']
    
    for col1 in payload.columns:
        for col2 in collect_columns:
            if col1==col2:
                payload = payload.withColumn(col1, round(col1, 1))
    return payload



