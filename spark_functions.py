from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import boto3
import config as c


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
    spark = c.spark()
    payload = spark.read.option('header', True).csv(data)
    payload = payload.select([F.col(col).alias(col.replace(' ','_')) for col in payload.columns])
    payload = payload.toDF(*[i.capitalize() for i in payload.columns])
    
    collect_columns=['Open', 'High', 'Low','Close', 'Volume_btc', 'Tradecount', 'Volume_usdt']
    
    for col1 in payload.columns:
        for col2 in collect_columns:
            if col1==col2:
                payload = payload.withColumn(col1, round(col1, 1))
    payload = payload.withColumnRenamed('Open', 'Open_market')

    return payload



def upload_to_s3(bucketname, local_file_path, s3_file_path):

    try:
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file(local_file_path, bucketname, s3_file_path)
        print('Success!')
    except Exception as e:
        print(f'{e}\n Fail!')
        

def download_from_s3(s3_bucket, s3_file_path, local_file_path):

    try:
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket(s3_bucket)
        my_bucket.download_file(s3_file_path, local_file_path)
        print('Success!')
    except Exception as e:
        print(f'{e}\n Fail!')