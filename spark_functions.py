from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import boto3


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


def create_boto3_session(aws_key, aws_secret_key):
    session = boto3.Session\
    (
        aws_access_key_id=aws_key,
        aws_secret_access_key= aws_secret_key
     )
    return session


def upload_to_s3(bucketname, local_file_path, s3_file_path):

    s3 = boto3.resource('s3')
    try:
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