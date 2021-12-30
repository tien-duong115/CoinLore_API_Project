from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import boto3
import config as c


def binance_BtcUSDT_minute(data, column_to_keep):
    """[
        - Take data CSV format and manipulate the spark dataframe
        - Capitalize first letter of each column
        - Round float to ceil and single digit
        ]

    Args:
        data ([type]): [description]
        columns_to_keep:   LIST of columns to keep

    Returns:
        [type]: [description]
    """
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()
    
    payload = spark.read.option('header', True).csv(data)
    payload = payload.select([F.col(col).alias(col.replace(' ','_')) for col in payload.columns])
    payload = column_to_keep
    payload = payload.toDF(*[i.capitalize() for i in payload.columns])
    
    for col1 in payload.columns:
        for col2 in payload:
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