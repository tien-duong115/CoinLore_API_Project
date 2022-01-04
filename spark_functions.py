from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import boto3
import config as c


def binance_coins_data(data, output):
    """[
        - Take data CSV format and manipulate the spark dataframe
        - Capitalize first letter of each column
        - Round float to ceil and single digit
        ]

    Args:
        data ([type]): [description]
        columns_to_keep:   LIST of columns to keep

    Returns:
        [None]: [Exported out file only]
    """
    try:
        spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().getOrCreate()
        payload = spark.read.format("csv").option("header", True).load(data)
        payload = payload.select([F.col(col).alias(col.replace(' ','_')) for col in payload.columns])
        payload = payload.toDF(*[i.capitalize() for i in payload.columns])
        payload = payload.withColumnRenamed('Open', 'Open_price')
        payload.write.csv(output, mode='overwrite', header=True)
        print(f"\nSucessfully export file to: {output}!\n\n")
        # return payload
    except Exception as e:
        print(e)
        
    
def upload_to_s3(bucketname, local_file_path, s3_file_path):

    try:
        c.s3.meta.client.upload_file(local_file_path, bucketname, s3_file_path)
        print('Success!')
    except Exception as e:
        print(f'{e}\n Fail to exported to S3!\n')
        

def download_from_s3(s3_bucket, s3_file_path, local_file_path):

    try:
        my_bucket = c.s3.Bucket(s3_bucket)
        my_bucket.download_file(s3_file_path, local_file_path)
        print('Success!')
    except Exception as e:
        print(f'{e}\n Fail to download from S3!')