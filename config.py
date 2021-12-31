#!/mnt/c/Users/tienl/Udacity_Courses/DE_capstone/capstone_venv/bin/python3.8
from dotenv import load_dotenv
import os
import boto3
from pyspark.sql import SparkSession

load_dotenv()

KEY                    = os.getenv('AWS_ACCESS_KEY_ID')
SECRET                 = os.getenv('AWS_SECRET_ACCESS_KEY')

DWH_CLUSTER_TYPE       = os.getenv("DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = os.getenv("DWH_NUM_NODES")
DWH_NODE_TYPE          = os.getenv("DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = os.getenv("DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = os.getenv("DWH_DB")
DWH_DB_USER            = os.getenv("DWH_DB_USER")
DWH_DB_PASSWORD        = os.getenv("DWH_DB_PASSWORD")
DWH_PORT               = os.getenv("DWH_PORT")

DWH_IAM_ROLE_NAME      = os.getenv("DWH_IAM_ROLE_NAME")

DB_HOST_NAME = os.getenv("HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
S3_BUCKET_NAME=os.getenv('S3_BUCKET_NAME')
COINS_DATA=os.getenv('COINS_DATA')

ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )



session = boto3.Session(
   aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
   aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY'))


