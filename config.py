from dotenv import load_dotenv
import os
from botocore.exceptions import ClientError
import boto3

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
