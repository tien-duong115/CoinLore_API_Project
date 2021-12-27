import pandas as pd
import json
import time
from pandas_functions import prettyRedshiftProps
import config as c
from contextlib import redirect_stdout


# (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)
print("\n\n")
print(">>> Starting Redshift Cluster!\n>>> This is the configuration of redshift cluster!")
print("\n")

print(pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [c.DWH_CLUSTER_TYPE, c.DWH_NUM_NODES, c.DWH_NODE_TYPE, c.DWH_CLUSTER_IDENTIFIER, c.DWH_DB, c.DWH_DB_USER, c.DWH_DB_PASSWORD, c.DWH_PORT, c.DWH_IAM_ROLE_NAME]
             }))

print("\n\n")


ec2 = c.ec2
s3 = c.s3
iam = c.iam
redshift = c.redshift


try:
    print("1.1 Creating a new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=c.DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)
    
#1.1 Create the role, 
try:
    print("1.1 Creating a new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=c.DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)
    
    
print("1.2 Attaching Policy")

iam.attach_role_policy(RoleName=c.DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

print("1.3 Get the IAM role ARN")
roleArn = iam.get_role(RoleName=c.DWH_IAM_ROLE_NAME)['Role']['Arn']

print(f"\n\n>>> {roleArn}\n\n")

try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=c.DWH_CLUSTER_TYPE,
        NodeType=c.DWH_NODE_TYPE,
        NumberOfNodes=int(c.DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=c.DWH_DB,
        ClusterIdentifier=c.DWH_CLUSTER_IDENTIFIER,
        MasterUsername=c.DWH_DB_USER,
        MasterUserPassword=c.DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  )
except Exception as e:
    print(f"\n >>> {e} \n\n")
    

myClusterProps = redshift.describe_clusters(ClusterIdentifier=c.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

cluster_df = prettyRedshiftProps(myClusterProps)

check_status = cluster_df['Value'][2]
status = 'available'


while True:
    time.sleep(5)
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=c.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    cluster_df = prettyRedshiftProps(myClusterProps)
    check_status = cluster_df['Value'][2]
    if check_status == status:
        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        print("\n\n>>> Cluster successfully created!\n")
        print(f"\n>>> DWH_ENDPOINT :: {DWH_ENDPOINT}\n")
        print(f"\n>>> DWH_ROLE_ARN :: { DWH_ROLE_ARN}\n")
        break
    print('Cluster not up yet')


with open('.env_db', 'w') as file:
    with redirect_stdout(file):
        print(f'DWH_ENDPOINT={DWH_ENDPOINT}')
        print(f'DWH_ROLE_ARN={DWH_ROLE_ARN}')
        print(f'S3_BUCKET_NAME={c.S3_BUCKET_NAME}')
        print(f'DB_NAME={c.DWH_DB}')
        print(f'DWB_DB_USER={c.DWH_DB_USER}')
        print(f'DWB_DB_PASSWORD={c.DWH_DB_PASSWORD}')
        print(f'DWH_PORT={c.DWH_PORT}')
        
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(c.DWH_PORT),
        ToPort=int(c.DWH_PORT)
    )
except Exception as e:
    print(f"\n>>> {e}\n\n")
    
