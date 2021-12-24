from dotenv import load_dotenv
import os
from botocore.exceptions import ClientError
import boto3
import time
import pandas as pd
from pandas_functions import prettyRedshiftProps
import config as c

def main():
    
    status = 'deleting'
    c.redshift.delete_cluster( ClusterIdentifier=c.DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    c.iam.detach_role_policy(RoleName=c.DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    c.iam.delete_role(RoleName=c.DWH_IAM_ROLE_NAME)
    while True:
        time.sleep(5)
        myClusterProps = c.redshift.describe_clusters(ClusterIdentifier=c.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        cluster_df = prettyRedshiftProps(myClusterProps)
        check_status = cluster_df['Value'][2]
        # check_status = cluster_df['Value'][2]
        delete_time = 0
        try:
            if check_status == status:
                delete_time += 5
                print(f'\n\nDeleting Cluster: {delete_time} seconds.\n\n')
                print(f'\n\n>>> Cluster status: {check_status}!\n')
        except Exception as e:
            if e == e:
                print('\n\n>>> Cluster deleted!\n')
                break

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        if e ==e:
            print('\n\n>>> No cluster found!\n>>> Cluster deleted!\n\n')