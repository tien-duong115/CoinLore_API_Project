from os import path
from requests.api import get
from pandas_functions import *
from spark_functions import *
import pandas as pd
import os
from dotenv import load_dotenv



def main():
    """[
        - Data pipelines to extract, tranform and load data into S3 bucket
        ]
    """
    
    load_dotenv()
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY')
    s3_bucket_name = os.getenv('S3_BUCKET_NAME')
        
    # Create S3 session
    bucket_name=s3_bucket_name
    create_boto3_session(aws_key=aws_access_key_id, aws_secret_key=aws_secret_access_key)
    
    # file paths
    coin_path = 'data/coins_data.csv'
    coin_market_path = 'data/coin_market_info.csv'
    coin_exchange_path = 'data/coin_exchange_info.csv'
    binance_btc_file = 'data/binance_btc.csv' 
    
    # Exchange data pipeline
    GET_all_exchanges = 'https://api.coinlore.net/api/exchanges/'
    exchange_requests = get_request(GET_all_exchanges)
    exchange_data = exchange_data_filter(exchange_requests)
    export_csv(exchange_data,coin_exchange_path)
    print("Sucessfully get data and export out to {coin_exchange_path}!")

    # coins data pipeline 
    coin_requests= get_coin_request(start=1, limit=7000)
    export_csv(coin_requests, coin_path)
    print("Sucessfully get data and export out to {coin_path}!")


    # coin's market data pipeline
    coin_market_data = get_coin_market_request(HowMany=25, DataPath=coin_path)
    export_csv(coin_market_data, coin_market_path)
    print("Sucessfully get data and export out to {coin_market_path}!")

    
    # Stage binance 1 minutes data to s3
    upload_to_s3(bucketname=bucket_name, local_file_path=binance_btc_file, s3_file_path=binance_btc_file)
    print(f"Successfully Uploaded {binance_btc_file}to S3!")
    
    
    # stage coins_data info to s3
    upload_to_s3(bucketname=bucket_name, local_file_path=coin_path, s3_file_path=coin_path)
    print(f"Successfully Uploaded {coin_path}to S3!")

    # stage coin market data into s3
    upload_to_s3(bucketname=bucket_name, local_file_path=coin_market_path, s3_file_path=coin_market_path)
    print(f"Successfully Uploaded {coin_market_path}to S3!")


    # stage coin exchange path into s3
    upload_to_s3(bucketname=bucket_name, local_file_path=coin_exchange_path, s3_file_path=coin_exchange_path)
    print(f"Successfully Uploaded {coin_exchange_path}to S3!")



if __name__ == "__main__":
    main()