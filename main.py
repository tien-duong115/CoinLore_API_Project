
from requests.api import get
from pandas_functions import *
from  spark_functions import *
import pandas as pd
import os
from dotenv import load_dotenv
import config as c


def main():
    """[
        - Data pipelines to extract, tranform and load data into S3 bucket
        ]
    """
    
    load_dotenv()
    # file paths
    coin_path = 'data/coins_data.csv'
    top_coins_path = 'data/top_coins.csv'
    coin_exchange_path = 'data/coin_exchange_info.csv'
    historical_data = 'data/historical_data.csv' 
    
    # Exchange data pipeline
    try:
        GET_all_exchanges = 'https://api.coinlore.net/api/exchanges/'
        exchange_requests = get_request(GET_all_exchanges)
        exchange_data = exchange_data_filter(exchange_requests)
        export_csv(exchange_data,coin_exchange_path)
        print("Sucessfully get data and export out to {coin_exchange_path}!")
    except Exception as e:
        print(f'\nFail to GET at {GET_all_exchanges}\n')
        print(e)
        
    #coins data pipeline 
    try:
        coin_requests= get_coin_request(start=0, limit=7000)
        export_csv(coin_requests, coin_path)
        print("Sucessfully get data and export out to {coin_path}!")
    except Exception as e:
        print('\nFail to GET at {coin_path}\n')
        print(e)

    #coin's market data pipeline
    try:
        top_coins_path = 'data/top_coins.csv'
        coin_market_data = top_rank_coins(HowMany=25)
        export_csv(coin_market_data, top_coins_path)
        print("\nSucessfully get data and export out to: {top_coins_path}!\n\n")
    except Exception as e:
        print('\nFail to GET at {top_coins_path}\n')
        print(e)

    
    #Stage historical 1 minutes data to s3
    try:
        upload_to_s3(bucketname=c.S3_BUCKET_NAME, local_file_path=historical_data, s3_file_path=historical_data)
        print(f"Successfully Uploaded {historical_data}to S3!")
    except Exception as e:
        print(f'\nFail to upload {historical_data}\n')
        print(e)
    
    # stage coins_data info to s3
    
    try:
        upload_to_s3(bucketname=c.S3_BUCKET_NAME, local_file_path=coin_path, s3_file_path=coin_path)
        print(f"Successfully Uploaded {coin_path}to S3!")
    except Exception as e:
        print(f'\nFail to upload {coin_path}\n')
        print(e)
    
    # stage coin market data into s3
    try:
        upload_to_s3(bucketname=c.S3_BUCKET_NAME, local_file_path=top_coins_path, s3_file_path=top_coins_path)
        print(f"Successfully Uploaded {top_coins_path}to S3!")
    except Exception as e:
        print(f'\nFail to upload {top_coins_path}\n')
        print(e)
    
    # stage coin exchange path into s3
    try:
        upload_to_s3(bucketname=c.S3_BUCKET_NAME, local_file_path=coin_exchange_path, s3_file_path=coin_exchange_path)
        print(f"Successfully Uploaded {coin_exchange_path}to S3!")
    except Exception as e:
        print(f'\nFail to upload {coin_exchange_path}\n')
        print(e)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)