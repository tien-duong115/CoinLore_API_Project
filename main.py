
from requests.api import get
from pandas_functions import *
from  spark_functions import *
import pandas as pd
import os
from dotenv import load_dotenv
import config as c
import glob

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
    historical_data_path = "data/Downloaded_data/*.csv"
    final_historical = "data/final_historical_data.csv"
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
        
    # coins data pipeline 
    try:
        coin_requests= get_coin_request(start=0, limit=7000)
        export_csv(coin_requests, coin_path)
        print("Sucessfully get data and export out to {coin_path}!")
    except Exception as e:
        print('\nFail to GET at {coin_path}\n')
        print(e)

    #top coins data pipeline
    try:
        top_coins_path = 'data/top_coins.csv'
        coin_market_data = top_rank_coins(HowMany=25)
        export_csv(coin_market_data, top_coins_path)
        print("\nSucessfully get data and export out to: {top_coins_path}!\n\n")
    except Exception as e:
        print('\nFail to GET at {top_coins_path}\n')
        print(e)

    # binance historical coins data clean and export to csv
    try:
        path = r'data/historical_data.csv'
        binance_coins_data(historical_data_path, historical_data)
        all_files = glob.glob(path + "/*.csv")
        li = []
        for filename in all_files:
            df = pd.read_csv(filename, index_col=None, header=0)
            li.append(df)
        frame = pd.concat(li, axis=0, ignore_index=True)
        frame.to_csv('data/final_historical_data.csv', index=False)
        print(f"\nSucessfully clean and export historical data to {historical_data}\n")
    except Exception as e:
        print(f'\nFail to clean and export {historical_data_path}\n')
        print(e) 
    
    # Stage historical 1 minutes data to s3
    try:
        upload_to_s3(bucketname=c.S3_BUCKET_NAME, local_file_path=final_historical, s3_file_path=final_historical)
        print(f"Successfully Uploaded {final_historical}to S3!")
    except Exception as e:
        print(f'\nFail to upload {final_historical}\n')
        print(e)
    
    # stage coins_data info to s3
    
    try:
        upload_to_s3(bucketname=c.S3_BUCKET_NAME, local_file_path=coin_path, s3_file_path=coin_path)
        print(f"\nSuccessfully Uploaded {coin_path} to S3!\n")
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
    main()