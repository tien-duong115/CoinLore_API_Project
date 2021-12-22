from os import path
from requests.api import get
from functions import *
import pandas as pd


# Exchange data pipeline
GET_all_exchanges = 'https://api.coinlore.net/api/exchanges/'
exchange_path = 'data/exchange_info.csv'
exchange_requests = get_request(GET_all_exchanges)
exchange_data = exchange_data_filter(exchange_requests)
export_csv(exchange_data,exchange_path)


# coins data pipeline
coin_path = 'data/coins_data.csv'
coin_requests= get_coin_request(start=1, limit=7000)
export_csv(coin_requests, coin_path)


# coin's market data pipeline
coin_market_path = get_coin_market_request(HowMany=25, DataPath=coin_path)
export_csv(coin_market_path, 'data/coin_market_info.csv')

