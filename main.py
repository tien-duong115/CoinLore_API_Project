from os import path

from requests.api import get
from functions import *
import pandas as pd



GET_all_exchanges = 'https://api.coinlore.net/api/exchanges/'

requests = get_request(url)

data = exchange_data_filter(requests)

my_path = 'data/coins_data.csv'

coin_data = get_coin_request(start=1, limit=7000)

export_csv(coin_data, my_path)
