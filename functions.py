from os import write
import requests
from requests.api import request
import pandas as pd
import csv, json


def get_request(url):
    """[
        - Generate Request from input Url
        ]

    Args:
        url ([type]): [description]

    Returns:
        [type]: [description]
    """
    r = requests.get(url)
    return r.json()


def get_coin_request(start=0, limit=100):
    """[
        - Generate requests from API
        - return out as Pandas DataFrame
        ]
    Args:
        start (int, optional): [description]. Defaults to 0.
        limit (int, optional): [description]. Defaults to 100.

    Returns:
        [type]: [description]
    """
    df = pd.DataFrame()
    for i in range(start,limit,100):
        r = requests.get(f'https://api.coinlore.net/api/tickers/?start={i}').json()
        entry = r['data']
        for line in entry:
            df = df.append(line, ignore_index=True)
            print(f"Successfully request number {i}")
    return df

               
        
def export_csv(data,path):
    """[Used Pandas to export out as CSV format]

    Args:
        data ([dataframe]): [data to be export]
        path ([None]): [path to be export]
    """
    data.to_csv(path)
    print(f'exported to {path}!')


def exchange_data_filter(data):
    """[Expect incoming data as Pandas dataframe and return DF as output]
    Args:
        data ([json]): [return exchange data records with country filtered]
    """
    df_data = pd.DataFrame.from_dict(data.values())
    df_data = df_data.astype({'id':int}).copy()
    df_data = df = df_data[['id','name', 'name_id', 'url','country','date_live', 'date_added', 'usdt','fiat','auto','volume_usd','udate','volume_usd_adj']].sort_values(by=['id']).set_index('id').query("country != '' ")
    return df_data

