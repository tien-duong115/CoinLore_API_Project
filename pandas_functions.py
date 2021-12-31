from os import write
import requests
from requests.api import request
import pandas as pd


def get_request(url):
    """[
        - Generate Request from input Url
        ]
    Args:
        url ([type]): [description]
    Returns:
        [type]: [description]
    """
    payload = requests.get(url)
    return payload.json()


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
    payload = pd.DataFrame()
    for i in range(start,limit,100):
        r = requests.get(f'https://api.coinlore.net/api/tickers/?start={i}').json()
        entry = r['data']
        for line in entry:
            payload = payload.append(line, ignore_index=True)
        print(f"Successfully request number {i}")
    payload.drop(columns='nameid', inplace=True)
    return payload


def top_rank_coins(HowMany=10, DataPath='data/coins_data.csv'):
    """[
        - GET market of top coins
        - Return Pandas DataFrame
        ]   
    Args:
        HowMany (int, optional): [description]. Defaults to 10.
        DataPath (str, optional): [description]. Defaults to ''.
    """
    payload=pd.read_csv(DataPath)
    finalPayload = pd.DataFrame()
    df = payload.copy()
    df['rank'] = df['rank'].astype(int)
    df = df.set_index('id')
    df.drop([45577,51539, 48829, 33435], inplace=True)
    df.reset_index(inplace=True)
    sort_by_rank = df.sort_values(by='rank')
    top_coins = sort_by_rank.head(HowMany)
    list_of_top_coins = list(top_coins.id)
    count = 0
    for i in list_of_top_coins:
        print(f'Sending request to: https://api.coinlore.net/api/coin/markets/?id={i}\n\nRequest number: {count}\n')
        r = requests.get(f'https://api.coinlore.net/api/coin/markets/?id={i}').json()
        count+=1
        for line in r:
            finalPayload = finalPayload.append(line, ignore_index=True)
    finalPayload['time'] = pd.to_datetime(finalPayload['time'], unit='s')
    return finalPayload
       
       
def export_csv(payload,path):
    """[Used Pandas to export out as CSV format]

    Args:
        payload ([dataframe]): [data to be export]
        path ([None]): [path to be export]
    """
    payload.to_csv(path, index=True)
    print(f'exported to {path}!')


def exchange_data_filter(data):
    """[Expect incoming data as Pandas dataframe and return DF as output]
    Args:
        data ([json]): [return exchange data records with country filtered]
    """
    payload = pd.DataFrame.from_dict(data.values())
    payload = payload.astype({'id':int}).copy()
    payload = payload[['id','name', 'url','country','date_live', 'usdt','fiat','auto','volume_usd','udate','volume_usd_adj']].sort_values(by=['id']).set_index('id').query("country != '' ")
    payload['country'].replace(r'[@#&$%+-/\n\r;*]','', regex=True, inplace=True)
    payload.replace({'country':{r'U+K':'United Kingdom ', r'U+S' :'United State ', r'H+K': 'Hong Kong ', r'E+U': 'European Union '}}, regex=True, inplace=True)
    return payload


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])