import pandas as pd
import requests

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

        
def export_csv(payload,path):
    """[Used Pandas to export out as CSV format]

    Args:
        payload ([dataframe]): [data to be export]
        path ([None]): [path to be export]
    """
    payload.to_csv(path, index=True)
    print(f'exported to {path}!')


# Exchange data pipeline
coin_exchange_path = 'data/coin_exchange_info.csv'
GET_all_exchanges = 'https://api.coinlore.net/api/exchanges/'
exchange_requests = get_request(GET_all_exchanges)
exchange_data = exchange_data_filter(exchange_requests)
exchange_data
export_csv(exchange_data,coin_exchange_path)
