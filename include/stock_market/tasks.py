from airflow.hooks.base import BaseHook
from minio import Minio
def _get_stock_prices(url, symbol):
    import requests
    import json
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers= api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])

def _store_prices():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'],
        access_key=minio.login, 
        secret_key=minio.password,
        secure=False
    )

 