from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO

def _get_stock_prices(url, symbol):
    import requests
    import json

    api = BaseHook.get_connection('stock_api')
    url = f"{url}{symbol}?metrics=high&interval=1d&range=1y"
    response = requests.get(url, headers=api.extra_dejson['headers'])

    try:
        data = response.json()
        result = data['chart']['result']
        if result:
            return json.dumps(result[0])
        else:
            raise ValueError("No result found in chart data")
    except Exception as e:
        print(f"[ERROR] Failed to fetch or parse data: {e}")
        raise

def _store_prices(stock):
    import json

    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'],
        access_key=minio.login, 
        secret_key=minio.password,
        secure=False
    )

    bucket_name = 'stock-market'
    # check to see if that bucket exists in minio, you can go to localhost:9001 to check manually
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    print(symbol)
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=bucket_name, 
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/{symbol}'
 