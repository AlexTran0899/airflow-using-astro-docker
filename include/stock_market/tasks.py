from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO
from airflow.exceptions import AirflowNotFoundException

BUCKET_NAME = 'stock-market'

def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'],
        access_key=minio.login, 
        secret_key=minio.password,
        secure=False
    )

    return client

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

    client = _get_minio_client()
    # check to see if that bucket exists in minio, you can go to localhost:9001 to check manually
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=BUCKET_NAME, 
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    print('[DEBUG] stored price completed')
    return f'{objw.bucket_name}/{symbol}'


def _get_formatted_csv(path):
    print("the path is ", path)
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted-prices"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
        
    raise AirflowNotFoundException('The csv file does not exist')