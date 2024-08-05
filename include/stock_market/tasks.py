import json
from minio import Minio
import requests
from airflow.hooks.base import BaseHook

def _get_stock_prices(endpoint, symbol):
    url = f"{endpoint}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_yahoo')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0])
    
def _store_precios():
    minio = BaseHook.get_connection('minio').extra_dejson
    client = Minio(
        endpoint=minio['endpoint_url'].split('//')[1],
        access_key=minio['aws_access_key_id'],
        secret_key=minio['aws_secret_access_key'],
        secure=False
    )
    
    bucket_name = 'stock-prices'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    