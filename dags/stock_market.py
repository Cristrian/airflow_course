from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
import requests

from include.stock_market.tasks import _get_stock_prices

SYMBOL = "AAPL"

@dag(
    start_date=datetime(year=2024, month=1, day=1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market']
)
def stock_market():
    @task.sensor(poke_interval=30, timeout=90, mode='poke')
    def is_api_avb() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_yahoo')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={'endpoint': '{{task_instance.xcom_pull(task_ids="is_api_avb")}}', 'symbol': SYMBOL}
    )
    store_prices = PythonOperator(
        
    )
        
    is_api_avb() >> get_stock_prices
    
    pass



stock_market()