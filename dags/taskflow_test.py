from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from datetime import datetime


    
    
    
@dag(
    start_date=datetime(2023,1,1),
    schedule_interval='@daily',
    catchup=False,
    tags=['taskflow','YacoJackieFreyja']
)
def taskflow():
    @task
    def task_a():
        print("Task A: Yaco")
        return 42

    @task
    def task_b(value):
        print("Task B: Freyja")
        print("Down from task_a")
        print(value)
    
    task_b(task_a())
    
taskflow()