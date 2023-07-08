from airflow import DAG
from datetime import datetime

from airflow.decorators import task


with DAG(
    dag_id="example-dag",
    start_date=datetime(2023, 7, 7),
    schedule=None,
    tags=['python', 'example']
):

    @task
    def hello_world():
        print("Hello World.")

    hello_world()