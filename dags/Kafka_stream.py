from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from streaming_to_kafka import load_to_kafka
default_args = {
    'owner': 'Ducnguyen',
    'start_date': datetime(2026, 1, 21),
    'depend_on_past': False
}


with DAG('push_data_to_broker',
         default_args=default_args,
         schedule='@hourly',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api_to_broker',
        python_callable= load_to_kafka
    )

