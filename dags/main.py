from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from jobs.utils.aws_s3_storage import store_data_s3
from jobs.utils.data_joining import join_data
from jobs.piezometrie.piezometrie_api import get_piezometrie_data
from jobs.weather.weather_api import get_weather_data
from jobs.weather.weather_data_cleaning import process_weather_data
from jobs.piezometrie.piezometrie_data_cleaning import process_piezometrie_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 10),
}

dag = DAG('merge_weather_piezometrie_data', default_args=default_args, schedule_interval='0 20 * * 0')

def piezometrie_process():
    piezometrie_data = get_piezometrie_data()
    return process_piezometrie_data(piezometrie_data)

def weather_process():
    weather_data = get_weather_data()
    print(weather_data)
    return process_weather_data(weather_data)

def main():
    piezometrie_data_format = piezometrie_process()
    weather_data_format = weather_process()
    combined_data = join_data(weather_data_format, piezometrie_data_format)
    store_data_s3(combined_data, 'combined_data.csv', 'combined_data', 'big-data-water-isep')

start_task = PythonOperator(
    task_id='start',
    python_callable=main,
    dag=dag
)

start_task
