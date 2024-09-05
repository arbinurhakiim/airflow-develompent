from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from utils.weather_utils import *
from airflow.models import Variable
import json 

# Define location for the weather q (city) parameters as keys and the represented time zone as values
location = {
    'ambon': 'Asia/Jayapura',
    'balikpapan': 'Asia/Makassar',
    'banda aceh': 'Asia/Jakarta',
    'bandar lampung': 'Asia/Jakarta',
    'bandung': 'Asia/Jakarta',
    'banjarmasin': 'Asia/Makassar',
    'bekasi': 'Asia/Jakarta',
    'bengkulu': 'Asia/Jakarta',
    'bogor': 'Asia/Jakarta',
    'bukittinggi': 'Asia/Jakarta',
    'cilegon': 'Asia/Jakarta',
    'denpasar': 'Asia/Makassar',
    'depok': 'Asia/Jakarta',
    'jakarta': 'Asia/Jakarta',
    'jambi': 'Asia/Jakarta',
    'jayapura': 'Asia/Jayapura',
    'kendari': 'Asia/Makassar',
    'kupang': 'Asia/Makassar',
    'makassar': 'Asia/Makassar',
    'malang': 'Asia/Jakarta',
    'manado': 'Asia/Makassar',
    'mataram': 'Asia/Makassar',
    'medan': 'Asia/Jakarta',
    'padang': 'Asia/Jakarta',
    'palangka raya': 'Asia/Jakarta',
    'palembang': 'Asia/Jakarta',
    'palu': 'Asia/Makassar',
    'pangkalpinang': 'Asia/Jakarta',
    'pekanbaru': 'Asia/Jakarta',
    'pontianak': 'Asia/Jakarta',
    'samarinda': 'Asia/Makassar',
    'semarang': 'Asia/Jakarta',
    'serang': 'Asia/Jakarta',
    'sorong': 'Asia/Jayapura',
    'surabaya': 'Asia/Jakarta',
    'surakarta': 'Asia/Jakarta',
    'tangerang': 'Asia/Jakarta',
    'tangerang selatan': 'Asia/Jakarta',
    'tanjungpinang': 'Asia/Jakarta',
    'yogyakarta': 'Asia/Jakarta'
}

# Define OpenWeatherMap API parameters
api_key = Variable.get('OPEN_WEATHER_API')
endpoint = 'data/2.5/weather?'

# Define AWS S3 parameters
aws_cred_json = Variable.get('JSON_AWS_CRED')
aws_cred = json.loads(aws_cred_json)
ACCESS_KEY_ID = aws_cred.get('ACCESS_KEY_ID')
SECRET_ACCESS_KEY = aws_cred.get('SECRET_ACCESS_KEY')

# Define default args
default_args = {
    "start_date": datetime(2024, 3, 1),
    "retries": 10,
    "retry_delay": timedelta(minutes=5)
}

# Define dag and tasks
with DAG(
    'aws_s3_weather_dag', 
    schedule_interval='0 00,9 * * *', 
    default_args=default_args, 
    catchup=False, 
    tags=['weather']
) as dag:

    start_task = EmptyOperator(
        task_id='start_task'
    )

    extracting_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_args=[location, endpoint, api_key]
    )

    dumps_raw_data = PythonOperator(
        task_id='dump_raw_data',
        python_callable=dump_raw_data,
        op_args=[location, endpoint, api_key, ACCESS_KEY_ID, SECRET_ACCESS_KEY, "de-exploration/arbi/weather_data/raw"]
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    # Define dependencies
    start_task >> extracting_data >> dumps_raw_data >> end_task
