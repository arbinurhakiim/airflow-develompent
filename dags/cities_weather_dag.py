from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from utils.cities_weather_utils import *
from airflow.models import Variable
import json 
import s3fs


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

# Define AWS S3 parameters
s3 = s3fs.S3FileSystem(key=ACCESS_KEY_ID, secret=SECRET_ACCESS_KEY)

# Define S3 raw and clean buckets
raw_bucket = "de-exploration/arbi/weather_data/raw"
clean_bucket = "de-exploration/arbi/weather_data/clean"

# Define default args and dag
default_args = {
    "start_date": datetime(2024, 3, 1),
    "retries": 10,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    'aws_s3_cities_weather_dag', 
    schedule_interval='0 00,9 * * *', 
    default_args=default_args, 
    catchup=False, 
    tags=['weather']
) as dag:

    start_task = EmptyOperator(
        task_id='start_task'
    )

    extracts_and_dumps_data = PythonOperator(
        task_id='extract_and_dump_data',
        python_callable=extract_and_dump_data,
        op_args=[s3, location, endpoint, api_key, raw_bucket]
    )

    lists_raw_files = PythonOperator(
        task_id='list_raw_files',
        python_callable=list_raw_files,
        op_args=[s3, raw_bucket]
    )

    normalizes_and_transforms_json_to_parquet = PythonOperator(
        task_id='normalize_and_transform_json_to_parquet',
        python_callable=normalize_and_transform_json_to_parquet,
        op_args=[s3, raw_bucket, clean_bucket]
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    start_task >> extracts_and_dumps_data >> lists_raw_files >> normalizes_and_transforms_json_to_parquet >> end_task