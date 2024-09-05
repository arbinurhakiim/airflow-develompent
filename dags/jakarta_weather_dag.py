from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from utils.jakarta_weather_utils import *
from airflow.models import Variable
import json

# Define variables for each necessary parameters
endpoint = 'data/2.5/weather?'
api_key = Variable.get('OPEN_WEATHER_API')
city = 'jakarta'
weather_directory = f'/plugins/{city}_weather_data'

# Define variables for postgres
postgres_cred_json = Variable.get('JSON_POSTGRES_CRED')
postgres_cred = json.loads(postgres_cred_json)
host = postgres_cred.get('host')
user = postgres_cred.get('user')
password = postgres_cred.get('password')
port = postgres_cred.get('port')
dbname = postgres_cred.get('dbname')
table_name = f"{city}_weather"

# Define default args
default_args = {
    "start_date":datetime(2024, 3, 1),
    "retries":10,
    "retry_delay":timedelta(minutes=5)
}

# Define dag and tasks
with DAG(
    f'pg_{city}_weather_dag', 
    schedule_interval='0 00,9 * * *', 
    default_args=default_args, 
    catchup=False, 
    tags=['weather']
) as dag:

    start_task = EmptyOperator(
        task_id = 'start_task'
    )

    extracting_data = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract_data,
        op_args = [endpoint, api_key, city]
    )

    creating_table = PythonOperator(
        task_id = 'create_table',
        python_callable = create_table,
        op_args = [city, host, user, password, port, dbname]
    )

    truncating_table = PythonOperator(
        task_id = 'truncate_table',
        python_callable = truncate_table,
        op_args = [city, host, user, password, port, dbname]
    )

    generating_csv = PythonOperator(
        task_id = 'generate_csv',
        python_callable = generate_csv,
        op_args = [endpoint, api_key, city]
    )

    file_list = PythonOperator(
        task_id = 'fetch_file_list',
        python_callable = fetch_file_list,
        op_args = [weather_directory]
    )

    storing_data = PythonOperator(
        task_id = 'store_data',
        python_callable = copy_csv_to_postgres,
        op_args = [city, weather_directory, host, user, password, port, dbname, table_name]
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    # Define dependencies
    start_task >> [extracting_data, creating_table] >> truncating_table >> generating_csv >> file_list >> storing_data >> end_task
