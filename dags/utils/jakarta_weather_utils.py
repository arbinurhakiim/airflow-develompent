import requests
import json
import os
import csv
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from datetime import datetime, timedelta
import pytz


def extract_data(endpoint: str, api_key: str, city: str) -> dict:

    """
    This function is used for extracting weather data for the specified city from OpenWeatherMap API.

    Parameters:
        endpoint (str): The API endpoint for weather data, e.g., 'data/2.5/weather?'
        api_key (str): The API key for accessing the OpenWeatherMap API
        city (str): The city for which weather data is extracted

    Returns:
        dict: dictionary containing weather data for the specified city
    """

    # API url
    url = f"http://api.openweathermap.org/{endpoint}"

    # Specify url parameter as dictionary data contains api key and city
    params = {
        "appid":f"{api_key}",
        "q":f"{city}"
    }

    # Get response using request.get method and insert url and params as args which lead to json format as output
    response = requests.get(url, params).json()

    # Define the variables with specified values from response
    longitude = response.get('coord').get('lon')
    latitude = response.get('coord').get('lat')
    weather = response.get('weather')[0].get('main')
    weather_desc = response.get('weather')[0].get('description')
    temperature = response.get('main').get('temp')
    feels_like = response.get('main').get('feels_like')
    temperature_min = response.get('main').get('temp_min')
    temperature_max = response.get('main').get('temp_max')
    country = response.get('sys').get('country')
    id = response.get('id')
    city_name = response.get('name')
    datetime_unix = response.get('dt')

    # Create timestamp: str variable that reflects the current time for the specified city
    time_zone = pytz.timezone(f"asia/{city}")
    timestamp = datetime.now(time_zone)
    timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")

    # Generate datetime_str
    dt_object = datetime.fromtimestamp(datetime_unix, time_zone)
    datetime_str = dt_object.strftime("%Y-%m-%d %H:%M:%S")

    # Generate timestamp_str
    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    # Constructing a dictionary data containing the specified value
    data_dict = {"longitude": longitude,
            "latitude": latitude,
            "weather": weather,
            "weather_desc": weather_desc,
            "temperature": temperature,
            "feels_like": feels_like,
            "temperature_min": temperature_min,
            "temperature_max": temperature_max,
            "country": country,
            "id": id,
            "city_name": city_name,
            "datetime_unix": datetime_unix,
            "datetime_str": datetime_str,
            f"datetime_{city}": timestamp_str,
            "created_at": now_str,
            "updated_at": now_str
            }
    
    # Print the success message
    print(f"{city} weather data extracted successfully")

    # Return the constructed dictionary
    return data_dict

def create_table(city: str, host: str, user: str, password: str, port: int, dbname:str) -> None:

    """
    This function is used for creating a table for a specified city in the database

    Parameters:
        city (str): The name of the city associated with the table to be created
        host (str): The hostname or IP address of the database server
        user (str): The username for accessing the database
        password (str): The password for accessing the database
        port (int): The port number on which the database server is listening
        dbname (str): The name of the database containing the table to be created

    Returns:
        None
    """

    # Connect to the database
    conn = psycopg2.connect(host=host, user=user, password=password, port=port, dbname=dbname)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Create table if not exists
    cur.execute(f"CREATE TABLE IF NOT EXISTS {city}_weather (longitude DECIMAL, latitude DECIMAL, weather TEXT, weather_desc TEXT, temperature DECIMAL, feels_like DECIMAL, temperature_min DECIMAL, temperature_max DECIMAL, country TEXT, id INTEGER, city_name TEXT, datetime_unix INTEGER, datetime_str TIMESTAMP, datetime_{city} TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP)")
    conn.commit()

    # Print the success message and close the cursor and connection
    print(f"Table {city}_weather created successfully.")
    cur.close()
    conn.close()

def truncate_table(city: str, host: str, user: str, password: str, port: int, dbname:str) -> None:

    """
    This function is used for truncating a table for a specified city in the database

    Parameters:
        city (str): The name of the city associated with the table to be truncated
        host (str): The hostname or IP address of the database server
        user (str): The username for accessing the database
        password (str): The password for accessing the database
        port (int): The port number on which the database server is listening
        dbname (str): The name of the database containing the table to be truncated

    Returns:
        None
    """

    # Connect to the database
    conn = psycopg2.connect(host=host, user=user, password=password, port=port, dbname=dbname)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Truncate table
    cur.execute(f'TRUNCATE TABLE {city}_weather')
    conn.commit()

    # Print the success message and close the cursor and connection
    print(f"Table {city}_weather truncated successfully.")
    cur.close()
    conn.close()

def generate_csv(endpoint: str, api_key: str, city: str) -> None:
    
    """
    This function is used for generating a CSV file for a specified city from OpenWeatherMap API.

    Parameters:
        endpoint (str): The API endpoint for weather data, e.g., 'data/2.5/weather?'
        api_key (str): The API key for accessing the OpenWeatherMap API
        city (str): The name of the city associated with the generated CSV file

    Returns:
        None
    """

    # Extract data
    data_dict = extract_data(endpoint, api_key, city)

    # Generate timestamp
    time_zone = pytz.timezone(f"asia/{city}")
    timestamp = datetime.now(time_zone)
    timestamp_str = timestamp.strftime("%Y%m%d-%H")

    # Generate CSV
    output_dir = f"/opt/airflow/plugins/{city}_weather_data"
    os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists
    output_csv_path = os.path.join(output_dir, f"output_{timestamp_str}.csv")
    fields = list(data_dict.keys())

    # Write data to CSV
    with open(output_csv_path, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerow(data_dict)

    # Print success message
    print(f"CSV file generated successfully: {output_csv_path}")

def fetch_file_list(weather_directory) -> list:

    """
    This function is used for fetching a list of files from the specified directory.

    Parameters:
        weather_directory (str): The directory containing the files to be listed

    Returns:
        list: A list of files in the specified directory
    """

    # Get the current working directory and list the files
    current_working_directory = str(os.getcwd())
    file_list = os.listdir(current_working_directory+weather_directory)

    # Return the list
    return file_list

def copy_csv_to_postgres(city: str, weather_directory: str, host: str, user: str, password: str, port: int, dbname: str, table_name: str) -> None:

    """
    This function is used for copying data from CSV files in the specified directory to the specified postgres table.

    Parameters:
        city (str): The name of the city associated with the table to be copied
        weather_directory (str): The directory containing the CSV files to be copied
        host (str): The hostname or IP address of the database server
        user (str): The username for accessing the database
        password (str): The password for accessing the database
        port (int): The port number on which the database server is listening
        dbname (str): The name of the database containing the table to be copied
        table_name (str): The name of the table to be copied

    Returns:
        None
    """

    # Extract file list
    file_list = fetch_file_list(weather_directory)

    # Connect to the database
    conn = psycopg2.connect(host=host, user=user, password=password, port=port, dbname=dbname)
    cur = conn.cursor()

    # Copy data from CSV file to postgres table
    for iter_file_list in file_list:
        cur.execute(f"COPY {table_name} FROM '/opt/airflow/plugins/{city}_weather_data/{iter_file_list}' DELIMITER ',' CSV HEADER")
        conn.commit()
    
    # Print the success message and close cursor and connection
    print(f"Data copied successfully.")
    cur.close()
    conn.close()
    