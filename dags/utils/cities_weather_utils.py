import requests
import json
import pandas as pd
import pytz
from datetime import datetime

def extract_and_dump_data(s3, location: dict, endpoint: str, api_key: str, raw_bucket: str) -> None:
    """
    Extract weather data for a list of cities from the OpenWeatherMap API and dump it in JSON format into the S3 raw bucket.
    """
    url = f"http://api.openweathermap.org/{endpoint}"
    infile = {}

    # Extract weather data
    for city in location.keys():
        params = {
            "appid": api_key,
            "q": city
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        infile[city] = data

    print("Weather data extracted successfully")

    # Dump weather data to S3
    time_zone = pytz.timezone("Asia/Jakarta")
    timestamp = datetime.now(time_zone)
    timestamp_str = timestamp.strftime("%Y%m%d-%H")

    # Define the file path in the S3 bucket
    s3_path = f"{raw_bucket}/cities_weather_{timestamp_str}.json"

    # Write the JSON data to the S3 bucket
    with s3.open(s3_path, 'w') as outfile:
        json.dump(infile, outfile, indent=4)

    print(f"Successfully wrote JSON to {s3_path}")


def list_raw_files(s3, raw_bucket):
    """
    List all JSON files in the S3 raw bucket.
    """
    files = s3.ls(raw_bucket)
    
    # Filter out only JSON files
    json_files = [file for file in files if file.endswith('.json')]
    
    # Limit to 10 files for printing
    json_files_limited = json_files[:10]
    
    if len(json_files) > 10:
        json_files_limited = json_files[:10] + ['...']
    else:
        json_files_limited = json_files
    
    # Print the list of JSON files limited by 10
    print(f"raw_files = {json_files_limited}")

    return json_files

def normalize_and_transform_json_to_parquet(s3, raw_bucket, clean_bucket):
    """
    Normalize and transform all raw JSON data files from the raw S3 bucket to Parquet and save them in the clean S3 bucket.
    """
    # List all JSON files in the raw bucket
    json_files_list = list_raw_files(s3, raw_bucket)

    for iter_json_files in json_files_list:
        # Read the JSON data from the S3 file
        with s3.open(iter_json_files, 'r') as file:
            data = json.load(file)
        
        # Normalize the JSON data
        records = []
        for city, city_data in data.items():
            record = {
                'city_params': city,
                'longitude': city_data['coord']['lon'],
                'latitude': city_data['coord']['lat'],
                'weather': city_data['weather'][0]['main'],
                'weather_desc': city_data['weather'][0]['description'],
                'temperature': city_data['main']['temp'],
                'feels_like': city_data['main']['feels_like'],
                'temperature_min': city_data['main']['temp_min'],
                'temperature_max': city_data['main']['temp_max'],
                'pressure': city_data['main']['pressure'],
                'humidity': city_data['main']['humidity'],
                'visibility': city_data.get('visibility'),
                'wind_speed': city_data['wind']['speed'],
                'rain': city_data.get('rain', {}).get('1h', 0),
                'clouds': city_data['clouds']['all'],
                'country': city_data['sys']['country'],
                'city_name': city_data['name'],
                'datetime_unix': city_data['dt']
            }

            # Convert Unix timestamp to local date and time
            timezone = pytz.timezone(location[city])
            record['datetime_local'] = datetime.fromtimestamp(city_data['dt'], timezone).strftime('%Y-%m-%d %H:%M:%S')

            records.append(record)

        # Convert the list of records into a DataFrame
        df = pd.DataFrame(records)

        # Generate a Parquet filename based on the input JSON filename
        json_filename = iter_json_files.split('/')[-1].replace('.json', '')
        parquet_file_path = f"{clean_bucket}/{json_filename}.parquet"

        # Save the DataFrame as a Parquet file to the S3 clean bucket
        with s3.open(parquet_file_path, 'wb') as f:
            df.to_parquet(f, index=False)

        print(f"Successfully wrote PARQUET to {parquet_file_path}")