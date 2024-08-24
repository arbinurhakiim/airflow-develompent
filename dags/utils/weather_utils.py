import requests
import json
import s3fs
import pytz
from datetime import datetime, timedelta

def extract_data(location: dict, endpoint: str, api_key: str) -> dict:

    """
    This function used for extracts weather data for the list of cities from OpenWeatherMap API.

    location: list -> List of cities for which weather data is extracted
    endpoint: str -> The API endpoint for weather data, e.g., 'data/2.5/weather?'
    api_key: str -> The API key for accessing the OpenWeatherMap API

    Returns:
        dict: dictionary containing weather data for the list of cities (location)

    """

    url = f"http://api.openweathermap.org/{endpoint}"

    weather_data = {}

    for city in location:
        params = {
            "appid": api_key,
            "q": city
        }
        response = requests.get(url, params)
        response.raise_for_status()
        data = response.json()
        weather_data[city] = data

    ## Below is the dictionary comprehension method of the above script
    # weather_data = {city: requests.get(url, params={"appid": api_key, "q": city}).json() for city in location}

    print("Weather data extracted successfully")
    return weather_data

def dump_raw_data(location: dict, endpoint: str, api_key: str, ACCESS_KEY_ID: str, SECRET_ACCESS_KEY: str, s3_bucket: str) -> None:

    # Extract data and convert to JSON string
    infile = extract_data(location, endpoint, api_key)

    time_zone = pytz.timezone("Asia/Jakarta")
    timestamp = datetime.now(time_zone)
    timestamp_str = timestamp.strftime("%Y%m%d-%H")

    # Define the file path in the S3 bucket
    s3_path = f"{s3_bucket}/output_{timestamp_str}.json"

    # Write the JSON data to the S3 bucket
    fs = s3fs.S3FileSystem(key=ACCESS_KEY_ID, secret=SECRET_ACCESS_KEY)
    with fs.open(s3_path, 'w') as outfile:
        json.dump(infile, outfile, indent=4)  # Write JSON string directly to file

    print(f"Raw data has been dumped to {s3_path}")