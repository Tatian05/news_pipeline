import os
import json
import requests
import pandas

from dotenv import load_dotenv

load_dotenv()

CITY = "Buenos Aires"
API_KEY = os.environ.get("API_KEY")
print(API_KEY)
GEOCODING_URL = f"http://api.openweathermap.org/geo/1.0/direct?q={CITY}&appid={API_KEY}"

def get_coords():
     res = requests.get(GEOCODING_URL)
     data = res.json()[0]
     return data['lat'], data['lon']

lat, lon = get_coords()

WEATHER_URL = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={API_KEY}"

RAW_FILE_PATH = '/tmp/raw_weather.json'
TRANSFORMED_FILE_PATH = '/tmp/transformed_weather.json'


def extract():
    res = requests.get(WEATHER_URL)
    data = res.json()

    if res.status_code != 200:
         raise Exception(f"Error in the API: {data}")
    
    with open(RAW_FILE_PATH, 'w') as f:
         json.dump(data, f)

    print("Data extracted correctly")

