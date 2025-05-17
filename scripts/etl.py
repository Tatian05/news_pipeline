import os
import json
import requests
import pandas as pd
import psycopg2 as pg2

from sqlalchemy import create_engine
from datetime import datetime


CITY = "Buenos Aires"
API_KEY = os.getenv("API_KEY")

GEOCODING_URL = f"https://api.openweathermap.org/geo/1.0/direct?q={CITY}&appid={API_KEY}"

def get_coords():
     try:
          res = requests.get(GEOCODING_URL)
          data = res.json()[0]
     except requests.RequestException as e:
          print(f"Error in API request: {e}")
     return data['lat'], data['lon']

lat, lon = get_coords()

WEATHER_URL = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"

RAW_FILE_PATH = '/tmp/raw_weather.json'
TRANSFORMED_FILE_PATH = '/tmp/transformed_weather.json'

def extract() -> dict:
    res = requests.get(WEATHER_URL)
    data = res.json()

    if res.status_code != 200:
         raise Exception(f"Error in the API: {data}")

    print("Data extracted correctly")

    return data

def transform(raw_data:dict) -> pd.DataFrame:
     df = pd.DataFrame([raw_data])

     #COORD
     df_coord = pd.json_normalize(df["coord"])

     #WEATHER COLUMN
     df_weather = pd.json_normalize(df["weather"])
     data_weather = df_weather.iloc[0,0]
     df_weather = pd.DataFrame([data_weather])
     df_weather = df_weather.drop(columns=["id", "icon"])

     #MAIN COLUMN
     df_main = pd.json_normalize(df['main'])

     #WIND COLUMN
     df_wind = pd.json_normalize(df["wind"])

     #RAIN COLUMN
     df_rain = pd.DataFrame()
     if "rain" in df.columns:
          df_rain = pd.json_normalize(df["rain"])
          df_rain = df_rain.rename(columns={"1h": "rain_mm_h"})

     #SNOW
     df_snow = pd.DataFrame()
     if "snow" in df.columns:
          df_snow = pd.json_normalize(df["snow"])
          df_snow = df_snow.rename(columns={"1h": "snow_mm_h"})

     #CLOUDS COLUMN
     df_clouds = pd.json_normalize(df["clouds"])

     #SYS COLUMN
     df_sys = pd.json_normalize(df["sys"])
     df_sys = df_sys.drop(columns=["type", "id"])
     
     columns_to_drop = ["coord", "weather", "main", "wind", "rain", "snow","clouds", "sys", "base"]
     df = df.drop(columns= [col for col in columns_to_drop if col in df.columns])

     df_cleaned = pd.concat([df_coord, df, df_weather, df_main, df_wind, df_rain, df_snow, df_clouds, df_sys], axis=1).reset_index(drop=True)

     df_cleaned = df_cleaned.rename(columns={
          "id": "city_id",
          "country":"country_code",
          "lon": "longitude",
          "lat": "latitude",
          "dt": "datetime",
          "name": "city_name",
          "main": "weather_main",
          "description": "weather_description",
          "temp": "temperature",
          "speed": "wind_speed",
          "deg": "wind_deg",
          "gust": "wind_gust",
          "all":"cloudiness"
     })

     df_cleaned["sunrise"] = pd.to_datetime(df_cleaned["sunrise"], unit='s')
     df_cleaned["sunrise"] = df_cleaned["sunrise"].dt.strftime("%Y-%m-%dT%H:%M:%S")

     df_cleaned["sunset"] = pd.to_datetime(df_cleaned["sunset"], unit='s')
     df_cleaned["sunset"] = df_cleaned["sunset"].dt.strftime("%Y-%m-%dT%H:%M:%S")

     df_cleaned["datetime"] = pd.to_datetime(df_cleaned["datetime"], unit='s')
     df_cleaned["datetime"] = df_cleaned["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S")

     print("Data transformed correctly")

     return df_cleaned


def load(df_transformed:pd.DataFrame):
     QUERY_CREATE_SCHEMA="""
          CREATE SCHEMA IF NOT EXISTS weather;
     """

     QUERY_CREATE_WEATHER_TABLE = """
          CREATE TABLE IF NOT EXISTS weather.weather_data(
               id SERIAL PRIMARY KEY,
               city_id INTEGER,
               city_name VARCHAR(100),
               country_code CHAR(2),
               longitude FLOAT,
               latitude FLOAT,
               datetime TIMESTAMP,
               timezone INTEGER,
               weather_main VARCHAR(50),
               weather_description VARCHAR(255),
               temperature FLOAT,
               feels_like FLOAT,
               temp_min FLOAT,
               temp_max FLOAT,
               visibility INTEGER,
               pressure INTEGER,
               humidity INTEGER,
               sea_level INTEGER,
               grnd_level INTEGER,
               wind_speed FLOAT,
               wind_deg INTEGER,
               wind_gust FLOAT,
               rain_mm_h FLOAT DEFAULT 0.0,
               snow_mm_h FLOAT DEFAULT 0.0,
               cloudiness INTEGER DEFAULT 0,
               sunrise TIMESTAMP,
               sunset TIMESTAMP,
               cod INTEGER,
               ingestion_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
          );
     """

     db_name = os.getenv("POSTGRES_DB")
     db_host = os.getenv("POSTGRES_HOST")
     db_port = os.getenv("POSTGRES_PORT")
     db_user = os.getenv("POSTGRES_USER")
     db_password = os.getenv("POSTGRES_PASSWORD")

     engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}", future=True)

     try:
          with engine.begin() as conn:
               conn.exec_driver_sql(QUERY_CREATE_SCHEMA)
               conn.exec_driver_sql(QUERY_CREATE_WEATHER_TABLE)

               df_transformed.to_sql(name="weather_data", con=conn, if_exists="append", schema="weather", index=False)

          print("Data loaded to database successfully")
     except Exception as e:
          print(f"Error during SQL operation: {e}")


load(transform(extract()))