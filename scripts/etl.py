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

WEATHER_URL = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"

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

def transform():
     with open(RAW_FILE_PATH, 'r') as f:
          weather_data = json.load(f)

     df = pd.DataFrame([weather_data])

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

     df_stage = pd.concat([df_coord, df, df_weather, df_main, df_wind, df_rain, df_snow, df_clouds, df_sys], axis=1).reset_index(drop=True)

     df_stage = df_stage.rename(columns={
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

     df_stage["sunrise"] = pd.to_datetime(df_stage["sunrise"], unit='s')
     df_stage["sunrise"] = df_stage["sunrise"].dt.strftime("%Y-%m-%dT%H:%M:%S")

     df_stage["sunset"] = pd.to_datetime(df_stage["sunset"], unit='s')
     df_stage["sunset"] = df_stage["sunset"].dt.strftime("%Y-%m-%dT%H:%M:%S")

     df_stage["datetime"] = pd.to_datetime(df_stage["datetime"], unit='s')
     df_stage["datetime"] = df_stage["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S")

     df_stage.to_json(TRANSFORMED_FILE_PATH, orient="records")
     df_stage.to_json("/opt/airflow/scripts/stage.json", orient="records", lines=True, indent=4)

     print("Data transformed correctly")


def load():
     with open(TRANSFORMED_FILE_PATH, 'r') as f:
          transformed_weather_data = json.load(f)

     df_stage = pd.DataFrame(transformed_weather_data)

     QUERY_CREATE_STAGE_SCHEMA="""
          CREATE SCHEMA IF NOT EXISTS stg;
     """

     QUERY_CREATE_DIM_SCHEMA = """
          CREATE SCHEMA IF NOT EXISTS dim;
     """

     QUERY_CREATE_STAGE_TABLE = """
          CREATE TABLE IF NOT EXISTS stg.weather_data(
               longitude FLOAT,
               latitude FLOAT,
               visibility INTEGER,
               datetime TIMESTAMP,
               timezone INTEGER,
               city_id INTEGER,
               city_name VARCHAR(100),
               weather_main VARCHAR(50),
               weather_description VARCHAR(255),
               temperature FLOAT,
               feels_like FLOAT,
               temp_min FLOAT,
               temp_max FLOAT,
               pressure INTEGER,
               humidity INTEGER,
               sea_level INTEGER,
               grnd_level INTEGER,
               wind_speed FLOAT,
               wind_deg INTEGER,
               wind_gust FLOAT,
               rain_mm_h FLOAT DEFAULT 0.0,
               snow_mm_h FLOAT DEFAULT 0.0,
               cloudiness INTEGER,
               country_code CHAR(2),
               sunrise TIMESTAMP,
               sunset TIMESTAMP,
               cod INTEGER,
               ingestion_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
          );
     """

     QUERY_CREATE_DIM_LOCATION = """
          CREATE TABLE IF NOT EXISTS dim.dim_location(
               location_id SERIAL PRIMARY KEY,
               city_id BIGINT,
               city_name VARCHAR(100),
               country_code CHAR(2),
               latitude FLOAT,
               longitude FLOAT,
               timezone INTEGER
          );
     """

     QUERY_CREATE_DIM_WEATHER = """
          CREATE TABLE IF NOT EXISTS dim.dim_weather_condition(
               weather_condition_id SERIAL PRIMARY KEY,
               main VARCHAR(50),
               description VARCHAR(255),
               cloudiness INTEGER
          );
     """

     QUERY_CREATE_DIM_DATETIME = """
          CREATE TABLE IF NOT EXISTS dim.dim_datetime(
               datetime_id SERIAL PRIMARY KEY,
               dt TIMESTAMP WITH TIME ZONE,
               sunrise TIMESTAMP WITH TIME ZONE,
               sunset TIMESTAMP WITH TIME ZONE
          );
     """

     QUERY_CREATE_FACT_METRICS = """
          CREATE TABLE IF NOT EXISTS dim.fact_weather_metrics(
               weather_id SERIAL PRIMARY KEY,
               datetime_id INTEGER REFERENCES dim_datetime(datetime_id),
               location_id INTEGER REFERENCES dim_location(location_id),
               weather_condition_id INTEGER REFERENCES dim_weather_condition(weather_condition_id),
               temperature FLOAT,
               feels_like FLOAT,
               temp_min FLOAT,
               temp_max FLOAT,
               pressure INTEGER,
               humidity INTEGER,
               sea_level INTEGER,
               grnd_level INTEGER,
               wind_speed INTEGER,
               wind_deg INTEGER,
               wind_gust FLOAT,
               rain_mm_h FLOAT,
               snow_mm_h FLOAT,
               cod INTEGER
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
               conn.exec_driver_sql(QUERY_CREATE_STAGE_SCHEMA)
               conn.exec_driver_sql(QUERY_CREATE_DIM_SCHEMA)
               conn.exec_driver_sql(QUERY_CREATE_STAGE_TABLE)

               df_stage.to_sql(name="weather_data", con=conn, if_exists="append", schema="stg", index=False)

          print("Data loaded to database successfully")
     except Exception as e:
          print(f"Error during SQL operation: {e}")



extract()
transform()
load()



#docker exec -it airflow_webserver python /opt/airflow/scripts/etl.py