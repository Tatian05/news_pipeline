import os
import json
import requests
import pandas as pd
import psycopg2 as pg2

from datetime import datetime


CITY = "Buenos Aires"
API_KEY = os.getenv("API_KEY")

GEOCODING_URL = f"http://api.openweathermap.org/geo/1.0/direct?q={CITY}&appid={API_KEY}"

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

     #CLOUDS COLUMN
     df_clouds = pd.json_normalize(df["clouds"])

     #SYS COLUMN
     df_sys = pd.json_normalize(df["sys"])
     df_sys = df_sys.drop(columns=["type", "id"])
     
     df_stage = pd.concat([df_coord, df, df_weather, df_main, df_wind, df_clouds, df_sys], axis=1).reset_index(drop=True)

     df_stage = df_stage.drop(columns=["coord", "weather", "main", "wind", "clouds", "sys", "base", "cod"])

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
          "speed": "wind_speed"
     })

     df_stage["sunrise"] = pd.to_datetime(df_stage["sunrise"], unit='s')
     df_stage["sunrise"] = df_stage["sunrise"].dt.strftime("%Y-%m-%dT%H:%M:%S")

     df_stage["sunset"] = pd.to_datetime(df_stage["sunset"], unit='s')
     df_stage["sunset"] = df_stage["sunset"].dt.strftime("%Y-%m-%dT%H:%M:%S")

     df_stage["datetime"] = pd.to_datetime(df_stage["datetime"], unit='s')
     df_stage["datetime"] = df_stage["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S")

     df_stage.to_json(TRANSFORMED_FILE_PATH, orient="records")

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
               lon FLOAT,
               lat FLOAT,
               visibility INTEGER,
               datetime TIMESTAMP,
               timezone INTEGER,
               city_id BIGINT,
               name VARCHAR(100),
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
               deg INTEGER,
               gust INTEGER,
               cloudiness INTEGER,
               country_code CHAR(2),
               sunrise TIMESTAMP,
               sunset TIMESTAMP,
               ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          );
     """


     #QUERY_CREATE_DIM_CITY = 

     db_name = os.getenv("POSTGRES_DB")
     db_host = os.getenv("POSTGRES_HOST")
     db_port = os.getenv("POSTGRES_PORT")
     db_user = os.getenv("POSTGRES_USER")
     db_password = os.getenv("POSTGRES_PASSWORD")

     try:
          with pg2.connect(
               dbname=db_name,
               host=db_host,
               port=db_port,
               user=db_user,
               password=db_password
          ) as conn:
               with conn.cursor() as cursor:
                    cursor.execute(QUERY_CREATE_STAGE_SCHEMA)
                    cursor.execute(QUERY_CREATE_DIM_SCHEMA)
                    cursor.execute(QUERY_CREATE_STAGE_TABLE)

          df_stage.to_sql(name="weather_data", con=conn, if_exists="append", schema="stg")

     except Exception as e:
          print(f"Error during SQL operation: {e}")


extract()
transform()
load()



#docker exec -it airflow_webserver python /opt/airflow/scripts/etl.py