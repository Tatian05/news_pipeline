import os
import requests
import pandas as pd

from sqlalchemy import create_engine

API_KEY = os.getenv("API_KEY")
URL = 'https://api.currentsapi.services/v1/latest-news'

params = {
    "apiKey": API_KEY
}

def extract() -> dict:
    res = requests.get(url=URL, params=params)
    data = res.json()

    if res.status_code != 200:
         raise Exception(f"Error in the API: {data}")

    print("Data extracted correctly")

    return data

def transform(raw_data:dict) -> dict:
     df = pd.DataFrame(raw_data["news"])
     
     df = df.fillna({"category": "None"})
     df = df.rename(columns={"published":"publication_date"})

     df['category'] = df['category'].apply(lambda x: ','.join(x) if isinstance(x, list) else x)
     df["category"] = df["category"].str.split(",")
     df = df.explode("category")

     df["publication_date"] = pd.to_datetime(df["publication_date"])
     df["publication_date"] = df["publication_date"].dt.strftime("%Y-%m-%dT%H:%M:%S")
         
     print("Data transformed correctly")

     return df.to_dict()


db_name = os.getenv("DB_NAME")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}", future=True)

def load(data_to_load:dict):
     df = pd.DataFrame(data_to_load)

     print(data_to_load)

     QUERY_SCHEMA="""
          CREATE SCHEMA IF NOT EXISTS news;
     """

     QUERY_TEMP_TABLE = """
          CREATE TABLE IF NOT EXISTS news.temp(
               id VARCHAR(255),
               title VARCHAR(255),
               description VARCHAR(1000),
               url VARCHAR(255),
               author VARCHAR(100),
               image VARCHAR(1000),
               language VARCHAR(50),
               category VARCHAR(50),
               publication_date TIMESTAMP
          )
     """

     QUERY_LATEST_TABLE = """
          CREATE TABLE IF NOT EXISTS news.latest (
               id VARCHAR(255),
               title VARCHAR(255),
               description VARCHAR(1000),
               url VARCHAR(255),
               author VARCHAR(100),
               image VARCHAR(1000),
               language VARCHAR(50),
               category VARCHAR(50),
               publication_date TIMESTAMP,
               UNIQUE (id, category)
          );
     """

     QUERY_MERGE_2= """
          INSERT INTO news.latest (id, title, description, url, author, image, language, category, publication_date)
          SELECT id, title, description, url, author, image, language, category, publication_date
          FROM news.temp 
          ON CONFLICT (id, category) DO UPDATE SET 
               title = EXCLUDED.title,
               description = EXCLUDED.description,
               url = EXCLUDED.url,
               author = EXCLUDED.author,
               image = EXCLUDED.image,
               language = EXCLUDED.language,
               publication_date = EXCLUDED.publication_date
     """

     try:
          with engine.begin() as conn:
               conn.exec_driver_sql(QUERY_SCHEMA)
               conn.exec_driver_sql(QUERY_TEMP_TABLE)
               conn.exec_driver_sql(QUERY_LATEST_TABLE)

               df.to_sql(name="temp", con=conn, if_exists="append", schema="news", index=False)
          
               conn.exec_driver_sql(QUERY_MERGE_2)
               conn.exec_driver_sql("TRUNCATE TABLE news.temp")

          print("Data loaded to database successfully")
     except Exception as e:
          print(f"Error during SQL operation: {e}")
