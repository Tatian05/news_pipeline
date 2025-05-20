from etl import engine
from fastapi import FastAPI, HTTPException
from sqlalchemy.exc import SQLAlchemyError

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Weather API is active."}

@app.get("/data")
def get_weather_data():
    try:
        with engine.begin() as conn:
            result = conn.exec_driver_sql("SELECT * FROM weather.weather_data")
            columns = result.keys()
            data = [dict(zip(columns, row)) for row in result.fetchall()]
            return data
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")