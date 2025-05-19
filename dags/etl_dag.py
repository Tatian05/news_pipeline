from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from scripts.etl import get_coords, extract, transform, load

def get_coords_task(**context):
    coords = get_coords()
    context['ti'].xcom_push(key='coords', value=coords)

def extract_task(**context):
    coords = context['ti'].xcom_pull(key='coords', task_ids='get_coords')
    raw_data=extract(coords)
    context['ti'].xcom_push(key='raw_weather_data', value=raw_data)

def transform_task(**context):
    raw_data = context['ti'].xcom_pull(key='raw_weather_data', task_ids='extract_weather')
    data_to_load = transform(raw_data)
    context['ti'].xcom_push(key='transformed_weather_data', value=data_to_load)

def load_task(**context):
    data_to_load=context['ti'].xcom_pull(key='transformed_weather_data', task_ids='transform_weather')
    load(data_to_load)

with DAG(
    dag_id='weather_etl_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:
    
    start = DummyOperator(task_id='start')

    get_coords_op = PythonOperator(
        task_id='get_coords',
        python_callable=get_coords_task,
        provide_context=True
    )

    extract_op = PythonOperator(
        task_id='extract_weather',
        python_callable=extract_task,
        provide_context=True
    )

    transform_op = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_task,
        provide_context=True
    )

    load_op = PythonOperator(
        task_id='load_weather',
        python_callable=load_task,
        provide_context=True
    )

    end = DummyOperator(task_id='end')


start >> get_coords_op >> extract_op >> transform_op >> load_op >> end