from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from scripts.etl import extract, transform, load

def extract_task(**context):
    raw_data=extract()
    context['ti'].xcom_push(key='raw_weather_data', value=raw_data)

def transform_task(**context):
    raw_data = context['ti'].xcom_pull(key='raw_weather_data', task_ids='extract_weather')
    df = transform(raw_data)
    context['ti'].xcom_push(key='transformed_weather_data', value=df)

def load_task(**context):
    df=context['ti'].xcom_pull(key='transformed_weather_data', task_ids='transform_data')
    load(df)


with DAG(
    dag_id='weather_etl_pipeline',
    start_date=datetime(),
    schedule_interval='@hourly',
    catchup=False
) as dag:
    
    start = DummyOperator(task_id='start')

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


start >> extract_op >> transform_op >> load_op >> end