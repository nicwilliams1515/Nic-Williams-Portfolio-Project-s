
from email.policy import default
import requests
import json
import boto3
import airflow
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

start_date = airflow.utils.dates.days_ago(1)

default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
        }

def json_scraper(url, file_name, bucket):
    response = requests.request("GET", url)
    json_data = response.json()

    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)
    s3 = boto3.client('s3') 
    s3.upload_file(file_name, bucket,f"data/{file_name}")

with DAG(
    'raw_predictit',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=default_args,
    description='Testing a DAG',
    schedule_interval=timedelta(days=1),
    start_date=start_date,
    catchup=False,
    tags=['example_tag'],
) as dag:

    extract_predictit = PythonOperator(
        task_id='extract_predictit',
        python_callable=json_scraper,
        op_kwargs= {
            "url":"https://www.predictit.org/api/marketdata/all",
            "file_name":"predicit_markets.json",
            "bucket":""},
        dag=dag
    )

    ready = DummyOperator(task_id='ready')

    extract_predictit >> ready

