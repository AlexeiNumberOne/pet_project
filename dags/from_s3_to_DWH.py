import logging
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from minio import Minio
#from sqlalchemy import create_engine

default_args = {
    'owner':'airflow',
    'start_date': datetime(2026, 1, 5), 
    'retries': 3,
    "catchup": True,
    "retry_delay": timedelta(minutes=5),
}

def get_dates(**context):
    start_date = context["data_interval_start"].strftime("%Y-%m-%d")
    end_date = context["data_interval_end"].strftime("%Y-%m-%d")
    return start_date, end_date

def get_and_load_from_s3_to_dwh(**context):

    start_date, end_date = get_dates(**context)
    logging.info(f"Загрузка данных с S3 в DWH за: {start_date}/{end_date}")

    logging.info(f'Подключение к S3')
    s3_client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False  
    )
    bucket_name = 'prod'
    if s3_client.bucket_exists(bucket_name):
        logging.info(f'Подключение к S3 успешно пройдено, бакет найден')

    tickers = [ 'AAPL',
                'MSFT',
                'GOOGL',
                'GOOG',
                'AMZN',
                'META',
                'TSLA',
                'NVDA',
                'INTC',
                'AMD',
                'ADBE',
                'JPM',
                'BAC',
                'GS',
                'MS',
                'V',
                'MA',
                'PG',
                'KO',
                'PEP',
                'WMT',
                'MCD',
                'BABA',
                'TSM',
                'ASML',
                'JNJ',
                'PFE',
                'MRK',
                'UNH',
                'ABBV'
            ]

    for ticker in tickers:
        with s3_client.get_object(bucket_name, f"{ticker}/{start_date}.json") as request:
            data = json.loads(request.read().decode('utf-8'))
        logging.info(f"Для ticker: {ticker} было получено {len(data)} строк")
    
with DAG(
    dag_id = 'from_s3_to_DWH',
    schedule_interval="0 0 * * 1-5",
    default_args=default_args,
    tags=["DWH"],
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    sensor = ExternalTaskSensor(
        task_id="sensor",
        external_dag_id="extract_from_API",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  # длительность работы сенсора
        poke_interval=60,  # частота проверки
    )

    get_and_load_from_s3_to_dwh = PythonOperator(
        task_id="get_and_load_from_s3_to_dwh",
        python_callable=get_and_load_from_s3_to_dwh,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor>> get_and_load_from_s3_to_dwh >> end

