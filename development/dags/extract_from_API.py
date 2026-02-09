import logging
import json
import io
import time

from massive import RESTClient
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio

default_args = {
    'owner':'airflow',
    'start_date': datetime(2026, 1, 5), 
    'retries': 3,
    "catchup": True,
    "retry_delay": timedelta(minutes=5),
}

def get_date(**context):
    start_date = context["data_interval_start"].strftime("%Y-%m-%d")
    return start_date

def extract_and_load_from_api_to_minio(**context):

    start_date = get_date(**context)
    logging.info(f"Сбор данных с API за: {start_date}")
    
    logging.info(f'Подключение к S3')
    s3_client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False  
    )
    bucket_name = 'prod'
    if not s3_client.bucket_exists(bucket_name):
        s3_client.make_bucket(bucket_name)
    logging.info(f'Подключение к S3 успешно пройдено, бакет найден')

    API_KEY = Variable.get("API_KEY")
    api_client = RESTClient(api_key = API_KEY)

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
   
    for index, ticker in enumerate(tickers):

        # objects = list(s3_client.list_objects(
        #     bucket_name=bucket_name, 
        #     prefix=f"{ticker}/{start_date}.json",
        #     recursive=False
        # ))

        # if len(objects) > 0:
        #     logging.info(f"Файл для ticker: {ticker} уже существует")
        #     continue

        if index % 5 == 0:
            logging.info("Пауза 65 секунд, из-за ограничений API...")
            time.sleep(65)
        aggs = api_client.get_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="minute",
            from_=start_date,
            to=start_date,
            limit = 5000
        )

        logging.info(f"Получено {len(aggs)} строк для {ticker}")
        
        data = []

        for agg in aggs:
            data_ticker = {
                'ticker':ticker,           
                'open':agg.open,
                'high':agg.high,
                'low':agg.low,
                'close':agg.close,
                'volume':agg.volume,
                'vwap':agg.vwap,
                'timestamp':agg.timestamp,
                'transactions':agg.transactions
            }

            data.append(data_ticker)

        json_bytes = json.dumps(data, ensure_ascii=False).encode('utf-8')

        s3_client.put_object(
            bucket_name,
            f"{ticker}/{start_date}.json",
            data=io.BytesIO(json_bytes),
            length=len(json_bytes),
            content_type='application/json; charset=utf-8'
        )

with DAG(
    dag_id='extract_from_API',
    schedule_interval="0 5 * * 1-5",     # новый день в 05:00 по utc
    default_args=default_args,
    tags=["docker"],
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    extract_and_load_from_api_to_minio = PythonOperator(
        task_id="extract_and_load_from_api_to_minio",
        python_callable=extract_and_load_from_api_to_minio,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> extract_and_load_from_api_to_minio >> end


