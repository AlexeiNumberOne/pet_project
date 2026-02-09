import logging
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from minio import Minio


default_args = {
    'owner':'airflow',
    'start_date': datetime(2026, 1, 9), 
    'retries': 3,
    "catchup": True,
    "retry_delay": timedelta(minutes=5),
}

def get_dates(**context):
    start_date = context["data_interval_start"].strftime("%Y-%m-%d")

    return start_date

def get_from_s3(**context):

    start_date = get_dates(**context)
    logging.info(f"Загрузка данных с S3 в DWH за: {start_date}")

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
    
    engine = create_engine('postgresql://postgres:postgres@postgres_dwh:5432/postgres')
    Session = sessionmaker(bind=engine)
    session = Session()

    tickers = [ 'AAPL'
                ,'MSFT',
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
        with s3_client.get_object(bucket_name, f"{ticker}/{start_date}.json") as request:
            dataset = json.loads(request.read().decode('utf-8'))
            logging.info(f'ticker: {ticker} - получено {len(dataset)} строк')
            
            for data in dataset:
                timestampz = datetime.fromtimestamp(data['timestamp']/1000)
                sql=text(f"""INSERT INTO minute_bars (ticker_id, time_stamp, open_price, high_price, low_price, close_price, volume, vwap, transactions) 
                        VALUES ({index + 1}, '{timestampz}', {data['open']}, {data['high']}, {data['low']}, {data['close']}, {data['volume']}, {data['vwap']}, {data['transactions']})""")
                
                session.execute(sql)

        session.commit()
        logging.info(f"Данные ticker:{ticker} успешно записаны")    
 
with DAG(
    dag_id = 'from_s3_to_DWH',
    schedule_interval="0 5 * * 1-5",
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

    get_from_s3 = PythonOperator(
        task_id="get_from_s3",
        python_callable=get_from_s3,
    )

    
    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor >> get_from_s3 >> end

