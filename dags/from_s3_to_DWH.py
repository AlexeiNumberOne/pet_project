import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner':'airflow',
    'start_date': datetime(2026, 1, 6), 
    'retries': 3,
    "catchup": True,
    "retry_delay": timedelta(minutes=5),
}

def get_dates(**context):
    start_date = context["data_interval_start"]
    end_date = context["data_interval_end"]
    return start_date, end_date

def get_and_load_from_s3_to_dwh(**context):

    start_date, end_date = get_dates(**context)

    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")


with DAG(
    dag_id = 'from_s3_to_DWH',
    schedule_interval="0 5 * * *",
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

