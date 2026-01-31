from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
with DAG(
    dag_id='test_spark_kubernetes',
    default_args=default_args,
    description='Тестовый DAG для запуска Spark',
    schedule_interval=None,  # Запуск только вручную
    catchup=False,
    tags=['test', 'spark'],
) as dag:
    
    run_spark = BashOperator(
        task_id='spark_bash_exec',
        bash_command="""
        kubectl exec -n spark deploy/spark-client-service -- \
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            /opt/spark-jobs/test_spark_job.py \
            --date {{ ds }}
        """
    )



