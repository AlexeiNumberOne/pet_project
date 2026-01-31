from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.apache.spark.sensors.spark_kubernetes import SparkKubernetesSensor

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
    description='Тестовый DAG для запуска Spark через SparkKubernetesOperator',
    schedule_interval=None,  # Запуск только вручную
    catchup=False,
    tags=['test', 'spark'],
) as dag:
    
    # 1. Оператор для отправки SparkApplication
    submit_spark_job = SparkKubernetesOperator(
        task_id='submit_spark_test_job',
        namespace='spark-jobs',
        application_file='spark_applications/spark-application.yaml',  # Путь к вашему YAML
        kubernetes_conn_id='kubernetes_default',  # Connection в Airflow к K8s
        do_xcom_push=True,  # Важно! Передает имя SparkApplication в XCom
        on_failure_action='delete',  # Удалить ресурс при ошибке
    )
    
    # 2. Сенсор для отслеживания выполнения
    monitor_spark_job = SparkKubernetesSensor(
        task_id='monitor_spark_test_job',
        namespace='spark-jobs',
        # Получаем имя SparkApplication из XCom предыдущей задачи
        application_name="{{ task_instance.xcom_pull(task_ids='submit_spark_test_job')['metadata']['name'] }}",
        kubernetes_conn_id='kubernetes_default',
        attach_log=True,  # Показать логи Spark в Airflow UI
        timeout=3600,  # Таймаут 1 час
        mode='reschedule',  # Эффективный режим ожидания
        poke_interval=30,  # Проверять каждые 30 секунд
    )
    
    # Определяем порядок задач
    submit_spark_job >> monitor_spark_job