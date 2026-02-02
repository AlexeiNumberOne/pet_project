from datetime import datetime
from airflow.decorators import dag

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

@dag(
    schedule=None,
    start_date= datetime(2024, 3, 1),
)
def spark_operator():


    submit = SparkKubernetesOperator(
        task_id="submit_and_wait",
        namespace="airflow",
        application_file="spark-application.yaml",
        kubernetes_conn_id="kubernetes_default",
        
        # 1. СОЗДАНИЕ И МОНИТОРИНГ:
        wait_for_completion=True,      # Ждать завершения
        watch=True,                    # Логировать в реальном времени
        get_logs=True,                 # Получать логи
        
        # 2. НАСТРОЙКИ МОНИТОРИНГА:
        startup_timeout_seconds=300,   # 5 минут на запуск
        polling_interval=10,           # Проверять каждые 10 секунд
        
        # 3. ОЧИСТКА РЕСУРСОВ:
        is_delete_operator_pod=True,   # Удалять Pod после
        on_finish_action="delete_pod", # Удалять при завершении
        termination_grace_period=120,  # 2 минуты на graceful shutdown
        
        # 4. ОБРАБОТКА ЗАВЕРШЕНИЯ:
        deferrable=False,              # Синхронный режим (проще)
        retries=0,                     # Без повторных попыток для теста
        
        # 5. XCOM (для отладки):
        do_xcom_push=True,             # Сохранить метаданные
    )

spark_operator()