from datetime import datetime
from airflow.decorators import dag

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)


@dag(
    schedule=None,
    start_date= datetime(2024, 3, 1),
)
def spark_operator():


    submit = SparkKubernetesOperator(
        task_id="submit",
        namespace="airflow",
        application_file="spark-application.yaml",
        do_xcom_push=False,
        on_finish_action="delete_pod",
        get_logs=True,
    )

spark_operator()