FROM apache/airflow:2.7.0-python3.11

# Установка дополнительных пакетов
RUN pip install --no-cache-dir \
    massive==2.0.2 \
    minio==7.2.20 