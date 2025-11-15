FROM apache/airflow:2.9.2

# Ejecutar pip como usuario airflow (no root)
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV AIRFLOW_HOME=/opt/airflow
