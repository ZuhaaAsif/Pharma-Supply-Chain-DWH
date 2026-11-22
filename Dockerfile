# Airflow 3 image + SDI pipeline dependencies (also used for the ingestion API / watcher services)
FROM apache/airflow:3.1.8-python3.12
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
ENV PYTHONPATH=/opt/airflow/project \
    SDI_PROJECT_ROOT=/opt/airflow/project
