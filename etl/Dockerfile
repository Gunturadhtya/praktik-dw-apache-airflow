ARG AIRFLOW_VERSION=3.0.6
FROM apache/airflow:${AIRFLOW_VERSION}

COPY src/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV PYTHONPATH=/opt/airflow/etl_src