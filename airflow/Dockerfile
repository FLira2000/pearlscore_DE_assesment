FROM apache/airflow:2.10.5-python3.12

USER airflow
COPY airflow/requirements.txt /tmp/requirements-extra.txt
RUN pip install --no-cache-dir -r /tmp/requirements-extra.txt
