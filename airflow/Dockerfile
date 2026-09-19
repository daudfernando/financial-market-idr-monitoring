FROM apache/airflow:3.3.0-python3.12
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless && rm -rf /var/lib/apt/lists/*
USER airflow
COPY airflow/requirements.txt /tmp/project-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.3.0" -r /tmp/project-requirements.txt
