FROM python:3.11-slim-bookworm
RUN pip install --no-cache-dir dbt-core==1.10.11 dbt-bigquery==1.10.2
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace/analytics
ENV DBT_PROFILES_DIR=/workspace/analytics DBT_LOG_PATH=/workspace/data/dbt/logs DBT_TARGET_PATH=/workspace/data/dbt/target
ENTRYPOINT ["dbt"]
