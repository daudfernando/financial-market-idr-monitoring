from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from operator_pipeline.kirim_alert import failure_alert
from operator_pipeline.operator_kurs import OperatorKurs
from operator_pipeline.tahapan_kurs import baca_pipeline

config = baca_pipeline()
with DAG(
    dag_id=config["dag_id"],
    schedule=config["schedule"],
    start_date=pendulum.datetime(2026, 9, 12, tz=config["timezone"]),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=12),
        "on_failure_callback": failure_alert,
    },
    tags=["final-project", "batch"],
) as daily:
    tasks = {}
    for tahap in config["tasks"]:
        tasks[tahap["task_id"]] = OperatorKurs(
            task_id=tahap["task_id"], tahap=tahap, konfigurasi=config
        )
        for parent in tahap.get("depends_on", []):
            tasks[parent] >> tasks[tahap["task_id"]]
