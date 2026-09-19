"""Daily batch using the existing validated commands; all DAGs start paused."""
from datetime import timedelta
import json
import os
from pathlib import Path
import subprocess
import sys
import pendulum
from airflow.sdk import dag, task
from pipeline_alerts import failure_alert

ROOT = Path(os.getenv('PROJECT_ROOT', '/workspace'))
PROJECT = 'jcdeah-009'
BUCKET = 'jcdeah-009-daud-finalproject'


def run_script(script, *args):
    result = subprocess.run([sys.executable, str(ROOT / 'scripts' / script), *args],
                            cwd=ROOT, text=True, capture_output=True, timeout=600)
    print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    if result.returncode:
        raise RuntimeError(f'{script} failed with exit code {result.returncode}; see task output')
    return result.stdout


@dag(dag_id='jisdor_daily', schedule='0 18 * * *',
     start_date=pendulum.datetime(2026, 9, 12, tz='Asia/Jakarta'), catchup=False,
     max_active_runs=1, default_args={'retries': 2, 'retry_delay': timedelta(minutes=1),
                                    'retry_exponential_backoff': True,
                                    'execution_timeout': timedelta(minutes=12),
                                    'on_failure_callback': failure_alert},
     tags=['final-project', 'batch'])
def daily():
    @task
    def collect():
        return json.loads(run_script('collect_jisdor_batch.py'))['run_id']

    @task
    def upload_raw(source_run_id):
        run_script('upload_jisdor_gcs.py', '--source-dir', f'data/source_samples/{source_run_id}',
                   '--project', PROJECT, '--bucket', BUCKET, '--raw-only', '--upload')
        return source_run_id

    @task
    def transform_spark(source_run_id):
        run_script('transform_jisdor_spark.py', '--run-id', source_run_id,
                   '--project', PROJECT, '--bucket', BUCKET)
        return source_run_id

    @task
    def upload(source_run_id):
        run_script('upload_jisdor_gcs.py', '--source-dir', f'data/source_samples/{source_run_id}',
                   '--project', PROJECT, '--bucket', BUCKET, '--upload')
        return source_run_id

    @task
    def warehouse(source_run_id):
        run_script('load_jisdor_bigquery.py', '--project', PROJECT, '--bucket', BUCKET,
                   '--run-id', source_run_id)
        return source_run_id

    @task
    def check_dates(source_run_id):
        run_script('check_jisdor_coverage.py', '--source-run-id', source_run_id)

    check_dates(warehouse(upload(transform_spark(upload_raw(collect())))))


daily()


@dag(dag_id='failure_alert_demo', schedule=None,
     start_date=pendulum.datetime(2026, 9, 12, tz='Asia/Jakarta'), catchup=False,
     tags=['final-project', 'alert-test'])
def alert_demo():
    @task(retries=0, on_failure_callback=failure_alert)
    def intentionally_fail():
        raise RuntimeError('Intentional failure to verify local email delivery')
    intentionally_fail()


alert_demo()
