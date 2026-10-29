from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import BaseOperator
from operator_pipeline.tahapan_kurs import jalankan_task


class OperatorKurs(BaseOperator):
    def __init__(self, *, tahap, konfigurasi, **kwargs):
        super().__init__(**kwargs)
        self.tahap = tahap
        self.konfigurasi = konfigurasi

    def execute(self, context):
        run = context["dag_run"]
        run_type = getattr(run.run_type, "value", run.run_type)
        if (
            self.task_id == "collect"
            and str(run_type).lower() == "manual"
            and run.run_id.startswith("demo_fail_collect_")
        ):
            raise AirflowFailException(
                "Controlled alert demo: collect intentionally failed before extraction"
            )
        source_run_id = (
            None
            if self.task_id == "collect"
            else context["ti"].xcom_pull(task_ids="collect")
        )
        return jalankan_task(self.tahap, self.konfigurasi, source_run_id)
