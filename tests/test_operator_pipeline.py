import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from operator_pipeline.muat_bigquery import muat_baris, muat_event, muat_json_gcs
from operator_pipeline.tahapan_kurs import baca_pipeline, jalankan_manual, perintah_task


class PipelineConfigurationTest(unittest.TestCase):
    def test_manual_and_scheduler_use_same_order(self):
        config = baca_pipeline()
        with patch(
            "operator_pipeline.tahapan_kurs.jalankan_task",
            return_value="20261024T010000000000Z",
        ) as run:
            jalankan_manual()
        self.assertEqual(
            [call.args[0]["task_id"] for call in run.call_args_list],
            [task["task_id"] for task in config["tasks"]],
        )
        self.assertEqual(run.call_args_list[-1].args[2], "20261024T010000000000Z")

    def test_failure_prevents_downstream_execution(self):
        with patch(
            "operator_pipeline.tahapan_kurs.jalankan_task",
            side_effect=RuntimeError("source failed"),
        ) as run:
            with self.assertRaises(RuntimeError):
                jalankan_manual()
        self.assertEqual(run.call_count, 1)

    def test_resume_transform_requires_source_id(self):
        config = baca_pipeline()
        task = next(t for t in config["tasks"] if t["task_id"] == "transform_spark")
        with self.assertRaises(ValueError):
            perintah_task(task, config)
        command = perintah_task(task, config, "20261024T010000000000Z")
        self.assertIn("scripts.transformasi.bersihkan_kurs_spark", command)
        self.assertIn("20261024T010000000000Z", command)

    def test_invalid_config_is_rejected_before_execution(self):
        config = baca_pipeline()
        for mutation in ["duplicate", "cycle", "unknown"]:
            value = copy.deepcopy(config)
            if mutation == "duplicate":
                value["tasks"][1]["task_id"] = "collect"
            elif mutation == "cycle":
                value["tasks"][0]["depends_on"] = ["warehouse"]
            else:
                value["tasks"][0]["proses"] = "arbitrary.module"
            with tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / "pipeline.yaml"
                path.write_text(yaml.safe_dump(value))
                with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                    baca_pipeline(path)


class WarehouseLoadTest(unittest.TestCase):
    def test_partial_load_is_not_success(self):
        client = Mock()
        client.load_table_from_json.return_value.output_rows = 0
        with self.assertRaises(ValueError):
            muat_baris(client, [{"id": 1}], "p.d.t", [], "asia-southeast2")

    def test_empty_load_never_reaches_client(self):
        client = Mock()
        with self.assertRaises(ValueError):
            muat_baris(client, [], "p.d.t", [], "asia-southeast2")
        client.load_table_from_json.assert_not_called()

    def test_gcs_count_checked_and_job_failure_propagates(self):
        client = Mock()
        client.load_table_from_uri.return_value.output_rows = 2
        with self.assertRaises(ValueError):
            muat_json_gcs(client, "gs://bucket/valid.jsonl", "p.d.t", [], 3)
        client.load_table_from_uri.return_value.result.side_effect = RuntimeError(
            "load failed"
        )
        with self.assertRaises(RuntimeError):
            muat_json_gcs(client, "gs://bucket/valid.jsonl", "p.d.t", [], 2)

    def test_event_rejection_propagates_without_mutating_input(self):
        client = Mock()
        row = {"id": "event-1"}
        client.insert_rows_json.return_value = [{"errors": [{"reason": "invalid"}]}]
        with self.assertRaises(RuntimeError):
            muat_event(client, "p.d.t", row, "run:event-1")
        self.assertEqual(row, {"id": "event-1"})


if __name__ == "__main__":
    unittest.main()
