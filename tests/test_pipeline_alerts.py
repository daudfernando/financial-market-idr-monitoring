import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from operator_pipeline.kirim_alert import failure_alert


class AlertTest(unittest.TestCase):
    def context(self):
        return {
            "task_instance": SimpleNamespace(
                dag_id="test", task_id="fail", run_id="manual"
            )
        }

    def test_success_records_delivery(self):
        with (
            tempfile.TemporaryDirectory() as folder,
            patch.dict("os.environ", {"PROJECT_ROOT": folder}),
            patch("operator_pipeline.kirim_alert.smtplib.SMTP") as smtp,
        ):
            failure_alert(self.context())
            smtp.return_value.__enter__.return_value.send_message.assert_called_once()
            record = json.loads((Path(folder) / "logs/alerts.jsonl").read_text())
            self.assertEqual(record["status"], "sent")

    def test_smtp_failure_is_not_silenced(self):
        with (
            tempfile.TemporaryDirectory() as folder,
            patch.dict("os.environ", {"PROJECT_ROOT": folder}),
            patch(
                "operator_pipeline.kirim_alert.smtplib.SMTP",
                side_effect=ConnectionError("test failure"),
            ),
        ):
            with self.assertRaises(ConnectionError), self.assertLogs(level="ERROR"):
                failure_alert(self.context())
            record = json.loads((Path(folder) / "logs/alerts.jsonl").read_text())
            self.assertEqual(record["status"], "delivery_failed")


if __name__ == "__main__":
    unittest.main()
