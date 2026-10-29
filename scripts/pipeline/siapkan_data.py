import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone

from operator_pipeline.jalankan_proses import jalankan as run
from operator_pipeline.konfigurasi import COMPOSE, ROOT


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--stocks-only",
        action="store_true",
        help="Refresh candidate archive only, no BigQuery publication",
    )
    args = parser.parse_args()
    run([sys.executable, "-m", "scripts.ekstraksi.ambil_histori_saham", "--activate"])
    if args.stocks_only:
        print(
            "Candidate updated; BigQuery is unchanged. Run python jalankan.py replay-lengkap after batch is ready."
        )
        return
    run(COMPOSE + ["up", "-d", "airflow"])
    run_id = "prepare_data_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for attempt in range(20):
        check = subprocess.run(
            COMPOSE
            + ["exec", "-T", "airflow", "airflow", "dags", "list", "--output", "json"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if check.returncode == 0 and "jisdor_daily" in check.stdout:
            break
        time.sleep(3)
    else:
        raise RuntimeError("Airflow not ready")
    run(
        COMPOSE
        + ["exec", "-T", "airflow", "airflow", "dags", "unpause", "jisdor_daily"]
    )
    run(
        COMPOSE
        + [
            "exec",
            "-T",
            "airflow",
            "airflow",
            "dags",
            "trigger",
            "jisdor_daily",
            "--run-id",
            run_id,
        ]
    )
    deadline = time.monotonic() + 1200
    print("Waiting for batch + date coverage audit:", run_id, flush=True)
    previous_state = None
    last_notice = 0
    while time.monotonic() < deadline:
        result = subprocess.run(
            COMPOSE
            + [
                "exec",
                "-T",
                "airflow",
                "airflow",
                "dags",
                "list-runs",
                "jisdor_daily",
                "--output",
                "json",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=60,
        )
        runs = []
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if line.strip().startswith("[{"):
                    runs = json.loads(line)
        match = next((r for r in runs if r["run_id"] == run_id), None)
        state = match["state"] if match else "waiting_for_scheduler"
        if state != previous_state or time.monotonic() - last_notice > 30:
            print("Airflow batch:", state, flush=True)
            previous_state, last_notice = state, time.monotonic()
        if match and match["state"] == "success":
            break
        if match and match["state"] == "failed":
            raise RuntimeError(
                "Batch failed; inspect Airflow. Stock publication not continued."
            )
        time.sleep(5)
    else:
        raise RuntimeError("Batch timeout; inspect Airflow")
    run([sys.executable, "-m", "scripts.pipeline.jalankan_replay"])
    print(
        "READY: latest candidate, batch/date audit, BigQuery marts and dashboard updated. Use python jalankan.py replay-event on demo day."
    )


if __name__ == "__main__":
    main()
