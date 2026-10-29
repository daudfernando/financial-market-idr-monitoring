import argparse
import json
import sys
import time
from datetime import datetime, timezone

from operator_pipeline.jalankan_proses import jalankan as run
from operator_pipeline.konfigurasi import COMPOSE, ROOT


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-run-id",
        help="Reuse a passed local Kafka replay capture instead of producing again",
    )
    parser.add_argument(
        "--build", action="store_true", help="Build market and dbt images first"
    )
    args = parser.parse_args()
    run(COMPOSE + ["up", "-d", "airflow"])
    if args.build:
        run(COMPOSE + ["build", "market", "dbt"], timeout=1800)
    source_run_id = args.source_run_id
    if not source_run_id:
        market = json.loads(
            (ROOT / "config/current_market_replay.json").read_text(encoding="utf-8")
        )
        topic = "market.prices.demo." + datetime.now(timezone.utc).strftime(
            "%Y%m%d%H%M%S"
        )
        run(COMPOSE + ["up", "-d", "--wait", "kafka"])
        run(
            COMPOSE
            + [
                "exec",
                "-T",
                "kafka",
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "kafka:19092",
                "--create",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
            ]
        )
        source_run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        name = "replay-consumer-" + source_run_id.lower()
        run(
            COMPOSE
            + [
                "run",
                "-d",
                "--name",
                name,
                "market",
                "-m",
                "scripts.streaming.terima_event",
                "--topic",
                topic,
                "--limit",
                str(market["records"]),
                "--run-id",
                source_run_id,
            ]
        )
        try:
            ready = ROOT / "data/streaming" / source_run_id / "ready.json"
            deadline = time.monotonic() + 45
            while not ready.exists():
                if time.monotonic() > deadline:
                    raise RuntimeError("Consumer did not become ready")
                time.sleep(0.5)
            run(
                COMPOSE
                + [
                    "run",
                    "--rm",
                    "market",
                    "-m",
                    "scripts.streaming.kirim_event",
                    "--mode",
                    "replay",
                    "--topic",
                    topic,
                    "--input",
                    "/workspace/" + market["input"],
                    "--limit",
                    str(market["records"]),
                    "--interval",
                    "0.005",
                ]
            )
            code = run(["docker", "wait", name], timeout=330).strip()
            if code != "0":
                raise RuntimeError("Python consumer failed; inspect its report.json")
        finally:
            run(["docker", "rm", "-f", name])
    run(
        [
            sys.executable,
            "-m",
            "scripts.pemuatan.muat_saham_bigquery",
            "--source-run-id",
            source_run_id,
            "--use-local-adc",
        ]
    )
    run(
        COMPOSE
        + [
            "run",
            "--rm",
            "dbt",
            "build",
            "--vars",
            "{market_table: stg_market_replay_demo}",
        ]
    )
    run([sys.executable, "-m", "scripts.laporan.ekspor_dashboard", "--use-local-adc"])
    run([sys.executable, "-m", "scripts.validasi.periksa_mart", "--use-local-adc"])
    print(
        "Demo complete: dashboard/index.html. Historical replay; Kafka + Python. GCS -> BigQuery after replay."
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        alert_code = (
            "from types import SimpleNamespace; from operator_pipeline.kirim_alert import failure_alert; "
            "failure_alert({'task_instance': SimpleNamespace(dag_id='market_replay', "
            "task_id='run_demo', run_id='manual_replay')})"
        )
        try:
            run(
                COMPOSE + ["exec", "-T", "airflow", "python", "-c", alert_code],
                timeout=30,
            )
        except Exception as alert_error:
            print(
                "Local alert delivery failed:",
                type(alert_error).__name__,
                file=sys.stderr,
            )
        raise
