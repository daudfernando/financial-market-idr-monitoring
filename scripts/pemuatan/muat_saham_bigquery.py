import argparse
import json
import re
from datetime import datetime, timezone

from operator_pipeline.koneksi_google import bigquery_client, storage_client
from operator_pipeline.konfigurasi import ROOT
from operator_pipeline.muat_bigquery import muat_json_gcs, skema_saham
from operator_pipeline.simpan_gcs import encoded, item, upload_one
from scripts.validasi.cocokkan_event import reconcile


def main():
    from google.cloud import bigquery

    parser = argparse.ArgumentParser()
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--use-local-adc", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"\d{8}T\d{12}Z", args.source_run_id):
        raise ValueError("Invalid source run ID")
    folder = ROOT / "data/streaming" / args.source_run_id
    report = json.loads((folder / "report.json").read_text())
    if (
        report["status"] != "passed"
        or report["runner"] != "PythonConsumer"
        or report["modes"] != ["replay"]
    ):
        raise ValueError(
            "Only successful, explicitly labelled local replay is accepted"
        )
    raw = [
        json.loads(line)
        for line in (folder / "kafka_capture.jsonl").read_text().splitlines()
    ]
    rows = [
        json.loads(line)
        for file in sorted(folder.glob("valid-*.jsonl"))
        for line in file.read_text().splitlines()
    ]
    check = reconcile(raw, rows)
    if (
        check["status"] != "passed"
        or len(rows) != report["valid"]
        or len(raw) != report["kafka_records"]
    ):
        raise ValueError("Kafka and Python output do not reconcile")
    processor = "local-python"
    pipeline = "local_kafka_python_replay"
    prefix = f"final-project/demo/{processor}/{args.source_run_id}"
    objects = [
        item(
            f"{prefix}/raw.jsonl",
            (folder / "kafka_capture.jsonl").read_bytes(),
            "application/x-ndjson",
        ),
        item(
            f"{prefix}/valid.jsonl",
            "".join(json.dumps(row) + "\n" for row in rows).encode(),
            "application/x-ndjson",
        ),
    ]
    manifest = {
        "source_run_id": args.source_run_id,
        "pipeline": pipeline,
        "cloud_dataflow_executed": False,
        "records": len(rows),
        "objects": [
            {k: v for k, v in obj.items() if k != "content"} for obj in objects
        ],
    }
    objects.append(
        item(f"{prefix}/manifest.json", encoded(manifest), "application/json")
    )
    bucket = storage_client("jcdeah-009", args.use_local_adc).bucket(
        "jcdeah-009-daud-finalproject"
    )
    for obj in objects:
        upload_one(bucket, obj)
    client = bigquery_client("jcdeah-009", "asia-southeast2", args.use_local_adc)
    target = bigquery.Table(
        "jcdeah-009.daud_finalproject.stg_market_replay_demo", schema=skema_saham()
    )
    target.time_partitioning = bigquery.TimePartitioning(field="event_date")
    target.clustering_fields = ["symbol", "ingestion_mode"]
    target.description = f"DEMO: real historical replay via {pipeline}; GCS load after replay completes. Not live prices."
    client.create_table(target, exists_ok=True)
    client.update_table(target, ["description"])
    uri = f"gs://{bucket.name}/{prefix}/valid.jsonl"
    load = muat_json_gcs(client, uri, target.reference, skema_saham(), len(rows))
    stored = [
        dict(row)
        for row in client.query(
            "SELECT * FROM `jcdeah-009.daud_finalproject.stg_market_replay_demo`",
            job_config=bigquery.QueryJobConfig(maximum_bytes_billed=100 * 1024 * 1024),
        ).result(timeout=90)
    ]
    result = reconcile(raw, stored)
    result.update(
        pipeline=pipeline,
        cloud_dataflow_executed=False,
        source_run_id=args.source_run_id,
        load_job_id=load.job_id,
        uri=uri,
        checked_at=datetime.now(timezone.utc).isoformat(),
    )
    (folder / "demo_publish.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, indent=2))
    return int(result["status"] != "passed" or len(stored) != len(rows))


if __name__ == "__main__":
    raise SystemExit(main())
