import argparse
import json
import math
import os
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from pathlib import Path

from operator_pipeline.konfigurasi import ROOT
from operator_pipeline.simpan_gcs import encoded, item, upload_one


def build_plan(source_dir, raw_only=False):
    folder = Path(source_dir).resolve()
    allowed = (ROOT / "data" / "source_samples").resolve()
    if not folder.is_relative_to(allowed) or not re.fullmatch(
        r"\d{8}T\d{12}Z", folder.name
    ):
        raise ValueError(
            "Use a run folder under data/source_samples with its original run ID"
        )
    report = json.loads((folder / "report.json").read_text(encoding="utf-8"))
    if report["run_id"] != folder.name or report["jisdor"]["status"] != "passed":
        raise ValueError("Run ID mismatch or JISDOR source validation did not pass")

    successful = [
        a for a in report["jisdor"]["attempts"] if a.get("http_status") == 200
    ]
    if not successful:
        raise ValueError("No successful source response recorded")
    xml = (folder / f"bi_{successful[-1]['method'].lower()}.xml").read_bytes()
    rows = [
        {c.tag.split("}")[-1]: c.text for c in node}
        for node in ET.fromstring(xml).iter()
        if node.tag.split("}")[-1] == "Table"
    ]
    expected = {}
    for row in rows:
        dt = datetime.fromisoformat(row["tgl_subkursasing"]).date().isoformat()
        currency = row["mts_subkursasing"].strip().upper()
        rate = float(row["jual_subkursasing"])
        if currency != "USD" or not math.isfinite(rate) or rate <= 0 or dt in expected:
            raise ValueError("Invalid or duplicate source rate")
        expected[dt] = rate
    if not expected or len(expected) != report["jisdor"]["records"]:
        raise ValueError("Raw record count differs from report")
    run_id = folder.name
    collected_date = datetime.strptime(run_id, "%Y%m%dT%H%M%S%fZ").date().isoformat()
    prefix = "final-project"
    objects = [
        item(
            f"{prefix}/raw/jisdor/ingestion_date={collected_date}/run_id={run_id}/response.xml",
            xml,
            "application/xml",
        )
    ]
    if raw_only:
        return objects
    files = sorted((folder / "processed" / "jisdor").glob("date=*/record.json"))
    seen = set()
    for file in files:
        record = json.loads(file.read_text(encoding="utf-8"))
        dt = date.fromisoformat(record["jisdor_date"]).isoformat()
        if file.parent.name != f"date={dt}" or dt in seen:
            raise ValueError("Partition date mismatch or duplicate partition")
        if record["currency"] != "USD" or record["usd_idr"] != expected.get(dt):
            raise ValueError("Processed record differs from raw source")
        if record["source"] != report["jisdor"]["source"]:
            raise ValueError("Source provenance mismatch")
        ingested = datetime.fromisoformat(record["ingested_at"])
        if ingested.tzinfo is None:
            raise ValueError("Ingestion timestamp must include timezone")
        if record.get("fx_available_at") is not None:
            raise ValueError("Sample has unverified FX publication time")
        seen.add(dt)
        objects.append(
            item(
                f"{prefix}/processed/jisdor/date={dt}/run_id={run_id}/record.json",
                encoded(record),
                "application/json",
            )
        )
    if seen != set(expected):
        raise ValueError("Processed partitions do not cover all raw dates")
    manifest = {
        "run_id": run_id,
        "records": len(seen),
        "source": report["jisdor"]["source"],
        "objects": [
            {k: v for k, v in obj.items() if k != "content"} for obj in objects
        ],
    }

    objects.append(
        item(
            f"{prefix}/manifests/jisdor/run_id={run_id}/manifest.json",
            encoded(manifest),
            "application/json",
        )
    )
    return objects


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--project", default=os.getenv("GOOGLE_CLOUD_PROJECT"))
    parser.add_argument("--bucket", default=os.getenv("GCS_BUCKET"))
    parser.add_argument(
        "--upload", action="store_true", help="Write to an existing GCS bucket"
    )
    parser.add_argument(
        "--raw-only",
        action="store_true",
        help="Land raw XML before Spark transformation",
    )
    parser.add_argument("--use-local-adc", action="store_true")
    args = parser.parse_args()
    status = "failed"
    result = {}
    try:
        objects = build_plan(args.source_dir, raw_only=args.raw_only)
        result = {
            "mode": "upload" if args.upload else "dry_run",
            "project": args.project,
            "bucket": args.bucket,
            "object_count": len(objects),
            "total_bytes": sum(o["bytes"] for o in objects),
            "objects": [
                {k: v for k, v in o.items() if k != "content"} for o in objects
            ],
        }
        if args.upload:
            if not args.project or not args.bucket or "/" in args.bucket:
                raise ValueError(
                    "Supply --project and --bucket (bucket name without gs://)"
                )
            from operator_pipeline.koneksi_google import storage_client

            client = storage_client(args.project, args.use_local_adc)
            bucket = client.bucket(args.bucket)
            result["results"] = []
            for obj in objects:
                outcome = upload_one(bucket, obj)
                result["results"].append({"name": obj["name"], "status": outcome})
            status = "uploaded"
        else:
            status = "dry_run_passed"
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    result["status"] = status
    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = log_dir / f"gcs_batch_{stamp}.json"
    output.write_bytes(encoded(result))
    print(
        json.dumps(
            {k: v for k, v in result.items() if k not in ["objects", "results"]},
            indent=2,
        )
    )
    return int(status == "failed")


if __name__ == "__main__":
    raise SystemExit(main())
