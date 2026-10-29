import argparse
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from operator_pipeline.konfigurasi import ROOT
from scripts.ekstraksi.baca_xml_bi import collect_bi


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback-days", type=int, default=45)
    args = parser.parse_args()
    if not 2 <= args.lookback_days <= 365:
        parser.error("lookback-days must be 2..365")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    folder = ROOT / "data" / "source_samples" / run_id
    folder.mkdir(parents=True)
    today = datetime.now(ZoneInfo("Asia/Jakarta")).date()
    batch = collect_bi(
        folder,
        prepare_processed=False,
        start_date=(today - timedelta(days=args.lookback_days - 1)).isoformat(),
        end_date=today.isoformat(),
    )
    report = {"run_id": run_id, "jisdor": batch}
    (folder / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report))
    return int(batch["status"] != "passed")


if __name__ == "__main__":
    raise SystemExit(main())
