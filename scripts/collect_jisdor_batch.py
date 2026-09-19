"""Collect only JISDOR for a scheduled batch; fail if source validation fails."""
try:
    from .command_log import open_progress
except ImportError:
    from command_log import open_progress
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import argparse
from pathlib import Path
import json
from command_log import log_command
from jisdor_source import collect_bi

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--lookback-days', type=int, default=45)
    args = parser.parse_args()
    if not 2 <= args.lookback_days <= 365:
        parser.error('lookback-days must be 2..365')
    log_command()
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    folder = ROOT / 'data' / 'source_samples' / run_id
    folder.mkdir(parents=True)
    today = datetime.now(ZoneInfo('Asia/Jakarta')).date()
    batch = collect_bi(folder, prepare_processed=False,
                       start_date=(today-timedelta(days=args.lookback_days-1)).isoformat(),
                       end_date=today.isoformat())
    report = {'run_id': run_id, 'jisdor': batch}
    (folder / 'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    with open_progress() as file:
        file.write(f'- Scheduled collection: {batch["status"]}; records={batch.get("records", 0)}; run_id={run_id}\n')
    print(json.dumps(report))
    return int(batch['status'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())
