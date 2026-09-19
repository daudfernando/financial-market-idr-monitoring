"""Reconcile one Dataflow run's finalized raw GCS records with BigQuery staging."""
import argparse
import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'streaming'))
from events import validate


def coordinate(row):
    return row['kafka_topic'], int(row['kafka_partition']), int(row['kafka_offset'])


def normalized(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value


def reconcile(raw, warehouse):
    expected = {}
    for envelope in raw:
        key = coordinate(envelope)
        event = validate(json.loads(base64.b64decode(envelope['value_base64'], validate=True).decode()))
        if key in expected and expected[key] != event:
            raise ValueError('Conflicting raw payloads for the same Kafka coordinate')
        expected[key] = event
    found = {}
    for row in warehouse:
        key = coordinate(row)
        if key in expected:
            found.setdefault(key, []).append(row)
    missing = sorted(set(expected) - set(found))
    mismatches = []
    for key, rows in found.items():
        for row in rows:
            for field, value in expected[key].items():
                actual = normalized(row.get(field))
                if field in {'event_timestamp', 'ingested_at', 'replayed_at'} and value is not None:
                    value = datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc).isoformat()
                if field == 'event_date' and actual is not None:
                    actual = str(actual)
                if actual != value:
                    mismatches.append({'coordinate': key, 'field': field})
    return {'status': 'passed' if expected and not missing and not mismatches else 'failed',
            'raw_records': len(raw), 'raw_coordinates': len(expected),
            'warehouse_coordinates': len(found), 'missing': missing, 'mismatches': mismatches,
            'duplicate_warehouse_rows': sum(len(rows) - 1 for rows in found.values()),
            'unique_event_ids': len({row['event_id'] for row in expected.values()})}


def main():
    from gcp_auth import storage_client, bigquery_client
    from google.cloud import bigquery
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-label', required=True)
    parser.add_argument('--use-local-adc', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', args.run_label):
        parser.error('Use a lowercase run label containing only letters, digits and hyphens')
    client = storage_client('jcdeah-009', args.use_local_adc)
    prefix = f'final-project/market/{args.run_label}/raw/'
    raw = []
    files = []
    for blob in client.list_blobs('jcdeah-009-daud-finalproject', prefix=prefix):
        relative = blob.name[len(prefix):]
        if '/' in relative or not relative.startswith('events') or not relative.endswith('.jsonl'):
            continue  # FileIO temporary files are not completed output.
        files.append(blob.name)
        for line in blob.download_as_text(if_generation_match=blob.generation, timeout=60).splitlines():
            if line:
                raw.append(json.loads(line))
            if len(raw) > 10000:
                raise ValueError('Verifier is bounded to 10,000 records per demo run')
    if not raw:
        result = {'status': 'not_ready', 'reason': 'No finalized raw GCS records for this run', 'raw_records': 0}
    else:
        events = [validate(json.loads(base64.b64decode(row['value_base64'], validate=True).decode())) for row in raw]
        dates = [row['event_date'] for row in events]
        topics = sorted({row['kafka_topic'] for row in raw})
        query = '''SELECT * FROM `jcdeah-009.daud_finalproject.stg_market_events`
                   WHERE event_date BETWEEN @first_date AND @last_date
                   AND kafka_topic IN UNNEST(@topics)
                   AND kafka_offset BETWEEN @min_offset AND @max_offset LIMIT 10001'''
        config = bigquery.QueryJobConfig(maximum_bytes_billed=100 * 1024 * 1024, query_parameters=[
            bigquery.ScalarQueryParameter('first_date', 'DATE', min(dates)),
            bigquery.ScalarQueryParameter('last_date', 'DATE', max(dates)),
            bigquery.ArrayQueryParameter('topics', 'STRING', topics),
            bigquery.ScalarQueryParameter('min_offset', 'INT64', min(row['kafka_offset'] for row in raw)),
            bigquery.ScalarQueryParameter('max_offset', 'INT64', max(row['kafka_offset'] for row in raw))])
        warehouse = [dict(row) for row in bigquery_client('jcdeah-009', 'asia-southeast2', args.use_local_adc)
                     .query(query, job_config=config).result(timeout=90)]
        if len(warehouse) > 10000:
            raise ValueError('Warehouse scan exceeds bounded verifier; narrow the demo run')
        result = reconcile(raw, warehouse)
    result.update(run_label=args.run_label, finalized_raw_files=files,
                  scope='Finalized raw GCS records versus staging; does not measure uncaptured Kafka offsets',
                  checked_at=datetime.now(timezone.utc).isoformat())
    folder = ROOT / 'data' / 'streaming_validation'
    folder.mkdir(parents=True, exist_ok=True)
    output = folder / f'{args.run_label}-{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")}.json'
    output.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps(result, indent=2))
    return int(result['status'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())
