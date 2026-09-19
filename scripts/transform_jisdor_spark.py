"""Transform raw JISDOR from GCS using local PySpark; publish only valid batches."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def transform(spark, xml_path):
    from pyspark.sql import functions as F
    raw = (spark.read.format('xml').option('rowTag', 'Table').option('mode', 'FAILFAST')
           .schema('tgl_subkursasing STRING, mts_subkursasing STRING, jual_subkursasing STRING')
           .load(str(xml_path)))
    frame = raw.select(
        F.to_date(F.try_to_timestamp('tgl_subkursasing')).alias('jisdor_date'),
        F.upper(F.trim('mts_subkursasing')).alias('currency'),
        F.expr('try_cast(jual_subkursasing AS DECIMAL(38,9))').alias('usd_idr'))
    # Small daily source: collect after Spark parsing/casting; validate before any output.
    rows = frame.orderBy('jisdor_date').collect()
    if not rows:
        raise ValueError('Empty JISDOR batch')
    seen = set()
    for row in rows:
        if row.jisdor_date is None or row.currency != 'USD' or row.usd_idr is None or row.usd_idr <= 0:
            raise ValueError('Invalid JISDOR date, currency or rate')
        key = (row.jisdor_date, row.currency)
        if key in seen:
            raise ValueError('Duplicate JISDOR date and currency')
        seen.add(key)
    return rows


def main():
    from gcp_auth import storage_client
    from upload_jisdor_gcs import build_plan, encoded
    from pyspark.sql import SparkSession
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--use-local-adc', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'\d{8}T\d{12}Z', args.run_id):
        raise ValueError('Invalid source run ID')
    folder = ROOT / 'data' / 'source_samples' / args.run_id
    expected = build_plan(folder, raw_only=True)[0]
    content = storage_client(args.project, args.use_local_adc).bucket(args.bucket).blob(
        expected['name']).download_as_bytes(timeout=60)
    if hashlib.sha256(content).hexdigest() != expected['sha256']:
        raise ValueError('GCS raw XML differs from collected source')
    input_dir = folder / 'spark_input'
    input_dir.mkdir(exist_ok=True)
    xml_path = input_dir / 'response.xml'
    xml_path.write_bytes(content)
    spark = (SparkSession.builder.master('local[2]').appName('jisdor-batch')
             .config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '2')
             .config('spark.sql.session.timeZone', 'Asia/Jakarta').getOrCreate())
    try:
        rows = transform(spark, xml_path)
        report = json.loads((folder / 'report.json').read_text(encoding='utf-8'))
        if len(rows) != report['jisdor']['records']:
            raise ValueError('Spark count differs from source report')
        ingested_at = datetime.strptime(args.run_id, '%Y%m%dT%H%M%S%fZ').replace(tzinfo=timezone.utc).isoformat()
        for row in rows:
            record = {'jisdor_date': row.jisdor_date.isoformat(), 'currency': row.currency,
                      'usd_idr': float(row.usd_idr), 'source': report['jisdor']['source'],
                      'ingested_at': ingested_at, 'fx_available_at': None}
            target = folder / 'processed' / 'jisdor' / f'date={row.jisdor_date}' / 'record.json'
            target.parent.mkdir(parents=True, exist_ok=True)
            payload = encoded(record)
            if target.exists() and target.read_bytes() != payload:
                raise ValueError('Existing processed output differs; use a fresh source run')
            target.write_bytes(payload)
        build_plan(folder)  # Reconcile every output with raw before downstream publish.
        result = {'status': 'passed', 'engine': 'pyspark', 'version': spark.version,
                  'run_id': args.run_id, 'records': len(rows), 'invalid': 0, 'duplicates': 0,
                  'raw_sha256': expected['sha256'], 'input_uri': f'gs://{args.bucket}/{expected["name"]}'}
        (folder / 'spark_report.json').write_bytes(encoded(result))
        print(json.dumps(result))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
