"""Read a complete GCS batch, validate hashes, stage rows, and MERGE into BigQuery."""
try:
    from .command_log import open_progress
except ImportError:
    from command_log import open_progress
import argparse
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]


def validated_record(record, run_id):
    result = dict(record)
    result['jisdor_date'] = date.fromisoformat(record['jisdor_date']).isoformat()
    rate = Decimal(str(record['usd_idr']))
    if not rate.is_finite() or rate <= 0 or record['currency'] != 'USD':
        raise ValueError('Invalid rate or currency')
    if rate.as_tuple().exponent < -9 or rate >= Decimal('1e29'):
        raise ValueError('Rate is outside BigQuery NUMERIC limits')
    ingested = datetime.fromisoformat(record['ingested_at'])
    if ingested.tzinfo is None:
        raise ValueError('Ingestion timestamp must include timezone')
    if record.get('fx_available_at') is not None:
        available = datetime.fromisoformat(record['fx_available_at'])
        if available.tzinfo is None:
            raise ValueError('FX availability timestamp must include timezone')
    if not record.get('source'):
        raise ValueError('Missing source provenance')
    result['usd_idr'] = str(rate)
    result['source_run_id'] = run_id
    return result


def read_batch(bucket, run_id):
    manifest_name = f'final-project/manifests/jisdor/run_id={run_id}/manifest.json'
    manifest = json.loads(bucket.blob(manifest_name).download_as_bytes(timeout=30))
    if manifest['run_id'] != run_id or not manifest['objects']:
        raise ValueError('Manifest run mismatch or empty manifest')
    rows, keys, names = [], set(), set()
    for obj in manifest['objects']:
        name = obj['name']
        if name in names or f'/run_id={run_id}/' not in name:
            raise ValueError('Duplicate object or foreign run in manifest')
        names.add(name)
        if not name.startswith(('final-project/raw/jisdor/', 'final-project/processed/jisdor/')):
            raise ValueError('Manifest contains an unexpected object prefix')
        content = bucket.blob(name).download_as_bytes(timeout=30)
        if len(content) != obj['bytes'] or hashlib.sha256(content).hexdigest() != obj['sha256']:
            raise ValueError(f'GCS content differs from manifest: {name}')
        if name.startswith('final-project/processed/jisdor/'):
            record = validated_record(json.loads(content), run_id)
            if f'/date={record["jisdor_date"]}/' not in name:
                raise ValueError('Partition date mismatch')
            if record['source'] != manifest['source']:
                raise ValueError('Source mismatch')
            key = (record['jisdor_date'], record['currency'])
            if key in keys:
                raise ValueError('Duplicate business key in batch')
            keys.add(key)
            rows.append(record)
    if not rows or len(rows) != manifest['records']:
        raise ValueError('Manifest record count mismatch')
    return rows, manifest_name


def table_schema():
    return [bigquery.SchemaField('jisdor_date', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('currency', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('usd_idr', 'NUMERIC', mode='REQUIRED'),
            bigquery.SchemaField('source', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('ingested_at', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('fx_available_at', 'TIMESTAMP'),
            bigquery.SchemaField('source_run_id', 'STRING', mode='REQUIRED')]


def main():
    from command_log import log_command
    from gcp_auth import storage_client, bigquery_client
    command = log_command()
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--dataset', default='daud_finalproject')
    parser.add_argument('--location', default='asia-southeast2')
    parser.add_argument('--use-local-adc', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z][a-z0-9-]{4,61}[a-z0-9]', args.project):
        parser.error('Invalid project ID')
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,100}', args.dataset):
        parser.error('Invalid dataset ID')
    if not re.fullmatch(r'\d{8}T\d{12}Z', args.run_id):
        parser.error('Invalid run ID')
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    result = {'status': 'failed', 'command': command, 'project': args.project,
              'dataset': args.dataset, 'location': args.location, 'source_run_id': args.run_id}
    try:
        storage = storage_client(args.project, args.use_local_adc)
        bucket = storage.get_bucket(args.bucket, timeout=30, retry=None)
        if bucket.location.lower() != args.location.lower():
            raise ValueError('Bucket and requested dataset region differ')
        rows, manifest_name = read_batch(bucket, args.run_id)
        result.update(source_records=len(rows), source_manifest=f'gs://{args.bucket}/{manifest_name}')
        client = bigquery_client(args.project, args.location, args.use_local_adc)
        dataset_id = f'{args.project}.{args.dataset}'
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = args.location
        dataset.description = 'Daud Final Project: financial market and IDR exposure data'
        dataset.labels = {'owner': 'daud', 'purpose': 'final-project'}
        actual = client.create_dataset(dataset, exists_ok=True, timeout=30, retry=None)
        if actual.location.lower() != args.location.lower():
            raise ValueError('Existing dataset has a different location')
        target = f'{dataset_id}.fact_jisdor_daily'
        table = bigquery.Table(target, schema=table_schema())
        table.time_partitioning = bigquery.TimePartitioning(field='jisdor_date')
        table.clustering_fields = ['currency']
        table.description = 'Latest ingested JISDOR per reference date and currency; availability may be unknown.'
        actual_table = client.create_table(table, exists_ok=True, timeout=30, retry=None)
        if [(f.name, f.field_type, f.mode) for f in actual_table.schema] != [
                (f.name, f.field_type, f.mode) for f in table_schema()]:
            raise ValueError('Existing target schema differs; no schema changes made')
        staging = f'{dataset_id}.stg_jisdor_{stamp.lower()}'
        stage_table = bigquery.Table(staging, schema=table_schema())
        stage_table.expires = datetime.now(timezone.utc) + timedelta(days=1)
        client.create_table(stage_table, timeout=30, retry=None)
        result.update(target_table=target, staging_table=staging)
        load = client.load_table_from_json(rows, staging, location=args.location,
                   job_config=bigquery.LoadJobConfig(schema=table_schema(),
                                                    write_disposition='WRITE_EMPTY'))
        result['load_job_id'] = load.job_id
        load.result(timeout=120)
        if load.output_rows != len(rows):
            raise ValueError('Staging load count differs from manifest')
        query_config = bigquery.QueryJobConfig(maximum_bytes_billed=100 * 1024 * 1024)
        # Refuse to merge into an already inconsistent target.
        duplicate_sql = f'SELECT COUNT(*) n FROM (SELECT 1 FROM `{target}` GROUP BY jisdor_date, currency HAVING COUNT(*) > 1)'
        if next(iter(client.query(duplicate_sql, job_config=query_config).result(timeout=60))).n:
            raise ValueError('Target contains duplicate keys; merge cancelled')
        sql = (ROOT / 'sql' / 'merge_jisdor.sql').read_text().format(target=target, staging=staging)
        merge = client.query(sql, job_config=query_config, location=args.location)
        result['merge_job_id'] = merge.job_id
        merge.result(timeout=120)
        result['affected_rows'] = merge.num_dml_affected_rows
        checks = f'''SELECT COUNT(*) total_rows,
          COUNT(*) - COUNT(DISTINCT CONCAT(CAST(jisdor_date AS STRING), '|', currency)) duplicate_keys,
          COUNTIF(usd_idr <= 0 OR currency != 'USD' OR ingested_at IS NULL) invalid_rows,
          MIN(jisdor_date) first_date, MAX(jisdor_date) last_date
          FROM `{target}`'''
        quality_job = client.query(checks, job_config=query_config)
        quality = dict(next(iter(quality_job.result(timeout=60))))
        quality['first_date'] = quality['first_date'].isoformat()
        quality['last_date'] = quality['last_date'].isoformat()
        result['quality'] = quality
        if quality['duplicate_keys'] or quality['invalid_rows']:
            raise ValueError('Warehouse quality checks failed')
        reconciliation = f'''SELECT COUNT(*) mismatches FROM `{staging}` S LEFT JOIN `{target}` T
          USING (jisdor_date, currency)
          WHERE T.jisdor_date IS NULL OR T.ingested_at < S.ingested_at
          OR (T.ingested_at = S.ingested_at AND (T.usd_idr != S.usd_idr
          OR T.source != S.source OR T.fx_available_at IS DISTINCT FROM S.fx_available_at))'''
        mismatch = next(iter(client.query(reconciliation, job_config=query_config).result(timeout=60))).mismatches
        result['batch_mismatches'] = mismatch
        if mismatch:
            raise ValueError('Warehouse rows do not reconcile to source batch')
        result['status'] = 'passed'
    except Exception as exc:
        result['error'] = f'{type(exc).__name__}: {exc}'
    output = ROOT / 'logs' / f'bigquery_batch_{stamp}.json'
    output.write_text(json.dumps(result, indent=2), encoding='utf-8')
    with open_progress() as file:
        file.write(f'- Hasil BigQuery: {result["status"]}; '
                   f'source_records={result.get("source_records", 0)}; '
                   f'affected_rows={result.get("affected_rows", "n/a")}\n'
                   f'- Bukti: logs/{output.name}\n')
        if result.get('error'):
            file.write(f'- Kendala: {result["error"]}\n')
    print(json.dumps(result, indent=2))
    return int(result['status'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())
