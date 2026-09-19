"""Validate an existing JISDOR sample, plan GCS objects, then optionally upload."""
try:
    from .command_log import open_progress
except ImportError:
    from command_log import open_progress
import argparse
from datetime import date, datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]


def encoded(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2) + '\n').encode('utf-8')


def item(name, content, content_type):
    return {'name': name, 'content': content, 'content_type': content_type,
            'sha256': hashlib.sha256(content).hexdigest(), 'bytes': len(content)}


def build_plan(source_dir, raw_only=False):
    folder = Path(source_dir).resolve()
    allowed = (ROOT / 'data' / 'source_samples').resolve()
    if not folder.is_relative_to(allowed) or not re.fullmatch(r'\d{8}T\d{12}Z', folder.name):
        raise ValueError('Use a run folder under data/source_samples with its original run ID')
    report = json.loads((folder / 'report.json').read_text(encoding='utf-8'))
    if report['run_id'] != folder.name or report['jisdor']['status'] != 'passed':
        raise ValueError('Run ID mismatch or JISDOR source validation did not pass')
    # Select the successful response; never upload arbitrary files from the folder.
    successful = [a for a in report['jisdor']['attempts'] if a.get('http_status') == 200]
    if not successful:
        raise ValueError('No successful source response recorded')
    xml = (folder / f'bi_{successful[-1]["method"].lower()}.xml').read_bytes()
    rows = [{c.tag.split('}')[-1]: c.text for c in node}
            for node in ET.fromstring(xml).iter() if node.tag.split('}')[-1] == 'Table']
    expected = {}
    for row in rows:
        dt = datetime.fromisoformat(row['tgl_subkursasing']).date().isoformat()
        currency = row['mts_subkursasing'].strip().upper()
        rate = float(row['jual_subkursasing'])
        if currency != 'USD' or not math.isfinite(rate) or rate <= 0 or dt in expected:
            raise ValueError('Invalid or duplicate source rate')
        expected[dt] = rate
    if not expected or len(expected) != report['jisdor']['records']:
        raise ValueError('Raw record count differs from report')
    run_id = folder.name
    collected_date = datetime.strptime(run_id, '%Y%m%dT%H%M%S%fZ').date().isoformat()
    prefix = 'final-project'
    objects = [item(f'{prefix}/raw/jisdor/ingestion_date={collected_date}/run_id={run_id}/response.xml',
                    xml, 'application/xml')]
    if raw_only:
        return objects
    files = sorted((folder / 'processed' / 'jisdor').glob('date=*/record.json'))
    seen = set()
    for file in files:
        record = json.loads(file.read_text(encoding='utf-8'))
        dt = date.fromisoformat(record['jisdor_date']).isoformat()
        if file.parent.name != f'date={dt}' or dt in seen:
            raise ValueError('Partition date mismatch or duplicate partition')
        if record['currency'] != 'USD' or record['usd_idr'] != expected.get(dt):
            raise ValueError('Processed record differs from raw source')
        if record['source'] != report['jisdor']['source']:
            raise ValueError('Source provenance mismatch')
        ingested = datetime.fromisoformat(record['ingested_at'])
        if ingested.tzinfo is None:
            raise ValueError('Ingestion timestamp must include timezone')
        if record.get('fx_available_at') is not None:
            raise ValueError('Sample has unverified FX publication time')
        seen.add(dt)
        objects.append(item(f'{prefix}/processed/jisdor/date={dt}/run_id={run_id}/record.json',
                            encoded(record), 'application/json'))
    if seen != set(expected):
        raise ValueError('Processed partitions do not cover all raw dates')
    manifest = {'run_id': run_id, 'records': len(seen), 'source': report['jisdor']['source'],
                'objects': [{k: v for k, v in obj.items() if k != 'content'} for obj in objects]}
    # Publish manifest LAST. Downstream jobs must consume only runs with a manifest.
    objects.append(item(f'{prefix}/manifests/jisdor/run_id={run_id}/manifest.json',
                        encoded(manifest), 'application/json'))
    return objects


def upload_one(bucket, obj):
    from google.api_core.exceptions import PreconditionFailed
    from google.cloud.storage.retry import DEFAULT_RETRY_IF_GENERATION_SPECIFIED
    blob = bucket.blob(obj['name'])
    try:
        blob.upload_from_string(obj['content'], content_type=obj['content_type'],
                                if_generation_match=0, checksum='crc32c', timeout=60,
                                retry=DEFAULT_RETRY_IF_GENERATION_SPECIFIED)
        return 'uploaded'
    except PreconditionFailed:
        # Compare bytes, not untrusted metadata. Pin the generation during the read.
        blob.reload(timeout=30)
        existing = blob.download_as_bytes(if_generation_match=blob.generation, timeout=60)
        if hashlib.sha256(existing).hexdigest() != obj['sha256']:
            raise ValueError(f'Existing object differs; overwrite refused: {obj["name"]}')
        return 'already_present'


def main():
    from command_log import log_command
    log_command()
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-dir', required=True)
    parser.add_argument('--project', default=os.getenv('GOOGLE_CLOUD_PROJECT'))
    parser.add_argument('--bucket', default=os.getenv('GCS_BUCKET'))
    parser.add_argument('--upload', action='store_true', help='Write to an existing GCS bucket')
    parser.add_argument('--raw-only', action='store_true', help='Land raw XML before Spark transformation')
    parser.add_argument('--use-local-adc', action='store_true')
    args = parser.parse_args()
    status = 'failed'
    result = {}
    try:
        objects = build_plan(args.source_dir, raw_only=args.raw_only)
        result = {'mode': 'upload' if args.upload else 'dry_run', 'project': args.project,
                  'bucket': args.bucket, 'object_count': len(objects),
                  'total_bytes': sum(o['bytes'] for o in objects),
                  'objects': [{k: v for k, v in o.items() if k != 'content'} for o in objects]}
        if args.upload:
            if not args.project or not args.bucket or '/' in args.bucket:
                raise ValueError('Supply --project and --bucket (bucket name without gs://)')
            from gcp_auth import storage_client
            client = storage_client(args.project, args.use_local_adc)
            bucket = client.bucket(args.bucket)
            result['results'] = []
            for obj in objects:
                outcome = upload_one(bucket, obj)
                result['results'].append({'name': obj['name'], 'status': outcome})
            status = 'uploaded'
        else:
            status = 'dry_run_passed'
    except Exception as exc:
        result['error'] = f'{type(exc).__name__}: {exc}'
    result['status'] = status
    log_dir = ROOT / 'logs'
    log_dir.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    output = log_dir / f'gcs_batch_{stamp}.json'
    output.write_bytes(encoded(result))
    with open_progress() as file:
        file.write(f'\n[{stamp}] Batch JISDOR GCS: {status}\n'
                   f'- Objek direncanakan: {result.get("object_count", 0)}\n'
                   f'- Bukti: logs/{output.name}\n')
        if 'error' in result:
            file.write(f'- Kendala: {result["error"]}\n')
    print(json.dumps({k: v for k, v in result.items() if k not in ['objects', 'results']}, indent=2))
    return int(status == 'failed')


if __name__ == '__main__':
    raise SystemExit(main())
