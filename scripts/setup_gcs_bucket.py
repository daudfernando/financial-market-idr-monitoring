"""Create the explicitly selected project bucket, with private access defaults."""
try:
    from .command_log import open_progress
except ImportError:
    from command_log import open_progress
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re

from gcp_auth import storage_client

ROOT = Path(__file__).resolve().parents[1]


def main():
    from command_log import log_command
    log_command()
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--location', default='asia-southeast2')
    parser.add_argument('--use-local-adc', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{1,61}[a-z0-9]', args.bucket):
        parser.error('Use 3-63 lowercase letters, digits, or hyphens for this project')
    result = {'project': args.project, 'bucket': args.bucket, 'location': args.location,
              'status': 'failed'}
    try:
        client = storage_client(args.project, args.use_local_adc)
        # Listing in the selected project prevents silently reusing another project's bucket.
        found = [b for b in client.list_buckets(project=args.project, prefix=args.bucket,
                                               timeout=20, retry=None) if b.name == args.bucket]
        if found:
            bucket = found[0]
            bucket.reload(timeout=20, retry=None)
            if bucket.location.lower() != args.location.lower():
                raise ValueError('Existing bucket location differs from requested location')
            if not bucket.iam_configuration.uniform_bucket_level_access_enabled:
                raise ValueError('Existing bucket does not use uniform access; no settings changed')
            if bucket.iam_configuration.public_access_prevention != 'enforced':
                raise ValueError('Existing bucket public access prevention is not enforced')
            result['status'] = 'already_exists_in_project'
        else:
            bucket = client.bucket(args.bucket)
            bucket.storage_class = 'STANDARD'
            bucket.iam_configuration.uniform_bucket_level_access_enabled = True
            bucket.iam_configuration.public_access_prevention = 'enforced'
            bucket.labels = {'purpose': 'data-engineering-final-project', 'owner': 'daud'}
            bucket = client.create_bucket(bucket, location=args.location, timeout=30, retry=None)
            result['status'] = 'created'
        result.update(location=bucket.location, project_number=bucket.project_number,
                      storage_class=bucket.storage_class,
                      uniform_access=bucket.iam_configuration.uniform_bucket_level_access_enabled,
                      public_access_prevention=bucket.iam_configuration.public_access_prevention)
    except Exception as exc:
        result['error'] = f'{type(exc).__name__}: {exc}'
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    logdir = ROOT / 'logs'
    logdir.mkdir(exist_ok=True)
    output = logdir / f'gcs_setup_{stamp}.json'
    output.write_text(json.dumps(result, indent=2), encoding='utf-8')
    with open_progress() as file:
        file.write(f'\n[{stamp}] Setup GCS: {result["status"]}\n'
                   f'- Project: {args.project}; bucket: {args.bucket}\n'
                   f'- Bukti: logs/{output.name}\n')
        if result.get('error'):
            file.write(f'- Kendala: {result["error"]}\n')
    print(json.dumps(result, indent=2))
    return int(result['status'] == 'failed')


if __name__ == '__main__':
    raise SystemExit(main())
