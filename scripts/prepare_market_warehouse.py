"""Prepare market staging and deduplicated view in the existing project dataset."""
import argparse
import json
from pathlib import Path

from gcp_auth import bigquery_client

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--use-local-adc', action='store_true')
    args = parser.parse_args()
    sql = (ROOT / 'sql' / 'market_staging.sql').read_text(encoding='utf-8')
    if not args.apply:
        print(sql)
        return
    client = bigquery_client('jcdeah-009', 'asia-southeast2', args.use_local_adc)
    dataset = client.get_dataset('jcdeah-009.daud_finalproject')
    if dataset.location.lower() != 'asia-southeast2':
        raise ValueError('Existing dataset location differs from the project configuration')
    job = client.query(sql, location='asia-southeast2')
    job.result(timeout=120)
    table = client.get_table('jcdeah-009.daud_finalproject.stg_market_events')
    print(json.dumps({'status': 'passed', 'job_id': job.job_id,
                      'table': table.full_table_id, 'rows': table.num_rows,
                      'partition_field': table.time_partitioning.field,
                      'view': 'jcdeah-009.daud_finalproject.market_events_deduplicated'}))


if __name__ == '__main__':
    main()
