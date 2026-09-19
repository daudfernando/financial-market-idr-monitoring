"""Audit BI-published dates against BigQuery; expose calendar gaps without inventing holidays."""
import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
import re
from google.cloud import bigquery
from gcp_auth import bigquery_client

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-run-id')
    parser.add_argument('--use-local-adc', action='store_true')
    args = parser.parse_args()
    client = bigquery_client('jcdeah-009','asia-southeast2',args.use_local_adc)
    cfg = bigquery.QueryJobConfig(maximum_bytes_billed=104857600)
    sql = '''CREATE OR REPLACE VIEW `jcdeah-009.daud_finalproject.jisdor_date_coverage` AS
      WITH bounds AS (SELECT MIN(jisdor_date) first_date
        FROM `jcdeah-009.daud_finalproject.fact_jisdor_daily`),
      dates AS (SELECT d FROM bounds, UNNEST(GENERATE_DATE_ARRAY(first_date,CURRENT_DATE('Asia/Jakarta'))) d),
      facts AS (SELECT jisdor_date,COUNT(*) row_count, MAX(usd_idr) usd_idr
        FROM `jcdeah-009.daud_finalproject.fact_jisdor_daily` WHERE currency='USD' GROUP BY 1)
      SELECT d AS calendar_date, COALESCE(row_count,0) row_count, usd_idr,
        CASE WHEN row_count>1 THEN 'duplicate'
             WHEN row_count=1 THEN 'available'
             WHEN d=CURRENT_DATE('Asia/Jakarta') THEN 'today_not_available_yet'
             WHEN EXTRACT(DAYOFWEEK FROM d) IN (1,7) THEN 'weekend_no_record'
             ELSE 'weekday_missing_unverified' END coverage_status
      FROM dates LEFT JOIN facts ON d=jisdor_date'''
    client.query(sql,job_config=cfg).result(timeout=120)
    coverage = [dict(r) for r in client.query(
        'SELECT * FROM `jcdeah-009.daud_finalproject.jisdor_date_coverage` ORDER BY calendar_date',job_config=cfg).result(timeout=120)]
    missing_published = []
    interval = None
    if args.source_run_id:
        if not re.fullmatch(r'\d{8}T\d{12}Z',args.source_run_id):
            raise ValueError('Invalid run ID')
        source_dir = ROOT / 'data/source_samples' / args.source_run_id
        source = json.loads((source_dir/'report.json').read_text())['jisdor']
        if source['status'] != 'passed':
            raise ValueError('Source run did not pass')
        published = {r['tgl_subkursasing'][:10] for r in json.loads((source_dir/'bi_records.json').read_text())}
        found = {str(r['calendar_date']) for r in coverage if r['row_count']>0}
        missing_published = sorted(published-found)
        interval = source.get('requested_interval')
    duplicates = [str(r['calendar_date']) for r in coverage if r['row_count']>1]
    report = {'status':'failed' if duplicates or missing_published else 'passed',
              'checked_at':datetime.now(timezone.utc).isoformat(), 'source_run_id':args.source_run_id,
              'source_interval_verified':interval, 'confirmed_missing_published_dates':missing_published,
              'duplicate_dates':duplicates,
              'weekday_gaps_to_review':[str(r['calendar_date']) for r in coverage if r['coverage_status']=='weekday_missing_unverified'],
              'note':'Weekday gap is not confirmed data loss: holiday/source non-publication is not verified. No rate imputation.',
              'calendar':coverage}
    folder = ROOT / 'data/jisdor_coverage'
    folder.mkdir(exist_ok=True)
    encoded = json.dumps(report,indent=2,default=str)
    (folder/'latest.json').write_text(encoded)
    if args.source_run_id:
        (folder/f'{args.source_run_id}.json').write_text(encoded)
    print(json.dumps({k:v for k,v in report.items() if k!='calendar'},indent=2))
    return int(report['status']!='passed')


if __name__=='__main__':
    raise SystemExit(main())
