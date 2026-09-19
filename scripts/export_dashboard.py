"""Export a reproducible dashboard snapshot from the tested BigQuery demo mart."""
import argparse
from datetime import date, datetime, timezone
from decimal import Decimal
import csv
import json
from pathlib import Path

from gcp_auth import bigquery_client

ROOT = Path(__file__).resolve().parents[1]


def scalar(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(type(value).__name__)


def main():
    from google.cloud import bigquery
    parser = argparse.ArgumentParser()
    parser.add_argument('--use-local-adc', action='store_true')
    args = parser.parse_args()
    client = bigquery_client('jcdeah-009', 'asia-southeast2', args.use_local_adc)
    query = '''SELECT * FROM `jcdeah-009.daud_finalproject_demo.mart_stock_monitoring`
               ORDER BY symbol, ingestion_mode, minute_utc LIMIT 10001'''
    rows = [dict(row) for row in client.query(query,
        job_config=bigquery.QueryJobConfig(maximum_bytes_billed=100*1024*1024)).result(timeout=120)]
    if not rows or len(rows) > 10000:
        raise ValueError('Dashboard requires 1..10000 rows; empty/oversize snapshot refused')
    if any(row['ingestion_mode'] != 'replay' for row in rows):
        raise ValueError('Demo snapshot must contain only explicitly labelled replay data')
    data = {'exported_at': datetime.now(timezone.utc).isoformat(), 'rows': rows,
            'source': 'jcdeah-009.daud_finalproject_demo.mart_stock_monitoring',
            'mode': 'REPLAY HISTORIS ASLI — LOCAL KAFKA / REPLAY', 'cloud_dataflow_executed': False}
    output = ROOT / 'dashboard'
    output.mkdir(exist_ok=True)
    encoded = json.dumps(data, default=scalar, allow_nan=False, ensure_ascii=False)
    (output / 'data.js').write_text('window.PROJECT_DATA = ' + encoded.replace('<', '\\u003c') + ';\n', encoding='utf-8')
    with (output / 'market_risk.csv').open('w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps({'status': 'passed', 'rows': len(rows), 'output': str(output / 'index.html'),
                      'missing_fx': sum(row['usd_idr'] is None for row in rows),
                      'review_movements': sum(row['risk_indicator'] == 'review_movement' for row in rows)}))


if __name__ == '__main__':
    main()
