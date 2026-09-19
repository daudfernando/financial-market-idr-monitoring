"""Read-only BigQuery readiness audit; writes a technical JSON report, no progress TXT."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from google.cloud import bigquery
from gcp_auth import bigquery_client


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--use-local-adc', action='store_true')
    args = parser.parse_args()
    client = bigquery_client('jcdeah-009', 'asia-southeast2', args.use_local_adc)
    queries = {
        'batch': '''SELECT COUNT(*) row_count, COUNT(DISTINCT jisdor_date) dates,
          MIN(jisdor_date) first_date, MAX(jisdor_date) last_date,
          COUNTIF(usd_idr IS NULL OR usd_idr <= 0 OR currency != 'USD') invalid
          FROM `jcdeah-009.daud_finalproject.fact_jisdor_daily`''',
        'history': '''SELECT COUNT(*) row_count, COUNT(DISTINCT event_id) unique_events,
          COUNT(DISTINCT symbol) symbols, COUNTIF(usd_idr IS NULL) missing_fx,
          COUNTIF(data_kind != 'real_historical_replay') wrong_mode,
          MIN(minute_utc) first_minute, MAX(minute_utc) last_minute
          FROM `jcdeah-009.daud_finalproject_demo.mart_stock_monitoring`''',
        'latest': '''SELECT symbol, minute_utc, strategy_signal, comparison_complete,
          aligned_with_latest FROM `jcdeah-009.daud_finalproject_demo.mart_stock_latest` ORDER BY symbol''',
        'signals': '''SELECT strategy_signal, COUNT(*) row_count
          FROM `jcdeah-009.daud_finalproject_demo.mart_stock_monitoring` GROUP BY 1 ORDER BY 1''',
        'scenarios': '''SELECT COUNT(*) row_count, COUNTIF(strategy_signal != expected_signal) mismatches
          FROM `jcdeah-009.daud_finalproject_demo.demo_signal_scenarios`''',
    }
    results = {name: [dict(row) for row in client.query(sql,
        job_config=bigquery.QueryJobConfig(maximum_bytes_billed=100*1024*1024)).result(timeout=120)]
        for name, sql in queries.items()}
    batch, history = results['batch'][0], results['history'][0]
    checks = {
        'batch_nonempty_unique_valid': batch['row_count'] > 0 and batch['row_count'] == batch['dates'] and batch['invalid'] == 0,
        'history_nonempty_unique': history['row_count'] > 0 and history['row_count'] == history['unique_events'],
        'four_stocks_with_fx': history['symbols'] == 4 and history['missing_fx'] == 0,
        'replay_labelled': history['wrong_mode'] == 0,
        'latest_four_aligned': len(results['latest']) == 4 and all(r['aligned_with_latest'] and r['comparison_complete'] for r in results['latest']),
        'synthetic_scenarios_match': results['scenarios'][0] == {'row_count': 5, 'mismatches': 0},
    }
    report = {'status': 'passed' if all(checks.values()) else 'failed',
              'checked_at': datetime.now(timezone.utc).isoformat(), 'checks': checks, 'results': results}
    path = Path(__file__).resolve().parents[1] / 'data/dashboard_readiness.json'
    path.write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')
    print(json.dumps(report, indent=2, default=str))
    return int(report['status'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())
