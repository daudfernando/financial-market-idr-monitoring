"""Collect real historical samples and diagnose BI using a browser-compatible TLS client."""
try:
    from .command_log import open_progress
except ImportError:
    from command_log import open_progress
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import logging
import math
from pathlib import Path
import xml.etree.ElementTree as ET

from curl_cffi import requests
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]


def save(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')


from jisdor_source import collect_bi


def collect_history(folder):
    result = {'source': 'yfinance.Ticker.history', 'symbol': 'JPM',
              'mode': 'historical_download', 'status': 'failed'}
    try:
        yf.set_tz_cache_location(str(ROOT / 'data' / 'yfinance_cache'))
        ticker = yf.Ticker('JPM')
        yf.config.debug.hide_exceptions = False
        history = ticker.history(period='5d', interval='1m', auto_adjust=False,
                                 actions=True, timeout=20)
        history.to_csv(folder / 'jpm_history_as_returned.csv')
        source_metadata = ticker.get_history_metadata()
        metadata = {key: source_metadata.get(key) for key in
                    ['symbol', 'currency', 'exchangeTimezoneName', 'exchangeName', 'instrumentType']}
        save(folder / 'jpm_metadata.json', metadata)
        if history.empty:
            raise ValueError('Historical response contains no bars')
        if metadata.get('currency') != 'USD':
            raise ValueError('USD currency not confirmed')
        if history.index.tz is None:
            raise ValueError('Historical timestamps have no timezone')
        if history.index.has_duplicates:
            raise ValueError('Duplicate historical timestamps')
        records = []
        for timestamp, row in history.iterrows():
            price = float(row['Close'])
            if not math.isfinite(price) or price <= 0:
                raise ValueError(f'Invalid close price at {timestamp}')
            records.append({'symbol': 'JPM', 'bar_start_utc': timestamp.tz_convert('UTC').isoformat(),
                            'price_usd': price, 'volume': int(row['Volume']),
                            'source': 'yfinance', 'source_interval': '1m',
                            'record_type': 'historical_bar', 'currency': 'USD'})
        with (folder / 'jpm_replay_candidates.jsonl').open('w', encoding='utf-8') as file:
            for record in records:
                file.write(json.dumps(record) + '\n')
        result.update(status='passed', records=len(records), currency=metadata['currency'],
                      exchange_timezone=metadata.get('exchangeTimezoneName'),
                      first_bar=records[0]['bar_start_utc'], last_bar=records[-1]['bar_start_utc'],
                      columns=list(history.columns))
    except Exception as exc:
        result['error'] = f'{type(exc).__name__}: {exc}'
        logging.exception('Historical sample failed')
    return result


def main():
    from command_log import log_command
    log_command()
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    folder = ROOT / 'data' / 'source_samples' / run_id
    folder.mkdir(parents=True)
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(),
                        logging.FileHandler(folder / 'execution.log', encoding='utf-8')])
    with ThreadPoolExecutor(max_workers=2) as pool:
        batch = pool.submit(collect_bi, folder)
        market = pool.submit(collect_history, folder)
        report = {'run_id': run_id, 'jisdor': batch.result(), 'market_history': market.result()}
    save(folder / 'report.json', report)
    with open_progress() as file:
        file.write(f'\n[{run_id}] Pengambilan sampel nyata dan diagnosis akses\n'
                   f'- BI browser-compatible TLS: {report["jisdor"]["status"]}\n'
                   f'- JPM historical 1m: {report["market_history"]["status"]}; '
                   f'records={report["market_history"].get("records", 0)}\n'
                   f'- Bukti: {folder.relative_to(ROOT).as_posix()}/report.json\n'
                   '- Historical download belum merupakan streaming/replay melalui Pub/Sub.\n')
    print(json.dumps(report, indent=2))
    return 0 if report['jisdor']['status'] == report['market_history']['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
