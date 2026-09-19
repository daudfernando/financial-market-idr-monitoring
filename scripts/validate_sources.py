"""Bounded source exploration. Saves evidence; does not provision cloud resources."""
try:
    from .command_log import open_progress
except ImportError:
    from command_log import open_progress
import argparse
import asyncio
from datetime import datetime, timezone
import importlib.metadata
import json
import logging
import math
from pathlib import Path
import time
import xml.etree.ElementTree as ET

from curl_cffi import requests
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
BI_URL = 'https://www.bi.go.id/biwebservice/wskursbi.asmx/getSubKursJisdor1'


def now():
    return datetime.now(timezone.utc).isoformat()


def save_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding='utf-8')


def probe_jisdor(folder):
    result = {'source': BI_URL, 'started_at': now(), 'status': 'failed'}
    for attempt in range(1, 3):
        try:
            method = 'GET' if attempt == 1 else 'POST'
            response = requests.request(method, BI_URL, timeout=(10, 20),
                                        impersonate='chrome',
                                        headers={'User-Agent': 'Mozilla/5.0',
                                                 'Connection': 'close'})
            # Retain even unsuccessful HTTP bodies for diagnosis.
            (folder / f'jisdor_response_{attempt}.xml').write_bytes(response.content)
            result.update(method=method, http_status=response.status_code,
                          content_type=response.headers.get('Content-Type'))
            response.raise_for_status()
            root = ET.fromstring(response.content)
            rows = [{child.tag.split('}')[-1]: child.text for child in node}
                    for node in root.iter() if node.tag.split('}')[-1] == 'Table']
            save_json(folder / 'jisdor_records.json', rows)
            if not rows:
                raise ValueError('XML contains no Table records; inspect actual schema.')
            required = {'tgl_subkursasing', 'jual_subkursasing', 'mts_subkursasing'}
            invalid = []
            for index, row in enumerate(rows):
                try:
                    if not required.issubset(row):
                        raise ValueError('Expected columns missing')
                    datetime.fromisoformat(row['tgl_subkursasing'])
                    rate = float(row['jual_subkursasing'])
                    if not math.isfinite(rate) or rate <= 0:
                        raise ValueError('Invalid exchange rate')
                    if row['mts_subkursasing'].strip().upper() != 'USD':
                        raise ValueError('Currency is not USD')
                except (ValueError, TypeError) as exc:
                    invalid.append({'row': index, 'error': str(exc)})
            result.update(status='passed' if not invalid else 'quality_failed',
                          records=len(rows), columns=sorted(rows[0]), invalid=invalid,
                          sample=rows[0])
            result.pop('error', None)
            break
        except Exception as exc:
            result['error'] = f'{type(exc).__name__}: {exc}'
            logging.warning('JISDOR attempt %s: %s', attempt, result['error'])
            if attempt < 2:
                time.sleep(2)
    result['finished_at'] = now()
    return result


async def probe_market(folder, symbol, seconds):
    result = {'source': 'yfinance', 'symbol': symbol, 'mode': 'live_websocket',
              'started_at': now(), 'status': 'failed', 'connected': False}
    events = []
    def handler(message):
        envelope = {'ingested_at': now(), 'ingestion_mode': 'live_websocket',
                    'payload': message}
        with (folder / 'market_events.jsonl').open('a', encoding='utf-8') as file:
            file.write(json.dumps(envelope) + '\n')
        events.append(message)

    async def collect():
        async with yf.AsyncWebSocket(verbose=False) as ws:
            await ws.subscribe([symbol])
            result['connected'] = True
            await ws.listen(handler)

    try:
        await asyncio.wait_for(collect(), timeout=seconds)
    except TimeoutError:
        result['observation_window_ended'] = True
    except Exception as exc:
        result['error'] = f'{type(exc).__name__}: {exc}'
    invalid = []
    for index, event in enumerate(events):
        try:
            price = float(event['price'])
            if event.get('id') != symbol or not math.isfinite(price) or price <= 0:
                raise ValueError('Invalid symbol or price')
            # yfinance live price timestamp is milliseconds; reject implausible units.
            ts = datetime.fromtimestamp(int(event['time']) / 1000, timezone.utc)
            if ts.year < 2000 or ts > datetime.now(timezone.utc):
                raise ValueError('Invalid event timestamp')
        except (KeyError, ValueError, TypeError, OverflowError, OSError) as exc:
            invalid.append({'row': index, 'error': str(exc)})
    result.update(events=len(events), invalid=invalid, finished_at=now())
    if events:
        result.update(status='passed' if not invalid and 'error' not in result else 'quality_failed',
                      columns=sorted(events[0]), sample=events[0])
    elif 'error' not in result:
        result['status'] = 'no_events' if result['connected'] else 'connection_timeout'
    return result


async def main():
    from command_log import log_command
    log_command()
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', choices=['JPM', 'BAC', 'GS', 'MS'], default='JPM')
    parser.add_argument('--seconds', type=int, default=45)
    args = parser.parse_args()
    if not 5 <= args.seconds <= 300:
        parser.error('--seconds must be between 5 and 300')
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    folder = ROOT / 'data' / 'source_validation' / run_id
    folder.mkdir(parents=True)
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s',
                        handlers=[logging.StreamHandler(),
                                  logging.FileHandler(folder / 'execution.log', encoding='utf-8')])
    logging.info('Run %s, output %s', run_id, folder)
    batch, market = await asyncio.gather(
        asyncio.to_thread(probe_jisdor, folder), probe_market(folder, args.symbol, args.seconds))
    report = {'run_id': run_id, 'versions': {name: importlib.metadata.version(name)
              for name in ['yfinance', 'requests']}, 'jisdor': batch, 'market': market}
    save_json(folder / 'report.json', report)
    print(json.dumps(report, indent=2))
    log = ROOT / 'logs' / 'progress.txt'
    log.parent.mkdir(exist_ok=True)
    with open_progress() as file:
        file.write(f'\n[{now()}] Validasi sumber | run {run_id}\n'
                   f'- JISDOR: {batch["status"]}; records={batch.get("records", 0)}\n'
                   f'- {args.symbol} WebSocket: {market["status"]}; events={market["events"]}\n'
                   f'- Bukti: {folder.relative_to(ROOT).as_posix()}/report.json\n')
        for name, item in [('JISDOR', batch), ('Market', market)]:
            if item.get('error'):
                file.write(f'- Kendala {name}: {item["error"]}\n')
    return 0 if batch['status'] == market['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
