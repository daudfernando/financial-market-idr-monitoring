"""Collect a common real historical minute window for JPM/BAC/GS/MS."""
from datetime import datetime, timedelta, timezone
import argparse
import json
import math
from pathlib import Path
import yfinance as yf
from market_refresh import activate

ROOT = Path(__file__).resolve().parents[1]
SYMBOLS = ['JPM', 'BAC', 'GS', 'MS']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--activate', action='store_true', help='Merge archive and switch replay to latest five sessions')
    args = parser.parse_args()
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    folder = ROOT / 'data/market_universe' / run_id
    folder.mkdir(parents=True)
    yf.set_tz_cache_location(str(ROOT / 'data/yfinance_cache'))
    report = {'run_id': run_id, 'status': 'collecting', 'symbols': {}, 'data_kind': 'real_historical'}
    collected = {}
    try:
        for symbol in SYMBOLS:
            ticker = yf.Ticker(symbol)
            frame = ticker.history(period='5d', interval='1m', auto_adjust=False, actions=True, timeout=30)
            frame.to_csv(folder / f'{symbol}_as_returned.csv')
            meta = ticker.get_history_metadata()
            metadata = {key: meta.get(key) for key in ['symbol', 'currency', 'exchangeTimezoneName', 'instrumentType']}
            (folder / f'{symbol}_metadata.json').write_text(json.dumps(metadata, indent=2), encoding='utf-8')
            if frame.empty or frame.index.tz is None or frame.index.has_duplicates or metadata['currency'] != 'USD':
                raise ValueError(f'{symbol}: empty data, timestamp issue, or unverified USD currency')
            records = []
            for ts, row in frame.iterrows():
                # A minute close is usable only after its bar has finished.
                if ts.to_pydatetime().astimezone(timezone.utc) + timedelta(minutes=1) > datetime.now(timezone.utc):
                    continue
                price = float(row['Close'])
                if not math.isfinite(price) or price <= 0:
                    raise ValueError(f'{symbol}: invalid source price')
                records.append({'symbol': symbol, 'bar_start_utc': ts.tz_convert('UTC').isoformat(),
                                'price_usd': price, 'volume': int(row['Volume']), 'source': 'yfinance',
                                'source_interval': '1m', 'record_type': 'historical_bar', 'currency': 'USD'})
            collected[symbol] = records
            if not records:
                raise ValueError(f'{symbol}: no completed bars')
            report['symbols'][symbol] = {'downloaded': len(records), 'first': records[0]['bar_start_utc'],
                                          'last': records[-1]['bar_start_utc']}
        # Common boundary, not intersection: internal missing minutes remain detectable.
        first = max(rows[0]['bar_start_utc'] for rows in collected.values())
        last = min(rows[-1]['bar_start_utc'] for rows in collected.values())
        if first > last:
            raise ValueError('No overlapping source period')
        output = []
        for symbol, rows in collected.items():
            aligned = [row for row in rows if first <= row['bar_start_utc'] <= last]
            report['symbols'][symbol]['aligned'] = len(aligned)
            output.extend(aligned)
        output.sort(key=lambda row: (row['bar_start_utc'], row['symbol']))
        with (folder / 'replay_candidates.jsonl').open('w', encoding='utf-8') as file:
            for row in output:
                file.write(json.dumps(row, allow_nan=False) + '\n')
        report.update(status='passed', records=len(output), first=first, last=last)
        if args.activate:
            report['activation'] = activate(ROOT, folder, output)
    except Exception as exc:
        report.update(status='failed', error=f'{type(exc).__name__}: {exc}')
    (folder / 'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report, indent=2))
    return int(report['status'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())
