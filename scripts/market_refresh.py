"""Merge real bars into an archive and activate a bounded latest-session replay."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from zoneinfo import ZoneInfo


def key(row):
    return row['symbol'], row['bar_start_utc']


def merge_bars(existing, incoming):
    merged = {key(r): r for r in existing}
    if len(merged) != len(existing):
        raise ValueError('Archive contains duplicate symbol/timestamp keys')
    seen, added, revised = set(), 0, 0
    for row in incoming:
        k = key(row)
        if k in seen:
            raise ValueError('Incoming duplicate symbol/timestamp')
        seen.add(k)
        added += int(k not in merged)
        revised += int(k in merged and merged[k] != row)
        merged[k] = row
    return sorted(merged.values(), key=lambda r: (r['bar_start_utc'], r['symbol'])), added, revised


def latest_sessions(rows, count=5):
    def session(r):
        return datetime.fromisoformat(r['bar_start_utc']).astimezone(ZoneInfo('America/New_York')).date()
    dates = sorted({session(r) for r in rows})[-count:]
    output = [r for r in rows if session(r) in dates]
    if not output or len(output) > 10000:
        raise ValueError('Latest five sessions must contain 1..10000 bars')
    if {r['symbol'] for r in output} != {'JPM', 'BAC', 'GS', 'MS'}:
        raise ValueError('Active window requires all four stocks')
    return output


def activate(root, folder, rows):
    config_path = root / 'config/current_market_replay.json'
    archive = root / 'data/market_universe/replay_candidates.jsonl'
    lock = root / 'data/market_universe/refresh.lock'
    with lock.open('x'):
        pass
    try:
        previous = json.loads(config_path.read_text(encoding='utf-8')) if config_path.exists() else {}
        seed = archive if archive.exists() else root / previous.get('input', 'data/market_universe/replay_candidates.jsonl')
        existing = [json.loads(s) for s in seed.read_text(encoding='utf-8').splitlines() if s] if seed.exists() else []
        if existing and max(r['bar_start_utc'] for r in rows) < max(r['bar_start_utc'] for r in existing):
            raise ValueError('Source is older than archive; active replay not replaced')
        merged, added, revised = merge_bars(existing, rows)
        active = latest_sessions(merged)
        active_path = folder / 'active_replay_candidates.jsonl'
        encode = lambda data: ''.join(json.dumps(r, allow_nan=False)+'\n' for r in data)
        active_path.write_text(encode(active), encoding='utf-8')
        temp = archive.with_suffix('.tmp')
        temp.write_text(encode(merged), encoding='utf-8')
        temp.replace(archive)
        config = {'source_run_id': folder.name, 'input':active_path.relative_to(root).as_posix(),
                  'archive':archive.relative_to(root).as_posix(), 'records':len(active),
                  'symbols':['JPM','BAC','GS','MS'], 'data_kind':'real_historical',
                  'refreshed_at':datetime.now(timezone.utc).isoformat(),
                  'first':active[0]['bar_start_utc'], 'last':active[-1]['bar_start_utc'],
                  'window':'latest_5_available_sessions', 'archive_records':len(merged)}
        temp_config = config_path.with_suffix('.tmp')
        temp_config.write_text(json.dumps(config,indent=2),encoding='utf-8')
        temp_config.replace(config_path)
        return {'added':added, 'revised':revised, 'archive_records':len(merged),
                'active_records':len(active), 'first':config['first'], 'last':config['last']}
    finally:
        lock.unlink(missing_ok=True)
