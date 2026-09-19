"""Shared event contract for producer, Beam, and tests. No cloud dependencies."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math

SYMBOLS = {'JPM', 'BAC', 'GS', 'MS'}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None or parsed.year < 2000:
        raise ValueError('Timestamp must include timezone and be >= year 2000')
    return parsed.astimezone(timezone.utc)


def event_id(event):
    # Stable for identical source observations across replay runs; not a trade ID.
    identity = {k: event[k] for k in ('symbol', 'event_timestamp', 'price_usd', 'currency',
                                     'source', 'record_type', 'source_interval', 'ingestion_mode')}
    return hashlib.sha256(json.dumps(identity, sort_keys=True, allow_nan=False).encode()).hexdigest()


def validate(event):
    if event['schema_version'] != 1 or event['symbol'] not in SYMBOLS or event['currency'] != 'USD':
        raise ValueError('Unsupported schema, symbol, or currency')
    price = event['price_usd']
    if isinstance(price, bool) or not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0:
        raise ValueError('Price must be a finite positive number')
    event_time = timestamp(event['event_timestamp'])
    received = timestamp(event['ingested_at'])
    if event_time > received + timedelta(minutes=5):
        raise ValueError('Source event is in the future relative to ingestion')
    if event['event_date'] != event_time.date().isoformat() or event['source'] != 'yfinance':
        raise ValueError('Invalid event date or source')
    if event['ingestion_mode'] == 'replay':
        if event['record_type'] != 'historical_bar' or event['source_interval'] != '1m':
            raise ValueError('Replay must identify historical one-minute bars')
        if timestamp(event['replayed_at']) != received:
            raise ValueError('Replay timestamp must equal ingestion timestamp')
    elif event['ingestion_mode'] == 'live_websocket':
        if event['record_type'] != 'price_update' or event['source_interval'] != 'event' or event['replayed_at'] is not None:
            raise ValueError('Invalid live event metadata')
    else:
        raise ValueError('Unsupported ingestion mode')
    if event['event_id'] != event_id(event):
        raise ValueError('Event identity mismatch')
    # Require payload to remain parseable for auditing.
    if not isinstance(json.loads(event['raw_payload']), dict):
        raise ValueError('Raw payload must be an object')
    return event


def make_event(payload, mode, ingested_at=None):
    received = ingested_at or utc_now()
    if mode == 'replay':
        if payload.get('record_type') != 'historical_bar' or payload.get('source_interval') != '1m' or payload.get('source') != 'yfinance':
            raise ValueError('Input is not a verified yfinance replay candidate')
        symbol, currency = payload['symbol'], payload['currency']
        event_time = timestamp(payload['bar_start_utc'])
        price, kind, interval = payload['price_usd'], 'historical_bar', '1m'
    elif mode == 'live_websocket':
        symbol, currency = payload['id'], payload.get('currency')
        event_time = datetime.fromtimestamp(int(payload['time']) / 1000, timezone.utc)
        price, kind, interval = payload['price'], 'price_update', 'event'
    else:
        raise ValueError('Unsupported ingestion mode')
    event = {'schema_version': 1, 'symbol': symbol, 'currency': currency,
             'event_timestamp': event_time.isoformat(), 'event_date': event_time.date().isoformat(),
             'price_usd': price, 'source': 'yfinance', 'record_type': kind,
             'source_interval': interval, 'ingestion_mode': mode, 'ingested_at': received,
             'replayed_at': received if mode == 'replay' else None,
             'raw_payload': json.dumps(payload, sort_keys=True, allow_nan=False)}
    event['event_id'] = event_id(event)
    return validate(event)
