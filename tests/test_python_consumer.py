import base64
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'streaming'))
from events import make_event
from python_consumer import decode_record


class ConsumerValidationTest(unittest.TestCase):
    def record(self):
        event = make_event(dict(symbol='JPM', currency='USD', record_type='historical_bar',
            source_interval='1m', source='yfinance', price_usd=100,
            bar_start_utc='2026-09-10T13:30:00+00:00'), 'replay', '2026-09-17T12:00:00+00:00')
        return dict(value_base64=base64.b64encode(json.dumps(event).encode()).decode(),
                    kafka_topic='test', kafka_partition=0, kafka_offset=12)

    def test_preserves_source_time_and_kafka_coordinate(self):
        event = decode_record(self.record())
        self.assertEqual(event['event_timestamp'], '2026-09-10T13:30:00+00:00')
        self.assertEqual(event['kafka_offset'], 12)
        self.assertEqual(event['ingestion_mode'], 'replay')

    def test_rejects_corrupted_payload(self):
        record = self.record()
        record['value_base64'] = 'invalid!'
        with self.assertRaises(ValueError):
            decode_record(record)

    def test_rejects_mutation_with_unchanged_identity(self):
        record = self.record()
        event = json.loads(base64.b64decode(record['value_base64']))
        event['price_usd'] = 200
        record['value_base64'] = base64.b64encode(json.dumps(event).encode()).decode()
        with self.assertRaises(ValueError):
            decode_record(record)
