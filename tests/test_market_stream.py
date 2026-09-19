import base64
import copy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'streaming'))
from events import make_event, validate

PAYLOAD = {'symbol': 'JPM', 'bar_start_utc': '2026-09-02T13:30:00+00:00',
           'price_usd': 358.28, 'currency': 'USD', 'source': 'yfinance',
           'record_type': 'historical_bar', 'source_interval': '1m'}
INGESTED = '2026-09-16T12:00:00+00:00'


class MarketContractTest(unittest.TestCase):
    def test_replay_preserves_source_time_and_stable_identity(self):
        first = make_event(PAYLOAD, 'replay', INGESTED)
        second = make_event(PAYLOAD, 'replay', '2026-09-16T12:01:00+00:00')
        self.assertEqual(first['event_timestamp'], PAYLOAD['bar_start_utc'])
        self.assertEqual(first['event_id'], second['event_id'])
        self.assertNotEqual(first['ingested_at'], second['ingested_at'])
        self.assertEqual(json.loads(first['raw_payload']), PAYLOAD)

    def test_invalid_prices_and_currency_rejected(self):
        for field, value in [('price_usd', float('nan')), ('price_usd', 0),
                             ('price_usd', -1), ('price_usd', True), ('currency', 'EUR')]:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                make_event({**PAYLOAD, field: value}, 'replay', INGESTED)

    def test_naive_and_future_timestamp_rejected(self):
        for value in ['2026-09-02T13:30:00', '2027-01-01T00:00:00+00:00']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                make_event({**PAYLOAD, 'bar_start_utc': value}, 'replay', INGESTED)

    def test_modified_identity_rejected(self):
        event = make_event(PAYLOAD, 'replay', INGESTED)
        event['price_usd'] += 1
        with self.assertRaises(ValueError):
            validate(event)

    def test_live_requires_verified_currency_and_millisecond_time(self):
        event = make_event({'id': 'JPM', 'price': 358.28, 'currency': 'USD',
                            'time': 1788960600000}, 'live_websocket', INGESTED)
        self.assertEqual(event['record_type'], 'price_update')
        self.assertIsNone(event['replayed_at'])
        with self.assertRaises(ValueError):
            make_event({'id': 'JPM', 'price': 358.28, 'time': 1788960600000}, 'live_websocket', INGESTED)


try:
    import apache_beam as beam
except ImportError:
    beam = None


@unittest.skipIf(beam is None, 'Beam integration test runs in market Docker image')
class BeamMarketTest(unittest.TestCase):
    def test_valid_and_malformed_records_accounted_for(self):
        from beam_pipeline import validated
        from apache_beam.testing.util import assert_that, equal_to
        event = make_event(PAYLOAD, 'replay', INGESTED)
        def wrap(value, offset):
            return {'value_base64': base64.b64encode(value).decode(), 'kafka_topic': 'test',
                    'kafka_partition': 0, 'kafka_offset': offset}
        records = [wrap(json.dumps(event).encode(), 0), wrap(b'not json', 1), wrap(b'\xff', 2)]
        with beam.Pipeline() as pipeline:
            output = validated(pipeline | beam.Create(records))
            assert_that(output.valid | beam.Map(lambda row: (row['event_id'], row['kafka_offset'])),
                        equal_to([(event['event_id'], 0)]), label='Valid row')
            assert_that(output.invalid | beam.Map(lambda row: row['kafka_offset']),
                        equal_to([1, 2]), label='Deadletter rows')


if __name__ == '__main__':
    unittest.main()
