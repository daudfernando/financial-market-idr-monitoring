import base64
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from verify_market_cloud import reconcile
from events import make_event


class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        event = make_event({'symbol': 'JPM', 'bar_start_utc': '2026-09-02T13:30:00+00:00',
                            'price_usd': 358.28, 'currency': 'USD', 'source': 'yfinance',
                            'record_type': 'historical_bar', 'source_interval': '1m'},
                           'replay', '2026-09-16T12:00:00+00:00')
        key = {'kafka_topic': 'replay', 'kafka_partition': 0, 'kafka_offset': 1}
        self.raw = {**key, 'value_base64': base64.b64encode(json.dumps(event).encode()).decode()}
        self.row = {**key, **event}

    def test_empty_is_not_success(self):
        self.assertEqual(reconcile([], [])['status'], 'failed')

    def test_missing_record_fails(self):
        result = reconcile([self.raw], [])
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(len(result['missing']), 1)

    def test_mutated_price_fails_even_when_identity_matches(self):
        result = reconcile([self.raw], [{**self.row, 'price_usd': 1}])
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['mismatches'][0]['field'], 'price_usd')

    def test_duplicate_transport_is_counted_without_losing_identity(self):
        result = reconcile([self.raw, self.raw], [self.row, self.row])
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(result['duplicate_warehouse_rows'], 1)
        self.assertEqual(result['raw_coordinates'], 1)
        self.assertEqual(result['unique_event_ids'], 1)


if __name__ == '__main__':
    unittest.main()
