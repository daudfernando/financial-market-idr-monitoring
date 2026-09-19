import hashlib
import json
import unittest
from unittest.mock import Mock
from scripts.load_jisdor_bigquery import validated_record, read_batch


class BatchValidationTest(unittest.TestCase):
    def setUp(self):
        self.record = {'jisdor_date': '2026-09-09', 'currency': 'USD', 'usd_idr': 17552,
                       'source': 'https://www.bi.go.id', 'ingested_at': '2026-09-09T21:00:00+00:00',
                       'fx_available_at': None}

    def test_valid_numeric_preserves_unknown_publication(self):
        row = validated_record(self.record, 'test_run')
        self.assertEqual(row['usd_idr'], '17552')
        self.assertIsNone(row['fx_available_at'])

    def test_invalid_rates_are_rejected(self):
        for rate in [0, -1, 'NaN', 'Infinity', '1e30']:
            with self.subTest(rate=rate), self.assertRaises(ValueError):
                validated_record(dict(self.record, usd_idr=rate), 'test_run')

    def test_naive_ingestion_time_is_rejected(self):
        with self.assertRaises(ValueError):
            validated_record(dict(self.record, ingested_at='2026-09-09T21:00:00'), 'run')

    def test_modified_cloud_object_is_rejected(self):
        run = '20260909T214210877646Z'
        name = f'final-project/processed/jisdor/date=2026-09-09/run_id={run}/record.json'
        content = json.dumps(self.record).encode()
        manifest = {'run_id': run, 'records': 1, 'source': self.record['source'], 'objects': [
            {'name': name, 'bytes': len(content), 'sha256': hashlib.sha256(content).hexdigest()}]}
        bucket = Mock()
        bucket.blob.return_value.download_as_bytes.side_effect = [json.dumps(manifest).encode(), b'changed']
        with self.assertRaisesRegex(ValueError, 'differs from manifest'):
            read_batch(bucket, run)


if __name__ == '__main__':
    unittest.main()
