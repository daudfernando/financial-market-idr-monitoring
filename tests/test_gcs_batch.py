"""Offline checks for safe cloud writes. No network or cloud credentials needed."""
import unittest
from unittest.mock import Mock

from google.api_core.exceptions import Forbidden, PreconditionFailed
from scripts.upload_jisdor_gcs import item, upload_one


class UploadSafetyTest(unittest.TestCase):
    def setUp(self):
        self.bucket = Mock()
        self.blob = self.bucket.blob.return_value
        self.obj = item('test/record.json', b'{"value":1}', 'application/json')

    def test_new_object_requires_absence(self):
        self.assertEqual(upload_one(self.bucket, self.obj), 'uploaded')
        self.assertEqual(self.blob.upload_from_string.call_args.kwargs['if_generation_match'], 0)

    def test_identical_rerun_skips(self):
        self.blob.upload_from_string.side_effect = PreconditionFailed('exists')
        self.blob.generation = 42
        self.blob.download_as_bytes.return_value = self.obj['content']
        self.assertEqual(upload_one(self.bucket, self.obj), 'already_present')
        self.assertEqual(self.blob.download_as_bytes.call_args.kwargs['if_generation_match'], 42)

    def test_conflicting_object_is_not_overwritten(self):
        self.blob.upload_from_string.side_effect = PreconditionFailed('exists')
        self.blob.download_as_bytes.return_value = b'different data'
        with self.assertRaisesRegex(ValueError, 'overwrite refused'):
            upload_one(self.bucket, self.obj)
        self.assertEqual(self.blob.upload_from_string.call_count, 1)

    def test_permission_failure_propagates(self):
        self.blob.upload_from_string.side_effect = Forbidden('denied')
        with self.assertRaises(Forbidden):
            upload_one(self.bucket, self.obj)


if __name__ == '__main__':
    unittest.main()
