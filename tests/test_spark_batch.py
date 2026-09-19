"""Run inside the Airflow image, which includes Java and PySpark."""
from pathlib import Path
import tempfile
import unittest

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None


@unittest.skipIf(SparkSession is None, 'Run in Airflow Docker image for Spark integration tests')
class SparkBatchTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from transform_jisdor_spark import transform
        cls.transform = staticmethod(transform)
        cls.spark = (SparkSession.builder.master('local[2]').appName('jisdor-tests')
                     .config('spark.ui.enabled', 'false')
                     .config('spark.sql.session.timeZone', 'Asia/Jakarta').getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def run_rows(self, values):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'input.xml'
            path.write_text('<NewDataSet>' + ''.join(
                f'<Table><tgl_subkursasing>{dt}</tgl_subkursasing>'
                f'<mts_subkursasing>{currency}</mts_subkursasing>'
                f'<jual_subkursasing>{rate}</jual_subkursasing></Table>'
                for dt, currency, rate in values) + '</NewDataSet>')
            return self.transform(self.spark, path)

    def test_normalization_and_jakarta_date(self):
        rows = self.run_rows([('2026-09-15T17:00:00Z', ' usd ', '16000.25')])
        self.assertEqual(str(rows[0].jisdor_date), '2026-09-16')
        self.assertEqual(str(rows[0].usd_idr), '16000.250000000')
        self.assertEqual(rows[0].currency, 'USD')

    def test_bad_rows_and_empty_batch_rejected(self):
        for row in [('bad', 'USD', '16000'), ('2026-09-16', 'USD', 'NaN'),
                    ('2026-09-16', 'USD', '-1'), ('2026-09-16', 'EUR', '16000')]:
            with self.subTest(row=row), self.assertRaises(ValueError):
                self.run_rows([row])
        with self.assertRaises(ValueError):
            self.run_rows([])

    def test_duplicate_keys_rejected(self):
        with self.assertRaises(ValueError):
            self.run_rows([('2026-09-16', 'USD', '16000')] * 2)


if __name__ == '__main__':
    unittest.main()
