import sys
from pathlib import Path
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from cloud_streaming_preflight import check, permission_result, resource_gaps
from requests.exceptions import ConnectionError


class PreflightTest(unittest.TestCase):
    def test_network_failure_is_unknown_not_missing_iam(self):
        session = Mock()
        session.request.side_effect = ConnectionError('DNS failed')
        result = check(session, 'permissions', 'https://example.invalid')
        self.assertEqual(permission_result(result, ['compute.instances.create'])['status'], 'unknown')

    def test_partial_access_identifies_exact_gap(self):
        result = {'ok': True, 'data': {'permissions': ['dataflow.jobs.create']}}
        self.assertEqual(permission_result(result, ['dataflow.jobs.create', 'compute.instances.create']),
                         {'status': 'missing_permissions', 'missing': ['compute.instances.create']})

    def test_forbidden_is_not_success(self):
        result = {'ok': False, 'http_status': 403, 'data': {'error': {}}}
        self.assertEqual(permission_result(result, ['iam.serviceAccounts.actAs'])['status'], 'unknown')

    def test_http_success_does_not_mean_resources_ready(self):
        data = {'dataflow_api': {'state': 'DISABLED'}, 'broker': {'status': 'TERMINATED'},
                'subnet': {'network': 'projects/test/global/networks/wrong'},
                'worker_identity': {'disabled': True}, 'warehouse': {}}
        checks = [{'check': name, 'ok': True, 'data': value} for name, value in data.items()]
        gaps = resource_gaps(checks, {'broker_ip': '10.90.0.10', 'network': 'daud-market'})
        self.assertEqual(len(gaps), 6)

    def test_expected_resource_configuration_has_no_gaps(self):
        data = {'dataflow_api': {'state': 'ENABLED'},
                'broker': {'status': 'RUNNING', 'networkInterfaces': [{'networkIP': '10.90.0.10'}]},
                'subnet': {'network': 'projects/test/global/networks/daud-market'},
                'worker_identity': {}, 'warehouse': {'timePartitioning': {'field': 'event_date'}}}
        checks = [{'check': name, 'ok': True, 'data': value} for name, value in data.items()]
        self.assertEqual(resource_gaps(checks, {'broker_ip': '10.90.0.10', 'network': 'daud-market'}), [])


if __name__ == '__main__':
    unittest.main()
