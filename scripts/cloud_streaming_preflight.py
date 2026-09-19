"""Read-only deployment checks using the selected ADC, with no IAM mutations."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path

import google.auth
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import RequestException

ROOT = Path(__file__).resolve().parents[1]
PROVISION = ['compute.instances.create', 'compute.networks.create', 'compute.subnetworks.create',
             'compute.firewalls.create', 'iam.serviceAccounts.create', 'resourcemanager.projects.setIamPolicy']
SUBMIT = ['dataflow.jobs.create', 'dataflow.jobs.get', 'dataflow.jobs.cancel', 'dataflow.jobs.updateContents']


def check(session, label, url, body=None):
    try:
        response = session.request('POST' if body is not None else 'GET', url, json=body, timeout=20)
        try:
            value = response.json()
        except ValueError:
            value = {'error': {'message': 'Non-JSON response'}}
        return {'check': label, 'http_status': response.status_code, 'ok': response.ok, 'data': value}
    except RequestException as exc:
        # Transport failure is unknown, never interpreted as permission denied.
        return {'check': label, 'http_status': None, 'ok': False,
                'error': type(exc).__name__, 'message': 'Transport/DNS failure; retry when network is available'}


def permission_result(result, required):
    if not result['ok']:
        return {'status': 'unknown', 'missing': None}
    missing = sorted(set(required) - set(result['data'].get('permissions', [])))
    return {'status': 'passed' if not missing else 'missing_permissions', 'missing': missing}


def resource_gaps(checks, config):
    by_name = {row['check']: row for row in checks}
    gaps = [row['check'] + ': unavailable or unknown' for row in checks if not row['ok']]
    data = lambda name: by_name[name].get('data', {})
    if data('dataflow_api').get('state') != 'ENABLED':
        gaps.append('Dataflow API is not confirmed ENABLED')
    if data('broker').get('status') != 'RUNNING':
        gaps.append('Broker VM is not confirmed RUNNING')
    interfaces = data('broker').get('networkInterfaces', [])
    if not any(x.get('networkIP') == config['broker_ip'] for x in interfaces):
        gaps.append('Broker private IP is not confirmed')
    if not data('subnet').get('network', '').endswith('/' + config['network']):
        gaps.append('Subnet network differs from deployment plan')
    if data('worker_identity').get('disabled', False):
        gaps.append('Worker service account is disabled')
    if data('warehouse').get('timePartitioning', {}).get('field') != 'event_date':
        gaps.append('Warehouse event_date partition is not confirmed')
    return gaps


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--use-local-adc', action='store_true')
    parser.add_argument('--config', type=Path, default=ROOT / 'config' / 'streaming_cloud.json')
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding='utf-8'))
    project = config['project_id']
    auth_args = {'scopes': ['https://www.googleapis.com/auth/cloud-platform'], 'quota_project_id': project}
    if args.use_local_adc:
        path = Path(os.environ['APPDATA']) / 'gcloud' / 'application_default_credentials.json'
        credentials, _ = google.auth.load_credentials_from_file(str(path), **auth_args)
    else:
        credentials, _ = google.auth.default(**auth_args)
    session = AuthorizedSession(credentials)
    project_check = check(session, 'project_permissions',
                          f'https://cloudresourcemanager.googleapis.com/v1/projects/{project}:testIamPermissions',
                          {'permissions': PROVISION + SUBMIT})
    print(json.dumps({'provision': permission_result(project_check, PROVISION),
                      'submit_and_stop': permission_result(project_check, SUBMIT)}), flush=True)
    compute = f'https://compute.googleapis.com/compute/v1/projects/{project}'
    account = config['worker_service_account']
    checks = [project_check]
    endpoints = [
        ('dataflow_api', f'https://serviceusage.googleapis.com/v1/projects/{project}/services/dataflow.googleapis.com', None),
        ('subnet', f'{compute}/regions/{config["region"]}/subnetworks/{config["subnet"]}', None),
        ('broker', f'{compute}/zones/{config["zone"]}/instances/{config["broker_vm"]}', None),
        ('worker_identity', f'https://iam.googleapis.com/v1/projects/{project}/serviceAccounts/{account}', None),
        ('worker_act_as', f'https://iam.googleapis.com/v1/projects/{project}/serviceAccounts/{account}:testIamPermissions',
         {'permissions': ['iam.serviceAccounts.actAs']}),
        ('warehouse', f'https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{config["dataset"]}/tables/{config["table"]}', None)
    ]
    for label, url, body in endpoints:
        result = check(session, label, url, body)
        checks.append(result)
        print(json.dumps({'check': label, 'http_status': result['http_status'], 'ok': result['ok']}), flush=True)
    # The API responses are diagnostic evidence, not proof of worker IAM or connectivity.
    result = {'checked_at': datetime.now(timezone.utc).isoformat(), 'project': project,
              'provision_permissions': permission_result(project_check, PROVISION),
              'job_permissions': permission_result(project_check, SUBMIT),
              'worker_act_as': permission_result(next(x for x in checks if x['check'] == 'worker_act_as'), ['iam.serviceAccounts.actAs']),
              'resource_gaps': resource_gaps(checks, config),
              'checks': checks, 'cloud_job_submitted': False,
              'runtime_connectivity_verified': False}
    folder = ROOT / 'data' / 'deployment_checks'
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.json')
    path.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print('Report:', path)
    return int(bool(result['resource_gaps']) or result['job_permissions']['status'] != 'passed'
               or result['worker_act_as']['status'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())
