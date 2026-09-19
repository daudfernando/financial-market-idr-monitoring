"""Prepare ignored local Compose secrets without printing them."""
try:
    from .command_log import open_progress
except ImportError:
    from command_log import open_progress
import json
import os
from pathlib import Path
import secrets
from command_log import log_command

ROOT = Path(__file__).resolve().parents[1]


def main():
    log_command()
    adc = Path(os.environ['APPDATA']) / 'gcloud' / 'application_default_credentials.json'
    if not adc.is_file():
        raise FileNotFoundError('User ADC file is not available')
    state = ROOT / 'data' / 'airflow-state'
    state.mkdir(parents=True, exist_ok=True)
    config = ROOT / '.env.airflow'
    if not config.exists():
        config.write_text(f'AIRFLOW_DB_PASSWORD={secrets.token_hex(24)}\n'
                          f'AIRFLOW_JWT_SECRET={secrets.token_hex(32)}\n'
                          f'GCP_ADC_PATH={adc.as_posix()}\n', encoding='utf-8')
    passwords = state / 'simple_auth_manager_passwords.json'
    if not passwords.exists():
        passwords.write_text(json.dumps({'daud': secrets.token_urlsafe(20)}), encoding='utf-8')
    print('Local Compose environment prepared. Secrets are in ignored files; ADC is mounted read-only.')
    with open_progress() as file:
        file.write('- Compose env prepared; UI user daud; password stored locally in data/airflow-state/simple_auth_manager_passwords.json\n')


if __name__ == '__main__':
    main()
