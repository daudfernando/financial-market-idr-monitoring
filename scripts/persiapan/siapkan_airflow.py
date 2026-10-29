import json
import os
import secrets
from pathlib import Path

from operator_pipeline.konfigurasi import ROOT


def main():
    adc = (
        Path(os.environ["APPDATA"]) / "gcloud" / "application_default_credentials.json"
    )
    if not adc.is_file():
        raise FileNotFoundError("User ADC file is not available")
    state = ROOT / "data" / "airflow-state"
    state.mkdir(parents=True, exist_ok=True)
    config = ROOT / ".env.airflow"
    if not config.exists():
        config.write_text(
            f"AIRFLOW_DB_PASSWORD={secrets.token_hex(24)}\n"
            f"AIRFLOW_JWT_SECRET={secrets.token_hex(32)}\n"
            f"GCP_ADC_PATH={adc.as_posix()}\n",
            encoding="utf-8",
        )
    passwords = state / "simple_auth_manager_passwords.json"
    if not passwords.exists():
        passwords.write_text(
            json.dumps({"daud": secrets.token_urlsafe(20)}), encoding="utf-8"
        )
    print(
        "Local Compose environment prepared. Secrets are in ignored files; ADC is mounted read-only."
    )


if __name__ == "__main__":
    main()
