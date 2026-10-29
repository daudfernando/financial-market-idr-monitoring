import os
from pathlib import Path

import google.auth
from google.cloud import bigquery, storage


def kredensial(project, use_local_adc):
    if not use_local_adc:
        return None
    path = Path(os.environ["APPDATA"]) / "gcloud/application_default_credentials.json"
    credentials, _ = google.auth.load_credentials_from_file(
        str(path),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
        quota_project_id=project,
    )
    return credentials


def storage_client(project, use_local_adc=False):
    return storage.Client(
        project=project, credentials=kredensial(project, use_local_adc)
    )


def bigquery_client(project, location, use_local_adc=False):
    return bigquery.Client(
        project=project,
        location=location,
        credentials=kredensial(project, use_local_adc),
    )
