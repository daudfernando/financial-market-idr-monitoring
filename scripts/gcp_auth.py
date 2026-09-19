"""Select local user ADC explicitly without changing machine environment variables."""
import os
from pathlib import Path
import google.auth
from google.cloud import storage


def storage_client(project, use_local_adc=False):
    if not use_local_adc:
        return storage.Client(project=project)
    path = Path(os.environ['APPDATA']) / 'gcloud' / 'application_default_credentials.json'
    credentials, _ = google.auth.load_credentials_from_file(
        str(path), scopes=['https://www.googleapis.com/auth/cloud-platform'],
        quota_project_id=project)
    return storage.Client(project=project, credentials=credentials)


def bigquery_client(project, location, use_local_adc=False):
    from google.cloud import bigquery
    if not use_local_adc:
        return bigquery.Client(project=project, location=location)
    path = Path(os.environ['APPDATA']) / 'gcloud' / 'application_default_credentials.json'
    credentials, _ = google.auth.load_credentials_from_file(
        str(path), scopes=['https://www.googleapis.com/auth/cloud-platform'], quota_project_id=project)
    return bigquery.Client(project=project, location=location, credentials=credentials)
