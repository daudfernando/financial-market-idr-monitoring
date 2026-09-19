"""Send failure email to the configured SMTP endpoint (local Mailpit in Compose)."""
from datetime import datetime, timezone
from email.message import EmailMessage
import json
import logging
import os
from pathlib import Path
import smtplib


def failure_alert(context):
    ti = context['task_instance']
    record = {'timestamp': datetime.now(timezone.utc).isoformat(), 'dag_id': ti.dag_id,
              'task_id': ti.task_id, 'run_id': context.get('run_id', ti.run_id),
              'category': 'pipeline_failure', 'status': 'pending'}
    # Send identifiers, not exception text that could contain credentials or source payloads.
    message = EmailMessage()
    message['Subject'] = f'[FAILED] {ti.dag_id}.{ti.task_id}'
    message['From'] = 'airflow@localhost'
    message['To'] = os.getenv('ALERT_EMAIL_TO', 'daud@localhost')
    message.set_content(json.dumps(record, indent=2) + '\nCheck the task log in Airflow.')
    try:
        with smtplib.SMTP(os.getenv('ALERT_SMTP_HOST', 'mailpit'),
                          int(os.getenv('ALERT_SMTP_PORT', '1025')), timeout=15) as smtp:
            smtp.send_message(message)
        record['status'] = 'sent'
    except Exception:
        record['status'] = 'delivery_failed'
        logging.exception('Failure notification could not be delivered')
        raise
    finally:
        path = Path(os.getenv('PROJECT_ROOT', '/workspace')) / 'logs' / 'alerts.jsonl'
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('a', encoding='utf-8') as file:
            file.write(json.dumps(record) + '\n')
