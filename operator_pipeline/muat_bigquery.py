from google.cloud import bigquery


def muat_baris(client, rows, table, schema, location, disposition="WRITE_EMPTY"):
    if not rows:
        raise ValueError("Tidak ada baris untuk dimuat")
    job = client.load_table_from_json(
        rows,
        table,
        location=location,
        job_config=bigquery.LoadJobConfig(schema=schema, write_disposition=disposition),
    )
    job.result(timeout=120)
    if job.output_rows != len(rows):
        raise ValueError("Jumlah baris BigQuery berbeda dari sumber")
    return job


def muat_json_gcs(
    client, uri, table, schema, expected_rows, disposition="WRITE_TRUNCATE"
):
    if expected_rows < 1 or not uri.startswith("gs://"):
        raise ValueError("Sumber GCS atau jumlah baris tidak valid")
    job = client.load_table_from_uri(
        uri,
        table,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=disposition,
        ),
    )
    job.result(timeout=120)
    if job.output_rows != expected_rows:
        raise ValueError("Jumlah baris BigQuery berbeda dari sumber")
    return job


def muat_event(client, table, row, insert_id):
    errors = client.insert_rows_json(table, [row], row_ids=[insert_id], timeout=30)
    if errors:
        raise RuntimeError("BigQuery menolak event: " + str(errors))


def skema_saham():
    fields = [
        ("schema_version", "INT64"),
        ("event_id", "STRING"),
        ("symbol", "STRING"),
        ("currency", "STRING"),
        ("price_usd", "FLOAT64"),
        ("event_timestamp", "TIMESTAMP"),
        ("event_date", "DATE"),
        ("ingested_at", "TIMESTAMP"),
        ("replayed_at", "TIMESTAMP"),
        ("source", "STRING"),
        ("record_type", "STRING"),
        ("source_interval", "STRING"),
        ("ingestion_mode", "STRING"),
        ("raw_payload", "STRING"),
        ("kafka_topic", "STRING"),
        ("kafka_partition", "INT64"),
        ("kafka_offset", "INT64"),
    ]
    return [bigquery.SchemaField(name, kind) for name, kind in fields]
