"""Shared Beam transforms and native KafkaIO -> Dataflow pipeline."""
import argparse
import base64
import json
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from events import validate

FIELDS = [('schema_version', 'INTEGER'), ('event_id', 'STRING'), ('symbol', 'STRING'),
          ('currency', 'STRING'), ('price_usd', 'FLOAT'), ('event_timestamp', 'TIMESTAMP'),
          ('event_date', 'DATE'), ('ingested_at', 'TIMESTAMP'), ('replayed_at', 'TIMESTAMP'),
          ('source', 'STRING'), ('record_type', 'STRING'), ('source_interval', 'STRING'),
          ('ingestion_mode', 'STRING'), ('raw_payload', 'STRING'), ('kafka_topic', 'STRING'),
          ('kafka_partition', 'INTEGER'), ('kafka_offset', 'INTEGER')]
SCHEMA = {'fields': [{'name': name, 'type': kind, 'mode': 'NULLABLE'} for name, kind in FIELDS]}


def kafka_record(row):
    return {'value_base64': base64.b64encode(row.value or b'').decode(),
            'kafka_topic': row.topic, 'kafka_partition': int(row.partition), 'kafka_offset': int(row.offset)}


class ValidateEvent(beam.DoFn):
    def process(self, record):
        try:
            event = validate(json.loads(base64.b64decode(record['value_base64'], validate=True).decode('utf-8')))
            output = {name: event[name] for name, _ in FIELDS if not name.startswith('kafka_')}
            output.update({name: record[name] for name in ('kafka_topic', 'kafka_partition', 'kafka_offset')})
            beam.metrics.Metrics.counter('market', 'valid_events').inc()
            yield output
        except (ValueError, TypeError, KeyError, OverflowError, AttributeError) as exc:
            beam.metrics.Metrics.counter('market', 'invalid_events').inc()
            yield beam.pvalue.TaggedOutput('invalid', {**record, 'error': f'{type(exc).__name__}: {exc}'})


def validated(records):
    return records | 'Validate market events' >> beam.ParDo(ValidateEvent()).with_outputs('invalid', main='valid')


def json_line(value):
    return json.dumps(value, sort_keys=True, allow_nan=False)


def write_windowed(records, label, path):
    # Kafka processing time drives file windows. Old replay event dates do not drop data.
    return (records | f'{label} windows' >> beam.WindowInto(beam.window.FixedWindows(60))
            | f'{label} JSON' >> beam.Map(json_line)
            | f'{label} files' >> fileio.WriteToFiles(path=path, shards=1,
                                                   file_naming=fileio.default_file_naming('events', '.jsonl')))


def build_cloud(pipeline, args, consumer):
    from apache_beam.io.kafka import ReadFromKafka
    from apache_beam.io.gcp.bigquery_tools import RetryStrategy
    raw = (pipeline | 'Read Kafka' >> ReadFromKafka(consumer_config=consumer, topics=[args.topic],
                                                   with_metadata=True, timestamp_policy='ProcessingTime')
           | 'Keep Kafka provenance' >> beam.Map(kafka_record))
    root = f'{args.output_prefix.rstrip("/")}/{args.run_label}'
    write_windowed(raw, 'Raw', f'{root}/raw')
    parsed = validated(raw)
    write_windowed(parsed.invalid, 'Invalid', f'{root}/deadletter/validation')
    writes = parsed.valid | 'BigQuery staging' >> beam.io.WriteToBigQuery(
        args.table, schema=SCHEMA, method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR)
    failures = writes.failed_rows_with_errors | 'BQ errors JSON' >> beam.Map(
        lambda row: {'table': str(row[0]), 'row': row[1], 'errors': row[2]})
    write_windowed(failures, 'Warehouse errors', f'{root}/deadletter/bigquery')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', required=True)
    parser.add_argument('--topic', required=True)
    parser.add_argument('--consumer-group', required=True)
    parser.add_argument('--output-prefix', required=True)
    parser.add_argument('--table', required=True, help='project:dataset.table')
    parser.add_argument('--run-label', required=True, help='Unique deployment ID for isolated GCS output')
    parser.add_argument('--consumer-config', type=Path, help='Optional Kafka Java client security properties as JSON; keep outside Git')
    parser.add_argument('--validate-only', action='store_true', help='Expand/serialize graph without submitting any job')
    args, beam_args = parser.parse_known_args()
    # A Docker-only hostname cannot be used by remote Dataflow workers.
    hosts = [address.rsplit(':', 1)[0].lower() for address in args.bootstrap.split(',')]
    if not args.validate_only and any(host in {'kafka', 'localhost', '127.0.0.1', '0.0.0.0', 'host.docker.internal', '::1', '[::1]'} for host in hosts):
        parser.error('Dataflow requires a broker reachable from its VPC; local Docker Kafka is only for smoke tests')
    if not args.output_prefix.startswith('gs://') or '/' in args.run_label or args.run_label in {'.', '..'}:
        parser.error('Use a GCS output prefix and a simple unique run label')
    options = PipelineOptions(beam_args)
    if options.view_as(StandardOptions).runner != 'DataflowRunner':
        parser.error('Use --runner DataflowRunner; run local_smoke.py for bounded local validation')
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True
    consumer = json.loads(args.consumer_config.read_text()) if args.consumer_config else {}
    consumer.update({'bootstrap.servers': args.bootstrap, 'group.id': args.consumer_group,
                     'auto.offset.reset': 'earliest', 'enable.auto.commit': 'false'})
    # setuptools writes egg-info beside setup.py; source mount is deliberately read-only.
    with TemporaryDirectory(prefix='daud-beam-package-') as package_dir:
        source = Path(__file__).resolve().parent
        for filename in ['setup.py', 'events.py', 'beam_pipeline.py']:
            copyfile(source / filename, Path(package_dir) / filename)
        options.view_as(SetupOptions).setup_file = str(Path(package_dir) / 'setup.py')
        pipeline = beam.Pipeline(options=options)
        build_cloud(pipeline, args, consumer)
        if args.validate_only:
            graph = pipeline.to_runner_api()
            print(json.dumps({'status': 'graph_validated', 'transforms': len(graph.components.transforms),
                              'cloud_dataflow_executed': False}))
        else:
            result = pipeline.run()
            print('Submitted Dataflow job:', result.job_id())


if __name__ == '__main__':
    main()
