"""Process arriving Kafka events individually; finite replay demo, no Beam/Dataflow."""
import argparse
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
import uuid

from confluent_kafka import Consumer, KafkaException, TopicPartition
from events import validate, utc_now


def decode_record(record):
    event = validate(json.loads(base64.b64decode(record['value_base64'], validate=True).decode('utf-8')))
    return {**event, **{key: record[key] for key in ('kafka_topic', 'kafka_partition', 'kafka_offset')}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'))
    parser.add_argument('--topic', required=True)
    parser.add_argument('--limit', type=int, required=True)
    parser.add_argument('--seconds', type=int, default=300)
    parser.add_argument('--run-id', required=True)
    args = parser.parse_args()
    datetime.strptime(args.run_id, '%Y%m%dT%H%M%S%fZ')
    if not 1 <= args.limit <= 10000 or not 1 <= args.seconds <= 3600:
        parser.error('limit 1..10000, seconds 1..3600')
    folder = Path('/workspace/data/streaming') / args.run_id
    folder.mkdir(parents=True, exist_ok=False)
    report = dict(run_id=args.run_id, runner='PythonConsumer', topic=args.topic,
                  source='Kafka event-by-event replay', cloud_dataflow_executed=False,
                  started_at=utc_now(), kafka_records=0, valid=0, invalid=0)
    consumer = Consumer({'bootstrap.servers': args.bootstrap, 'group.id': f'replay-{uuid.uuid4().hex}',
                         'auto.offset.reset': 'earliest', 'enable.auto.commit': False})
    ids, modes, delays = set(), set(), []
    try:
        metadata = consumer.list_topics(args.topic, timeout=15).topics[args.topic]
        if metadata.error:
            raise KafkaException(metadata.error)
        consumer.assign([TopicPartition(args.topic, p, -2) for p in sorted(metadata.partitions)])
        (folder / 'ready.json').write_text(json.dumps({'ready_at': utc_now()}))
        deadline = time.monotonic() + args.seconds
        with (folder / 'kafka_capture.jsonl').open('w', buffering=1) as raw, \
             (folder / 'valid-00000.jsonl').open('w', buffering=1) as valid, \
             (folder / 'invalid-00000.jsonl').open('w', buffering=1) as invalid:
            while report['kafka_records'] < args.limit and time.monotonic() < deadline:
                msg = consumer.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                record = dict(value_base64=base64.b64encode(msg.value() or b'').decode(),
                              kafka_topic=msg.topic(), kafka_partition=msg.partition(), kafka_offset=msg.offset())
                raw.write(json.dumps(record) + '\n')
                report['kafka_records'] += 1
                try:
                    event = decode_record(record)
                except (ValueError, KeyError, TypeError, OverflowError, AttributeError) as exc:
                    invalid.write(json.dumps({**record, 'error': type(exc).__name__}) + '\n')
                    report['invalid'] += 1
                    continue
                valid.write(json.dumps(event, allow_nan=False) + '\n')
                report['valid'] += 1
                ids.add(event['event_id'])
                modes.add(event['ingestion_mode'])
                processed = datetime.now(timezone.utc)
                report.setdefault('first_processed_at', processed.isoformat())
                report['last_processed_at'] = processed.isoformat()
                delays.append((processed - datetime.fromisoformat(event['ingested_at'])).total_seconds())
        if report['valid'] != args.limit or report['invalid']:
            raise RuntimeError('Incomplete or invalid replay; warehouse publication refused')
        report['status'] = 'passed'
    except Exception as exc:
        report.update(status='failed', error=type(exc).__name__ + ': ' + str(exc))
        raise
    finally:
        consumer.close()
        report.update(finished_at=utc_now(), unique_event_ids=len(ids), modes=sorted(modes),
                      mean_replay_processing_seconds=sum(delays) / len(delays) if delays else None)
        (folder / 'report.json').write_text(json.dumps(report, indent=2))
        print(json.dumps(report))


if __name__ == '__main__':
    main()
