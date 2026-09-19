"""Bounded Kafka capture + Beam DirectRunner validation, NOT a Dataflow cloud run."""
import argparse
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
import uuid

import apache_beam as beam
from confluent_kafka import Consumer, KafkaException, TopicPartition
from beam_pipeline import json_line, validated


def capture(bootstrap, topic, limit, seconds):
    consumer = Consumer({'bootstrap.servers': bootstrap, 'group.id': f'smoke-{uuid.uuid4().hex}',
                         'auto.offset.reset': 'earliest', 'enable.auto.commit': False})
    records = []
    try:
        metadata = consumer.list_topics(topic, timeout=15).topics[topic]
        if metadata.error:
            raise KafkaException(metadata.error)
        # Explicit beginning offsets make repeatable tests independent of group commits.
        consumer.assign([TopicPartition(topic, partition, -2) for partition in sorted(metadata.partitions)])
        deadline = time.monotonic() + seconds
        while len(records) < limit and time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            records.append({'value_base64': base64.b64encode(message.value() or b'').decode(),
                            'kafka_topic': message.topic(), 'kafka_partition': message.partition(),
                            'kafka_offset': message.offset()})
    finally:
        consumer.close()
    if len(records) != limit:
        raise RuntimeError(f'Expected {limit} Kafka records, captured {len(records)}')
    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'))
    parser.add_argument('--topic', default='market.prices.replay.v1')
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('--seconds', type=int, default=45)
    parser.add_argument('--output-root', type=Path, default=Path('/workspace/data/streaming'))
    args = parser.parse_args()
    if not 1 <= args.limit <= 10000 or not 1 <= args.seconds <= 300:
        parser.error('Bounded smoke test: limit 1..10000 and seconds 1..300')
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    folder = args.output_root / run_id
    folder.mkdir(parents=True)
    raw = capture(args.bootstrap, args.topic, args.limit, args.seconds)
    (folder / 'kafka_capture.jsonl').write_text(''.join(json_line(row) + '\n' for row in raw), encoding='utf-8')
    with beam.Pipeline() as pipeline:
        parsed = validated(pipeline | 'Captured Kafka data' >> beam.Create(raw))
        for label, collection in [('valid', parsed.valid), ('invalid', parsed.invalid)]:
            (collection | f'{label} encode' >> beam.Map(json_line)
             | f'{label} local file' >> beam.io.WriteToText(str(folder / label), file_name_suffix='.jsonl', num_shards=1))
    def read_output(prefix):
        return [json.loads(line) for file in folder.glob(f'{prefix}-*.jsonl')
                for line in file.read_text().splitlines() if line]
    valid, invalid = read_output('valid'), read_output('invalid')
    if len(valid) + len(invalid) != len(raw):
        raise RuntimeError('Kafka / Beam count mismatch')
    result = {'status': 'passed' if len(valid) == args.limit and not invalid else 'quality_failed',
              'run_id': run_id, 'runner': 'DirectRunner', 'source': 'bounded Kafka capture',
              'cloud_dataflow_executed': False, 'kafka_records': len(raw), 'valid': len(valid),
              'invalid': len(invalid), 'unique_event_ids': len({row['event_id'] for row in valid}),
              'modes': sorted({row['ingestion_mode'] for row in valid}), 'topic': args.topic}
    (folder / 'report.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps(result, indent=2))
    return int(result['status'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())
