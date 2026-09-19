"""Finite real-history replay: Kafka -> validation -> GCS -> BigQuery, one event at a time."""
import argparse
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
import time

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from google.cloud import bigquery, storage
from events import make_event, utc_now
from producer import Sender
from python_consumer import decode_record

PROJECT = 'jcdeah-009'
TABLE = PROJECT + '.daud_finalproject.demo_streaming_events'
VIEW = PROJECT + '.daud_finalproject_demo.demo_streaming_events'
ROOT = Path('/workspace')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=40)
    parser.add_argument('--interval', type=float, default=1)
    args = parser.parse_args()
    if not 4 <= args.limit <= 200 or not 0.1 <= args.interval <= 3:
        parser.error('Demo: limit 4..200; interval 0.1..3 seconds')
    config = json.loads((ROOT / 'config/current_market_replay.json').read_text())
    all_payloads = [json.loads(s) for s in (ROOT / config['input']).read_text().splitlines()]
    # Last N bars, then chronological delivery: a demo uses latest source time, not oldest.
    payloads = sorted(all_payloads, key=lambda r:(r['bar_start_utc'],r['symbol']))[-args.limit:]
    if len(payloads) != args.limit:
        raise ValueError('Insufficient source rows')
    # Validate the complete bounded input before creating cloud resources.
    for p in payloads:
        make_event(p, 'replay')
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    topic = 'market.incremental.' + run_id.lower()
    folder = ROOT / 'data/incremental_streaming' / run_id
    folder.mkdir(parents=True)
    bq = bigquery.Client(project=PROJECT, location='asia-southeast2')
    bucket = storage.Client(project=PROJECT).bucket('jcdeah-009-daud-finalproject')
    template = bq.get_table(PROJECT + '.daud_finalproject.stg_market_replay_demo')
    table = bigquery.Table(TABLE, schema=list(template.schema) + [
        bigquery.SchemaField('demo_run_id', 'STRING'), bigquery.SchemaField('consumer_received_at', 'TIMESTAMP'),
        bigquery.SchemaField('sink_requested_at', 'TIMESTAMP')])
    table.time_partitioning = bigquery.TimePartitioning(field='event_date')
    table.clustering_fields = ['demo_run_id', 'symbol']
    table.description = 'Isolated per-message Kafka replay demonstration. Real historical prices, not live market data. Append across runs.'
    bq.create_table(table, exists_ok=True)
    view = bigquery.Table(VIEW)
    view.view_query = f'''SELECT * FROM `{TABLE}`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY demo_run_id, event_id
      ORDER BY sink_requested_at DESC, kafka_offset DESC) = 1'''
    bq.create_table(view, exists_ok=True)
    bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    admin = AdminClient({'bootstrap.servers': bootstrap})
    for future in admin.create_topics([
            NewTopic(topic, num_partitions=1, replication_factor=1)]).values():
        future.result(timeout=30)
    consumer = Consumer({'bootstrap.servers': bootstrap, 'group.id': 'incremental-'+run_id,
                         'enable.auto.commit': False, 'auto.offset.reset': 'earliest'})
    consumer.assign([TopicPartition(topic, 0, -2)])
    stop = threading.Event()
    errors = []
    report = {'run_id':run_id, 'status':'running', 'mode':'real_historical_replay',
              'table':TABLE, 'view':VIEW, 'topic':topic, 'expected':args.limit,
              'producer_sent':0, 'bq_accepted':0, 'cloud_dataflow_executed':False,
              'started_at':utc_now(), 'input':config['input']}
    report.update(source_first=payloads[0]['bar_start_utc'],source_last=payloads[-1]['bar_start_utc'])
    output_lock = threading.Lock()
    def say(text):
        with output_lock:
            print(text, flush=True)
    def produce():
        try:
            sender = Sender(bootstrap, topic)
            for i, payload in enumerate(payloads,1):
                if stop.is_set():
                    break
                event = make_event(payload, 'replay')
                sender.send(event)
                sender.finish()  # Delivery acknowledgement per message, for a readable demo.
                report['producer_sent'] = i
                say(f"[KIRIM {i:03}] {event['symbol']} USD {event['price_usd']:.4f} | sumber={event['event_timestamp']}")
                if stop.wait(args.interval):
                    break
            report['producer_finished_at'] = utc_now()
        except Exception as exc:
            errors.append(exc)
            stop.set()
    def query_rows():
        config_query = bigquery.QueryJobConfig(use_query_cache=False, maximum_bytes_billed=104857600,
            query_parameters=[bigquery.ScalarQueryParameter('run_id','STRING',run_id)])
        return [dict(r) for r in bq.query(f'SELECT * FROM `{VIEW}` WHERE demo_run_id=@run_id',
                    job_config=config_query).result(timeout=60)]
    thread = threading.Thread(target=produce, daemon=True)
    expected = {}
    try:
        say(f'RUN_ID: {run_id}\nReplay per pesan -> Kafka -> Python -> BigQuery. BUKAN harga live.\nTABEL: {TABLE}')
        say(f"SUMBER TERPILIH: {report['source_first']} s/d {report['source_last']}")
        thread.start()
        deadline = time.monotonic() + 900
        with (folder/'events.jsonl').open('w', buffering=1) as evidence:
            while report['bq_accepted'] < args.limit and time.monotonic() < deadline:
                if errors:
                    raise errors[0]
                msg = consumer.poll(1)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                received = utc_now()
                raw = {'value_base64':base64.b64encode(msg.value() or b'').decode(),
                       'kafka_topic':msg.topic(),'kafka_partition':msg.partition(),'kafka_offset':msg.offset()}
                event = decode_record(raw)
                say(f"[TERIMA offset={msg.offset()}] {event['symbol']} valid | consumer={received}")
                # Each raw message is persisted independently before warehouse insertion.
                bucket.blob(f'final-project/demo/incremental/{run_id}/raw/{msg.offset():06}.json').upload_from_string(
                    json.dumps(raw),content_type='application/json',if_generation_match=0,timeout=30)
                row = {**event,'demo_run_id':run_id,'consumer_received_at':received,'sink_requested_at':utc_now()}
                for attempt in range(3):
                    try:
                        failures = bq.insert_rows_json(TABLE, [row], row_ids=[run_id+':'+event['event_id']], timeout=30)
                        if failures:
                            raise RuntimeError('BigQuery rejected event: '+json.dumps(failures))
                        break
                    except Exception:
                        if attempt == 2:
                            raise
                        time.sleep(attempt+1)
                report['bq_accepted'] += 1
                expected[event['event_id']] = row
                evidence.write(json.dumps(row)+'\n')
                say(f"[BIGQUERY {report['bq_accepted']:03}/{args.limit}] diterima API | offset={msg.offset()} | {event['symbol']}")
                if report['bq_accepted'] == 1:
                    visible = query_rows()
                    report['mid_run_visible_records'] = len(visible)
                    report['mid_run_checked_at'] = utc_now()
                    say(f'[CEK SAAT RUN] {len(visible)} baris sudah terlihat lewat SELECT sebelum consumer selesai.')
        if report['bq_accepted'] != args.limit:
            raise RuntimeError('Consumer timeout/incomplete')
        thread.join(timeout=40)
        if thread.is_alive() or errors or report['producer_sent'] != args.limit:
            raise RuntimeError('Producer did not complete successfully')
        visible = []
        for attempt in range(8):
            visible = query_rows()
            if len(visible) == args.limit:
                break
            time.sleep(2)
        if {r['event_id'] for r in visible} != set(expected):
            raise RuntimeError('BigQuery read-back reconciliation failed')
        for row in visible:
            for key, value in expected[row['event_id']].items():
                actual = row[key]
                if isinstance(actual, datetime):
                    actual = actual.astimezone(timezone.utc).isoformat()
                elif key == 'event_date':
                    actual = str(actual)
                if actual != value:
                    raise RuntimeError(f'Warehouse mismatch in {key}')
        report.update(status='passed', bq_visible=len(visible), mismatches=0)
        say(f'SUKSES: {len(visible)} pesan diverifikasi di BigQuery. Run: {run_id}')
    except BaseException as exc:
        report.update(status='failed',error=type(exc).__name__+': '+str(exc))
        raise
    finally:
        stop.set()
        if thread.ident:
            thread.join(timeout=40)
        consumer.close()
        report['finished_at'] = utc_now()
        (folder/'report.json').write_text(json.dumps(report,indent=2))
        print('REPORT:',str(folder/'report.json'),flush=True)


if __name__ == '__main__':
    main()
