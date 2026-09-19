"""Bounded Yahoo price producer; replay and live use separate Kafka topics."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import time

from confluent_kafka import Producer
from events import SYMBOLS, make_event, utc_now


class Sender:
    def __init__(self, bootstrap, topic):
        self.topic, self.sent, self.errors = topic, 0, []
        self.producer = Producer({'bootstrap.servers': bootstrap, 'enable.idempotence': True,
                                  'acks': 'all', 'message.timeout.ms': 30000})

    def delivered(self, error, message):
        if error:
            self.errors.append(str(error))
        else:
            self.sent += 1

    def send(self, event):
        self.producer.produce(self.topic, key=event['symbol'].encode(),
                              value=json.dumps(event, allow_nan=False).encode(), on_delivery=self.delivered)
        self.producer.poll(0)

    def finish(self):
        pending = self.producer.flush(35)
        if pending or self.errors:
            raise RuntimeError(f'Kafka delivery failed: pending={pending}; errors={self.errors[:3]}')


async def live(sender, symbols, seconds, rejected_path):
    import yfinance as yf
    deadline = time.monotonic() + seconds
    reconnects = rejected = 0
    connection_errors = []

    def handler(payload):
        nonlocal rejected
        try:
            event = make_event(payload, 'live_websocket')
        except (ValueError, KeyError, TypeError, OverflowError) as exc:
            rejected += 1
            with rejected_path.open('a', encoding='utf-8') as output:
                output.write(json.dumps({'payload': payload, 'error': f'{type(exc).__name__}: {exc}',
                                         'received_at': utc_now()}, default=str) + '\n')
            return
        sender.send(event)

    while time.monotonic() < deadline:
        async def session():
            async with yf.AsyncWebSocket(verbose=False) as websocket:
                await websocket.subscribe(symbols)
                await websocket.listen(handler)
        try:
            await asyncio.wait_for(session(), max(0.1, deadline - time.monotonic()))
        except TimeoutError:
            break
        except Exception as exc:
            reconnects += 1
            connection_errors.append(f'{type(exc).__name__}: {exc}')
            await asyncio.sleep(min(5, max(0, deadline - time.monotonic())))
    return {'reconnects': reconnects, 'rejected_events': rejected, 'connection_errors': connection_errors[-3:]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'))
    parser.add_argument('--mode', choices=['replay', 'live_websocket'], required=True)
    parser.add_argument('--input', type=Path)
    parser.add_argument('--topic')
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('--interval', type=float, default=0.1)
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--symbols', nargs='+', choices=sorted(SYMBOLS), default=['JPM'])
    parser.add_argument('--output-root', type=Path, default=Path('/workspace/data/streaming_producer'))
    args = parser.parse_args()
    if args.limit < 1 or args.interval < 0 or not 5 <= args.seconds <= 3600:
        parser.error('Positive limit, nonnegative interval, and seconds between 5 and 3600 required')
    if args.mode == 'replay' and not args.input:
        parser.error('--input is required for replay')
    topic = args.topic or ('market.prices.replay.v1' if args.mode == 'replay' else 'market.prices.live.v1')
    sender = Sender(args.bootstrap, topic)
    folder = args.output_root / utc_now().replace(':', '').replace('+', '_')
    folder.mkdir(parents=True)
    details = {}
    try:
        if args.mode == 'replay':
            with args.input.open(encoding='utf-8') as source:
                for index, line in enumerate(source):
                    if index >= args.limit:
                        break
                    sender.send(make_event(json.loads(line), 'replay'))
                    time.sleep(args.interval)
        else:
            details = asyncio.run(live(sender, args.symbols, args.seconds, folder / 'rejected.jsonl'))
    finally:
        sender.finish()
    report = {'status': 'passed' if sender.sent else 'no_events', 'mode': args.mode,
              'topic': topic, 'delivered': sender.sent, 'finished_at': utc_now(), **details}
    (folder / 'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report))
    return 0 if sender.sent else 2


if __name__ == '__main__':
    raise SystemExit(main())
