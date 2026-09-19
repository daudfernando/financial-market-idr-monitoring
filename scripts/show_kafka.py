"""Read broker topic metadata and three stored messages for the latest incremental demo."""
import argparse
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
BASE = ['docker','compose','--env-file','.env.airflow','-f','compose.yaml',
        '-f','compose.streaming.yaml','--profile','streaming','exec','-T','kafka']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', help='Optional exact topic; default latest market.incremental topic retained by Kafka')
    args = parser.parse_args()
    def run(parts, capture=False):
        return subprocess.run(BASE+parts,cwd=ROOT,check=True,text=True,capture_output=capture,timeout=45)
    listing = run(['/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka:19092','--list'],True)
    topics=sorted(t for t in listing.stdout.splitlines() if t.startswith('market.incremental.'))
    if not topics:
        raise RuntimeError('No incremental topic. Run run_streaming_demo.cmd first.')
    topic=args.topic or topics[-1]
    if topic not in topics:
        raise ValueError('Topic is not a retained incremental demo topic')
    print('1. TOPIC DI BROKER KAFKA:',topic,flush=True)
    run(['/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','kafka:19092','--describe','--topic',topic])
    print('\n2. BACA ULANG 3 PESAN LANGSUNG DARI KAFKA (bukan file lokal atau BigQuery)',flush=True)
    run(['/opt/kafka/bin/kafka-console-consumer.sh','--bootstrap-server','kafka:19092',
         '--topic',topic,'--from-beginning','--max-messages','3','--timeout-ms','15000',
         '--property','print.partition=true','--property','print.offset=true',
         '--property','print.key=true'])
    print('\nSelesai. Pembaca demo terpisah; pesan tidak dihapus dan tidak dikirim ulang ke BigQuery.')


if __name__=='__main__':
    main()
