"""Display arriving consumer output for the next replay run, without changing data."""
import json
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parents[1] / 'data/streaming'


def main():
    existing = set(ROOT.glob('*'))
    print('Menunggu replay BARU. Jalankan run_demo.cmd di CMD kedua. Ctrl+C untuk berhenti.', flush=True)
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        candidates = sorted(p for p in ROOT.glob('*') if p not in existing and p.is_dir())
        if candidates:
            folder = candidates[-1]
            break
        time.sleep(.3)
    else:
        raise RuntimeError('Tidak ada run baru dalam 10 menit')
    print('Run:', folder.name, flush=True)
    path = folder / 'valid-00000.jsonl'
    while not path.exists():
        if (folder / 'report.json').exists() or time.monotonic() > deadline:
            raise RuntimeError('Consumer belum menghasilkan file valid; periksa report run')
        time.sleep(.2)
    print('OFFSET | SAHAM | HARGA USD | WAKTU SUMBER | WAKTU REPLAY', flush=True)
    count = 0
    with path.open(encoding='utf-8') as f:
        while time.monotonic() < deadline:
            position = f.tell()
            line = f.readline()
            if line.endswith('\n'):
                row = json.loads(line)
                count += 1
                print(f"{row['kafka_offset']} | {row['symbol']} | {row['price_usd']:.4f} | "
                      f"{row['event_timestamp']} | {row['replayed_at']}", flush=True)
            else:
                f.seek(position)
                if (folder / 'report.json').exists():
                    report = json.loads((folder / 'report.json').read_text())
                    print(f"Consumer selesai: {report['status']}; {count} event ditampilkan. "
                          'Upload GCS/BigQuery dan dbt masih dilanjutkan oleh run_demo.cmd.', flush=True)
                    return
                time.sleep(.1)
    raise RuntimeError('Batas waktu pemantauan tercapai; cek CMD pipeline')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nPemantauan dihentikan; pipeline tidak dihentikan.')
