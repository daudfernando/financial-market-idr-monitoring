"""Package project producer and a bounded real source sample for the Kafka VM."""
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'streaming'))
from events import make_event


def main():
    source = ROOT / 'data/source_samples/20260909T214210877646Z/jpm_replay_candidates.jsonl'
    rows = [json.loads(line) for line in source.read_text(encoding='utf-8').splitlines()[:100]]
    if len(rows) != 100:
        raise ValueError('Expected at least 100 real replay candidates')
    for row in rows:
        make_event(row, 'replay')
    payloads = {name: (ROOT / 'streaming' / name).read_bytes() for name in ['producer.py', 'events.py']}
    payloads['replay.jsonl'] = ''.join(json.dumps(row) + '\n' for row in rows).encode()
    folder = ROOT / 'data/cloud_replay'
    folder.mkdir(parents=True, exist_ok=True)
    bundle = folder / 'replay_bundle.tar.gz'
    with tarfile.open(bundle, 'w:gz') as archive:
        for name, content in payloads.items():
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(content), 0o644
            archive.addfile(info, io.BytesIO(content))
    report = {'prepared_at': datetime.now(timezone.utc).isoformat(), 'records': len(rows),
              'source': source.relative_to(ROOT).as_posix(),
              'bundle_sha256': hashlib.sha256(bundle.read_bytes()).hexdigest(),
              'files': {name: hashlib.sha256(data).hexdigest() for name, data in payloads.items()},
              'uploaded': False}
    (folder / 'manifest.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
