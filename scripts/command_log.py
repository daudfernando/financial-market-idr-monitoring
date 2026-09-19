"""Record reproducible PowerShell commands for project scripts (no secret arguments)."""
from datetime import datetime, timezone
from pathlib import Path
import sys
import os
import shlex
import io

ROOT = Path(__file__).resolve().parents[1]


def log_command():
    script = Path(sys.argv[0]).resolve().relative_to(ROOT).as_posix()
    args = ' '.join("'" + value.replace("'", "''") + "'" for value in sys.argv[1:])
    command = (f".\\.venv\\Scripts\\python.exe '{script}' {args}".rstrip() if os.name == 'nt'
               else shlex.join([sys.executable, script, *sys.argv[1:]]))
    if os.getenv('WRITE_PROGRESS_LOG', '0') != '1':
        return command
    log = ROOT / 'logs' / 'progress.txt'
    log.parent.mkdir(exist_ok=True)
    with log.open('a', encoding='utf-8') as file:
        file.write(f'\n[{datetime.now(timezone.utc).isoformat()}] Command\n'
                   f'- Working directory: {ROOT}\n{command}\n')
    return command


def open_progress():
    if os.getenv('WRITE_PROGRESS_LOG', '0') != '1':
        return io.StringIO()
    path = ROOT / 'logs' / 'progress.txt'
    path.parent.mkdir(exist_ok=True)
    return path.open('a', encoding='utf-8')
