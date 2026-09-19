"""Docker Compose wrapper that records commands and exit codes without shell execution."""
try:
    from .command_log import open_progress
except ImportError:
    from command_log import open_progress
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
from command_log import log_command

ROOT = Path(__file__).resolve().parents[1]


def main():
    log_command()
    if len(sys.argv) < 2:
        raise SystemExit('Supply Compose arguments, e.g. ps')
    command = ['docker', 'compose', '--env-file', '.env.airflow', *sys.argv[1:]]
    with open_progress() as file:
        file.write('- Docker command: ' + subprocess.list2cmdline(command) + '\n')
    result = subprocess.run(command, cwd=ROOT)
    with open_progress() as file:
        file.write(f'- Docker Compose exit code: {result.returncode}\n')
    return result.returncode


if __name__ == '__main__':
    raise SystemExit(main())
