import subprocess
import sys

from operator_pipeline.konfigurasi import ROOT


def jalankan(command, timeout=900):
    print("Running:", subprocess.list2cmdline(command), flush=True)
    result = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        encoding="utf-8",
        errors="replace",
    )
    print(result.stdout, end="", flush=True)
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="", flush=True)
    if result.returncode:
        raise RuntimeError(f"Proses gagal dengan exit code {result.returncode}")
    return result.stdout
