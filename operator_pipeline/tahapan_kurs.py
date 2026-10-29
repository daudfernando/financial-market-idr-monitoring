import json
import re
import sys

import yaml

from operator_pipeline.daftar_proses import PROSES
from operator_pipeline.jalankan_proses import jalankan
from operator_pipeline.konfigurasi import ROOT


def baca_pipeline(path=None):
    path = path or ROOT / "config/pipeline_kurs.yaml"
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    tasks = config["tasks"]
    seen = set()
    for task in tasks:
        name = task["task_id"]
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name) or name in seen:
            raise ValueError("Task ID tidak valid atau duplikat")
        if task["proses"] not in PROSES:
            raise ValueError("Proses tidak terdaftar")
        if not set(task.get("depends_on", [])) <= seen:
            raise ValueError("Dependensi harus mendahului task dan tidak boleh siklik")
        if not isinstance(task.get("parameter"), list) or not all(
            isinstance(v, str) for v in task["parameter"]
        ):
            raise ValueError("Parameter harus berupa daftar string")
        seen.add(name)
    if (
        not tasks
        or tasks[0]["task_id"] != "collect"
        or tasks[0]["proses"] != "ambil-kurs"
    ):
        raise ValueError("Pipeline kurs harus dimulai dari collect")
    return config


def perintah_task(task, config, run_id=None):
    values = {
        "project": config["project"],
        "bucket": config["bucket"],
        "run_id": run_id,
    }
    if any("{run_id}" in p for p in task["parameter"]) and not re.fullmatch(
        r"\d{8}T\d{12}Z", run_id or ""
    ):
        raise ValueError("Source run ID diperlukan untuk tahap lanjutan")
    return [
        sys.executable,
        "-m",
        PROSES[task["proses"]],
        *(p.format_map(values) for p in task["parameter"]),
    ]


def jalankan_task(task, config, run_id=None):
    output = jalankan(perintah_task(task, config, run_id), timeout=600)
    if task["task_id"] == "collect":
        report = json.loads(output)
        if report["jisdor"]["status"] != "passed":
            raise ValueError("Ekstraksi kurs tidak lulus validasi")
        run_id = report["run_id"]
        if not re.fullmatch(r"\d{8}T\d{12}Z", run_id):
            raise ValueError("Source run ID tidak valid")
    return run_id


def jalankan_manual(task_id=None, run_id=None):
    config = baca_pipeline()
    tasks = config["tasks"]
    if task_id:
        tasks = [task for task in tasks if task["task_id"] == task_id]
        if not tasks:
            raise ValueError("Task ID tidak ditemukan di YAML")
    for task in tasks:
        run_id = jalankan_task(task, config, run_id)
    return run_id
