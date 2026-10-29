import os
from pathlib import Path

ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1])).resolve()
COMPOSE = [
    "docker",
    "compose",
    "--env-file",
    ".env.airflow",
    "-f",
    "compose.yaml",
    "-f",
    "compose.streaming.yaml",
    "-f",
    "compose.analytics.yaml",
    "--profile",
    "streaming",
    "--profile",
    "analytics",
]
