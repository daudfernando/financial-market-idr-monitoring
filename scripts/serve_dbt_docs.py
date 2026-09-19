"""Generate dbt documentation once and serve an isolated local static site."""
import argparse
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ['docker', 'compose', '--env-file', '.env.airflow', '-f', 'compose.yaml',
           '-f', 'compose.analytics.yaml', '--profile', 'analytics']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--refresh', action='store_true', help='Regenerate docs/catalog from BigQuery metadata')
    parser.add_argument('--stop', action='store_true', help='Stop only the local documentation server')
    args = parser.parse_args()
    def run(parts):
        subprocess.run(COMPOSE + parts, cwd=ROOT, check=True)
    if args.stop:
        run(['stop', 'dbt-docs'])
        return
    site = ROOT / 'data/dbt/site'
    files = ['index.html', 'manifest.json', 'catalog.json']
    if args.refresh or not all((site / f).is_file() for f in files):
        run(['run', '--rm', 'dbt', 'docs', 'generate', '--target-path', '/workspace/data/dbt/docs-target',
             '--vars', '{market_table: stg_market_replay_demo}'])
        target = ROOT / 'data/dbt/docs-target'
        for f in files:
            if not (target / f).is_file():
                raise RuntimeError(f'Missing documentation artifact: {f}')
        site.mkdir(parents=True, exist_ok=True)
        for f in files:
            shutil.copyfile(target / f, site / f)
    run(['up', '-d', 'dbt-docs'])
    print('dbt Docs: http://localhost:8086 (local documentation, not a build execution UI)')


if __name__ == '__main__':
    main()
