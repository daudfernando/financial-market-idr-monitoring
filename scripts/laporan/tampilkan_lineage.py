import argparse
import shutil

from operator_pipeline.jalankan_proses import jalankan
from operator_pipeline.konfigurasi import COMPOSE, ROOT


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Regenerate docs/catalog from BigQuery metadata",
    )
    parser.add_argument(
        "--stop", action="store_true", help="Stop only the local documentation server"
    )
    args = parser.parse_args()

    if args.stop:
        jalankan(COMPOSE + ["stop", "dbt-docs"])
        return
    site = ROOT / "data/dbt/site"
    files = ["index.html", "manifest.json", "catalog.json"]
    if args.refresh or not all((site / f).is_file() for f in files):
        jalankan(
            COMPOSE
            + [
                "run",
                "--rm",
                "dbt",
                "docs",
                "generate",
                "--target-path",
                "/workspace/data/dbt/docs-target",
                "--vars",
                "{market_table: stg_market_replay_demo}",
            ]
        )
        target = ROOT / "data/dbt/docs-target"
        for f in files:
            if not (target / f).is_file():
                raise RuntimeError(f"Missing documentation artifact: {f}")
        site.mkdir(parents=True, exist_ok=True)
        for f in files:
            shutil.copyfile(target / f, site / f)
    jalankan(COMPOSE + ["up", "-d", "dbt-docs"])
    print(
        "dbt Docs: http://localhost:8086 (local documentation, not a build execution UI)"
    )


if __name__ == "__main__":
    main()
