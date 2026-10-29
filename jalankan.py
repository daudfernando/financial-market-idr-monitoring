import argparse
import runpy
import sys

from operator_pipeline.daftar_proses import PROSES


def main():
    parser = argparse.ArgumentParser(description="Pipeline kurs dan saham")
    parser.add_argument("proses", choices=["batch", *PROSES])
    args, parameters = parser.parse_known_args()
    if args.proses == "batch":
        from operator_pipeline.tahapan_kurs import jalankan_manual

        batch_parser = argparse.ArgumentParser(
            description="Tahap manual dari YAML yang sama dengan Airflow"
        )
        batch_parser.add_argument("--task")
        batch_parser.add_argument("--run-id")
        batch_args = batch_parser.parse_args(parameters)
        print(jalankan_manual(batch_args.task, batch_args.run_id))
        return
    sys.argv = [PROSES[args.proses], *parameters]
    runpy.run_module(PROSES[args.proses], run_name="__main__")


if __name__ == "__main__":
    main()
