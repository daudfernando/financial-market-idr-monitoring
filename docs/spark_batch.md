# Batch JISDOR dengan PySpark

Alur aktif: `collect -> upload_raw -> transform_spark -> upload -> warehouse`.

1. Airflow mengambil XML Bank Indonesia dan metadata batch. Collector tidak lagi menulis partisi processed.
2. `upload_raw` menyimpan XML di GCS dengan aturan tidak menimpa objek yang berbeda.
3. PySpark 4.0.1 membaca XML yang diunduh dari GCS setelah checksum cocok dengan sumber. Spark menormalisasi tanggal dalam Asia/Jakarta, mata uang, dan kurs decimal. Batch kosong, nilai invalid, atau key duplikat ditolak.
4. Hasil ditulis ke partisi JSON harian. Untuk batch kecil ini hasil Spark dikumpulkan di driver; belum merupakan pipeline batch skala besar. `ingested_at` berasal dari ID pengambilan sumber agar stabil saat retry. Waktu publikasi FX belum terverifikasi dan tetap null.
5. Upload hasil memvalidasi ulang kecocokan dengan XML, lalu menerbitkan manifest terakhir. BigQuery memverifikasi checksum sebelum staging dan MERGE.

Spark berjalan lokal dengan dua thread di container Airflow, memakai Java 17. Tidak menggunakan cluster Dataproc. Kafka dan Dataflow merupakan rancangan streaming berikutnya, belum terpasang atau berjalan.

## Menjalankan dari CMD

Jalankan dari `C:\Purwadhika\Final Project` dengan Docker Desktop aktif.

```bat
docker compose --env-file .env.airflow build airflow
docker compose --env-file .env.airflow up -d
docker compose --env-file .env.airflow exec -T airflow airflow dags trigger jisdor_daily
```

Lihat kelima task pada http://localhost:8085. Gunakan run ID yang muncul untuk memeriksa status:

```bat
docker compose --env-file .env.airflow exec -T airflow airflow tasks states-for-dag-run jisdor_daily RUN_ID_AIRFLOW
```

`RUN_ID_AIRFLOW` berbeda dari ID folder sumber. Keduanya dapat dilihat pada run/task log. Untuk transformasi manual gunakan ID folder sumber baru yang sudah melewati `upload_raw`; folder lama yang dibuat collector Python dapat memiliki output berbeda dan akan ditolak agar tidak tertimpa.

## Verifikasi 16 September 2026

Run Airflow `spark_validation_20260916` berhasil untuk semua lima task. Sumber `20260916T112208364096Z` menghasilkan 14 record valid melalui Spark. BigQuery memiliki 19 tanggal unik (20 Agustus–16 September), duplikasi 0, invalid 0, dan batch mismatch 0.

- Bukti Spark: `data/source_samples/20260916T112208364096Z/spark_report.json`.
- Bukti warehouse: `logs/bigquery_batch_20260916T112234102450Z.json`.
- Rerun transformasi pada sumber yang sama berhasil dan menghasilkan output identik.
- Sepuluh unit test pipeline sebelumnya lulus.
- Tiga test integrasi Spark lulus, memakai fixture sintetis: normalisasi zona waktu dan decimal; penolakan tanggal/rate/currency invalid serta batch kosong; penolakan key duplikat.

```bat
.venv\Scripts\python.exe -m unittest discover -s tests -v
docker compose --env-file .env.airflow cp tests/test_spark_batch.py airflow:/tmp/test_spark_batch.py
docker compose --env-file .env.airflow exec -T airflow python /tmp/test_spark_batch.py
```

Test Spark dilewati di Windows jika PySpark tidak terpasang; jalankan command Docker untuk mengujinya. Penulisan progress TXT dimatikan secara default (`WRITE_PROGRESS_LOG=0`). Report teknis JSON dan log operasional Airflow tetap tersedia untuk debugging.
