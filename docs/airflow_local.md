# Orkestrasi Airflow dan Alert Lokal

Stack pengembangan menggunakan Airflow 3.3.0 dengan Python 3.12, LocalExecutor, PostgreSQL 16, dan Mailpit. Mode `standalone` menjalankan komponen Airflow dalam satu container untuk menyederhanakan demo lokal. Konfigurasi ini ditujukan untuk pengembangan individual.

## Alur DAG

```text
jisdor_daily
collect -> upload_raw -> transform_spark -> upload -> warehouse

failure_alert_demo
intentionally_fail -> failure callback -> SMTP Mailpit
```

Task `collect` mengambil JISDOR terbaru dengan script khusus batch. Task `upload_raw` menyimpan XML ke GCS, `transform_spark` membaca raw dari GCS dan menormalisasi batch, lalu `upload` menerbitkan processed dan manifest. Task `warehouse` membaca manifest GCS lalu memuat staging, menjalankan MERGE, dan memeriksa kualitas BigQuery. Setiap task menjalankan script terpisah; stdout dan stderr tersedia di log Airflow.

Jadwal harian adalah 18.00 WIB. `catchup=False` mencegah otomatis menjalankan semua jadwal lampau. `max_active_runs=1` membatasi tumpang tindih batch. Task batch memiliki dua retry dengan jeda awal satu menit dan exponential backoff. Callback email dipanggil setelah task gagal final. Hari tanpa JISDOR baru tetap dapat memproses ulang referensi yang tersedia; pemeriksaan kalender publikasi khusus belum diterapkan.

Semua DAG dibuat dalam kondisi paused sampai siap diuji. DAG alert tidak mempunyai jadwal otomatis dan sengaja gagal untuk demonstrasi. Menandai task failed melalui UI tidak sama dengan kegagalan eksekusi yang memicu callback.

## Menjalankan stack

```powershell
.\.venv\Scripts\python.exe scripts\prepare_airflow.py
.\.venv\Scripts\python.exe scripts\compose.py config --quiet
.\.venv\Scripts\python.exe scripts\compose.py up -d --build
.\.venv\Scripts\python.exe scripts\compose.py ps
```

Wrapper `scripts/compose.py` menjalankan Docker Compose dari root proyek tanpa menambahkan catatan ke `logs/progress.txt`.

File `.env.airflow` berisi secret lokal yang dibuat sekali. Credential ADC pengguna di-mount read-only dari lokasi aslinya di komputer. Credential tidak disalin ke image, folder project, atau log. Container menggunakan ADC tersebut melalui path Linux. Versi dependency container tercatat di `airflow/requirements.txt`, terpisah dari virtual environment Windows.

## Membuka UI

- Airflow di `http://localhost:8085`, username `daud`.
- Password lokal ada di `data/airflow-state/simple_auth_manager_passwords.json`; buka file tersebut di editor.
- Kotak masuk email Mailpit di `http://localhost:8025`.

Kedua UI hanya di-bind ke loopback komputer. SMTP Mailpit hanya tersedia pada jaringan internal Compose. Email uji ditujukan ke `daud@localhost` dan disimpan lokal. Tidak ada pengiriman ke email eksternal. Untuk notifikasi operasional di luar komputer, kanal SMTP eksternal masih perlu dikonfigurasi.

## Command pengujian runtime

Setelah DAG berhasil diparse, pengujian dilakukan melalui trigger manual dan scheduler. Hasil tersedia pada log task Airflow dan report teknis JSON.

```powershell
.\.venv\Scripts\python.exe scripts\compose.py exec -T airflow airflow dags list-import-errors --output json
.\.venv\Scripts\python.exe scripts\compose.py exec -T airflow airflow dags unpause jisdor_daily
.\.venv\Scripts\python.exe scripts\compose.py exec -T airflow airflow dags trigger jisdor_daily
.\.venv\Scripts\python.exe scripts\compose.py exec -T airflow airflow dags unpause failure_alert_demo
.\.venv\Scripts\python.exe scripts\compose.py exec -T airflow airflow dags trigger failure_alert_demo
```

Jalankan demo failure hanya ketika ingin menguji alert. Periksa status DAG, inbox Mailpit, serta `logs/alerts.jsonl`. Jika SMTP gagal, callback mencatat `delivery_failed` dan melempar error; kegagalan pengiriman tidak disembunyikan.

## Menghentikan sementara

```powershell
.\.venv\Scripts\python.exe scripts\compose.py stop
```

Command tersebut mempertahankan volume database dan log. Jadwal hanya berjalan selama komputer, Docker, dan container Airflow aktif.

## Hasil verifikasi 12 September 2026

- Compose berhasil dibangun dan tiga service berjalan.
- DAG import errors kosong; kedua DAG terdaftar dan telah di-unpause.
- `jisdor_daily`, run `manual_validation_20260912`, berstatus success.
- Sumber batch `20260911T232029024590Z` berisi 14 record; 16 objek diunggah ke GCS.
- BigQuery berisi 16 tanggal unik dari 20 Agustus sampai 11 September 2026. `duplicate_keys=0`, `invalid_rows=0`, dan `batch_mismatches=0`.
- `failure_alert_demo`, run `alert_validation_20260912`, berstatus failed sesuai rancangan. Satu email diterima Mailpit dengan subject `[FAILED] failure_alert_demo.intentionally_fail`.
- `logs/alerts.jsonl` mencatat status `sent`. Sepuluh unit test lokal lulus.

Kendala yang diperbaiki adalah port 8080 bentrok, wrapper PowerShell diblokir execution policy, dan nama argumen task `run_id` dicadangkan Airflow. Port diganti ke 8085, wrapper diganti Python, dan argumen task menjadi `source_run_id`. Retry upload pertama setelah perbaikan berhasil. Pengaturan execution policy komputer tidak diubah.

Bukti warehouse ada di `logs/bigquery_batch_20260911T232142652210Z.json`. Run yang teruji adalah trigger manual melalui scheduler; eksekusi otomatis pada pukul 18.00 berikutnya belum diobservasi. Stack tetap berjalan setelah pengujian.

## Pekerjaan berikutnya

Orkestrasi batch kini memakai PySpark lokal. Streaming Kafka + Dataflow, dbt, dan dashboard analitik tetap menjadi pekerjaan berikutnya. Lihat [panduan PySpark](spark_batch.md) untuk hasil uji terbaru. Alert lokal memberikan bukti alur notifikasi untuk demo, sedangkan kanal eksternal belum diuji.

## Referensi

- [Airflow Docker](https://airflow.apache.org/docs/apache-airflow/3.3.0/howto/docker-compose/index.html).
- [Airflow callbacks](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html).
- [Mailpit Docker](https://mailpit.axllent.org/docs/install/docker/).
