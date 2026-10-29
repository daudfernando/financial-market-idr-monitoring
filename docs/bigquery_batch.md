# Batch kurs, manual dan otomatis

Alur: BI XML, raw GCS, Spark, processed GCS, staging BigQuery, MERGE fact, audit tanggal.

| Task ID | Proses | Modul |
|---|---|---|
| collect | Ambil 45 hari terakhir | scripts/ekstraksi/ambil_kurs_bi.py |
| upload_raw | Simpan XML sumber | scripts/pemuatan/simpan_kurs_gcs.py |
| transform_spark | Cleaning dan validasi | scripts/transformasi/bersihkan_kurs_spark.py |
| upload | Simpan processed dan manifest | scripts/pemuatan/simpan_kurs_gcs.py |
| warehouse | Load, MERGE dan rekonsiliasi | scripts/pemuatan/muat_kurs_bigquery.py |
| check_dates | Audit tanggal | scripts/validasi/periksa_tanggal_kurs.py |

Semua task memakai OperatorKurs dan `config/pipeline_kurs.yaml`. Manual dan otomatis memakai implementasi transformasi yang sama.

## Manual, PowerShell

```powershell
docker compose --env-file .env.airflow -f compose.yaml exec -T airflow python jalankan.py batch
```

Satu tahap untuk sumber yang sudah ada:

```powershell
docker compose --env-file .env.airflow -f compose.yaml exec -T airflow python jalankan.py batch --task transform_spark --run-id SOURCE_RUN_ID
```

Raw harus sudah diunggah. Hasil Spark memeriksa hash sumber, tanggal, USD, kurs positif dan key unik; tidak mengimputasi tanggal hilang.

## Otomatis, Airflow

Buka [jisdor_daily](http://localhost:8085/dags/jisdor_daily), aktifkan DAG. Jadwal 18.00 WIB selama layanan aktif. Trigger manual scheduler:

```powershell
docker compose --env-file .env.airflow -f compose.yaml exec -T airflow airflow dags unpause jisdor_daily
docker compose --env-file .env.airflow -f compose.yaml exec -T airflow airflow dags trigger jisdor_daily
```

## Uji error terkontrol

```powershell
$alertRunId = 'demo_fail_collect_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
docker compose --env-file .env.airflow -f compose.yaml exec -T airflow airflow dags trigger jisdor_daily --run-id $alertRunId
```

Task collect sengaja gagal sebelum ekstraksi dan tanpa retry, hanya pada run manual dengan prefix ini. Buka [Mailpit](http://localhost:8025), cocokkan run ID pada email `[FAILED] jisdor_daily.collect`. Run normal tidak memakai prefix tersebut. Tidak diperlukan DAG error terpisah.

## Hasil warehouse

[BigQuery fact JISDOR](https://console.cloud.google.com/bigquery?project=jcdeah-009&p=jcdeah-009&d=daud_finalproject&t=fact_jisdor_daily&page=table).

```sql
SELECT * FROM `jcdeah-009.daud_finalproject.fact_jisdor_daily`
ORDER BY jisdor_date DESC LIMIT 10;

SELECT jisdor_date, currency, COUNT(*) jumlah
FROM `jcdeah-009.daud_finalproject.fact_jisdor_daily`
GROUP BY 1, 2 HAVING COUNT(*) > 1;
```

Query duplikasi harus kosong. Input lebih baru memperbarui key yang sama; tanggal baru menambah baris. Staging kedaluwarsa setelah satu hari. Report teknis berada di logs/bigquery_batch_*.json; bukan progress.txt. Source run ID memakai UTC dan tidak sama dengan tanggal kurs.
