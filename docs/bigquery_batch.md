# Batch JISDOR dari GCS ke BigQuery

## Hasil tahap ini

- Project `jcdeah-009`.
- Dataset `daud_finalproject`, region `asia-southeast2` agar sama dengan bucket.
- Tabel `fact_jisdor_daily`, 14 record tanggal 20 Agustus sampai 9 September 2026.
- Pemuatan awal memengaruhi 14 baris; rerun memengaruhi 0 baris.
- Pemeriksaan akhir menemukan 0 duplikasi, 0 record invalid, dan 0 selisih terhadap batch sumber.

Bukti eksekusi terdapat di `logs/bigquery_batch_20260909T220724849008Z.json` dan `logs/bigquery_batch_20260909T220750246691Z.json`. Log memuat job ID BigQuery, tabel staging, jumlah baris, dan hasil pemeriksaan.

## Alur proses

1. Baca manifest batch lengkap dari GCS.
2. Unduh objek yang tercantum dan cocokkan ukuran serta SHA-256 dengan manifest.
3. Periksa tipe tanggal, currency USD, rate positif, timezone timestamp, dan business key unik.
4. Buat dataset serta tabel proyek jika belum tersedia. Schema atau region existing yang berbeda menyebabkan proses berhenti.
5. Buat staging terpisah per eksekusi, dengan expiration satu hari, lalu muat record melalui BigQuery load job.
6. Jalankan `sql/merge_jisdor.sql` berdasarkan `jisdor_date` dan `currency`.
7. Periksa jumlah baris, duplikasi, nilai invalid, dan kesesuaian baris target dengan batch.

Dataset dan tabel pada project lain tidak diubah. Credential dipilih melalui `--use-local-adc` tanpa mengganti environment credential komputer.

## Schema dan alasan pemilihan

| Kolom | Tipe BigQuery | Keterangan |
|---|---|---|
| jisdor_date | DATE, required | Tanggal referensi kurs; partition key |
| currency | STRING, required | USD; clustering key |
| usd_idr | NUMERIC, required | Kurs desimal, tanpa penyimpanan floating-point di warehouse |
| source | STRING, required | Endpoint sumber |
| ingested_at | TIMESTAMP, required | Waktu ingestion sumber, dipertahankan saat rerun |
| fx_available_at | TIMESTAMP, nullable | Tetap null ketika waktu publikasi belum terverifikasi |
| source_run_id | STRING, required | Run sumber GCS untuk keterlacakan |

Grain tabel adalah satu tanggal referensi dan mata uang. Jika tanggal sudah ada, hanya record dengan ingestion sumber lebih baru yang memperbarui target. Rerun data identik tidak mengubah baris. Versi historis sumber tetap berada di GCS. Tabel fact ini menyimpan versi terbaru dan belum mencukupi kebutuhan historical as-of untuk revisi kurs; logika tersebut harus mempertimbangkan versi dan waktu ketersediaan sebelum mart exposure dibangun.

Staging baru pada rerun bersifat sementara dan otomatis kedaluwarsa. Jaminan idempotensi tahap ini berlaku untuk hasil tabel fact pada eksekusi berurutan. Airflow nanti perlu membatasi run batch yang tumpang tindih. Query dibatasi `maximum_bytes_billed` 100 MiB per query untuk skala sampel ini.

## Command yang digunakan

Instalasi dependency dan pencatatan versi lingkungan.

```powershell
.\.venv\Scripts\python.exe -m pip install google-cloud-bigquery
.\.venv\Scripts\python.exe -m pip freeze | Set-Content -LiteralPath requirements.txt -Encoding UTF8
```

Pengujian lokal.

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Pemuatan dan uji rerun memakai command yang sama dari root workspace.

```powershell
.\.venv\Scripts\python.exe scripts\load_jisdor_bigquery.py --project jcdeah-009 --bucket jcdeah-009-daud-finalproject --run-id 20260909T214210877646Z --use-local-adc
```

Script mencatat command sebelum bekerja, lalu menambahkan hasil dan lokasi report ke `logs/progress.txt`. Argumen script hanya berisi konfigurasi non-secret. Jika suatu proses gagal, exit code 1 dikembalikan dan error dicatat. Staging yang dibuat sebelum kegagalan tetap tersedia untuk diagnosis sampai expiration.

## Batas implementasi

Tahap ini merupakan load batch dari processed GCS melalui Python menuju warehouse. PySpark, orkestrasi Airflow, alert eksternal, dbt, dan mart exposure belum dijalankan. Log lokal belum menggantikan alert kegagalan yang diminta guideline.

Pemeriksaan integrasi memakai sampel nyata 14 record. Pengujian perubahan kurs revisi, concurrency, dan putus jaringan belum dilakukan. Dataset saham yang baru tersedia masih historis; belum ada event live JPM yang tervalidasi.

## Referensi

- [BigQuery schema](https://docs.cloud.google.com/bigquery/docs/schemas).
- [BigQuery MERGE](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax).
