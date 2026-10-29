# Standar pengembangan

## Kontrak yang dipakai bersama

| Tanggung jawab | Modul |
|---|---|
| Urutan dan parameter task | `config/pipeline_kurs.yaml` |
| Eksekusi manual dan otomatis | `operator_pipeline/tahapan_kurs.py` |
| Adapter Airflow | `operator_pipeline/operator_kurs.py` |
| Load JSON dan file GCS ke BigQuery | `operator_pipeline/muat_bigquery.py` |
| Upload GCS idempotent | `operator_pipeline/simpan_gcs.py` |
| Credential Google Cloud | `operator_pipeline/koneksi_google.py` |
| Failure callback | `operator_pipeline/kirim_alert.py` |

Fungsi pemuatan menerima client, sumber, tabel tujuan dan schema. Pengguna fungsi tidak menulis ulang konfigurasi load. Load memeriksa jumlah baris; insert event memeriksa error API. Modul bisnis menambahkan aturan khusus seperti MERGE kurs dan rekonsiliasi saham.

`operator_pipeline` sengaja tidak dinamai `operator`, karena nama tersebut sudah dipakai standard library Python. DAG hanya membuat instance OperatorKurs dari YAML. Setiap task menjalankan modul melalui subprocess terpisah dengan timeout dan propagasi exit code. Jalur manual memakai pembaca YAML serta runner yang sama.

## Menambah use case

1. Tetapkan sumber, grain, key, tipe data, aturan validasi dan output bersama tim.
2. Gunakan fungsi operator yang tersedia. Tambahkan modul dalam folder proses hanya jika aturan bisnisnya berbeda.
3. Daftarkan command pada `daftar_proses.py`; YAML hanya menerima proses terdaftar, bukan import arbitrer.
4. Tambahkan task ID, parameter dan dependensi. Dependensi harus muncul lebih dulu; duplikasi atau siklus ditolak.
5. Tambahkan test untuk input salah, sink gagal, rerun dan rekonsiliasi. Perubahan kontrak harus melewati review.
6. Untuk tabel baru, tulis schema/keys sebelum menjalankan load. Jangan mengubah sumber API menjadi PostgreSQL tanpa implementasi ekstraksi dan pengujiannya.

Satu file memiliki satu tanggung jawab yang jelas. Nama modul berbahasa Indonesia, nama kontrak warehouse dipertahankan agar tidak memutus konsumen. Penjelasan desain berada di dokumentasi, bukan komentar berulang dalam kode.

## Error dan retry

Kesalahan validasi/exit code menghentikan pipeline. Airflow mengulang task normal maksimal dua kali dengan backoff, lalu callback mengirim identitas DAG/task/run ke Mailpit. Task downstream tidak menulis data bila upstream gagal. Run manual terminal berhenti dan mengembalikan error; jalur replay lengkap juga mencoba mengirim alert melalui Airflow lokal.

BigQuery MERGE memakai tanggal dan mata uang. Upload GCS memakai generation precondition dan perbandingan hash. Identitas event stabil; view demo menghapus duplikasi per run/event. Ini bukan jaminan transaksi exactly-once lintas Kafka, GCS dan BigQuery.

## Kolaborasi dan penggunaan AI

AI membantu menulis atau merapikan kode, tetapi kontrak data, modul bersama, hasil test dan review manusia menjadi acuan. Perubahan baru harus mengikuti pola yang sudah disepakati, bukan membuat jalur berbeda untuk proses yang sama. Jangan memasukkan credential atau kode perusahaan yang tidak berizin ke prompt/repository publik.

Pembaca bisnis cukup memakai definisi sumber, cakupan, tanggal dan mart. Detail operator dan implementasi ditujukan untuk maintainer. Source code perusahaan tetap tunduk pada kepemilikan dan kebijakan perusahaan; repository ini adalah proyek portofolio pribadi.
