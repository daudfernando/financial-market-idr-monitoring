# Batch JISDOR ke GCS

## Status tahap ini

Script `scripts/upload_jisdor_gcs.py` telah diuji lokal dan dijalankan terhadap GCS. Sampel `20260909T214210877646Z` menghasilkan 16 objek berukuran total 15.330 byte. Semua objek berhasil diunggah, lalu rerun memverifikasi isi identik dan melewati seluruh objek.

Bucket `jcdeah-009-daud-finalproject` dibuat di project `jcdeah-009` pada region `asia-southeast2`, kelas Standard. Uniform bucket-level access aktif dan public access prevention enforced. Nama disesuaikan menjadi huruf kecil dengan prefix project agar lebih mudah memperoleh nama unik global.

Login pengguna ADC lokal berhasil digunakan untuk membuat bucket dan mengunggah objek. Environment `GOOGLE_APPLICATION_CREDENTIALS` menunjuk credential project lain, sehingga flag `--use-local-adc` memilih file ADC pengguna secara eksplisit tanpa mengubah environment komputer. Secret tidak dicetak atau disalin ke repository. Perintah `gcloud` belum ditemukan pada PATH; script memakai library Python.

## Yang dilakukan script

1. Membaca run sampel yang sudah lolos validasi JISDOR.
2. Memeriksa ulang XML asli, jumlah record, tanggal, mata uang, nilai kurs, dan kesesuaian seluruh partisi processed dengan sumber.
3. Menyiapkan nama objek dan SHA-256 untuk setiap file.
4. Pada mode upload, menulis objek ke bucket yang sudah ada dengan prasyarat objek belum ada.
5. Jika objek sudah ada, membandingkan isinya. Isi yang sama dilewati; perbedaan isi menyebabkan kegagalan tanpa overwrite.
6. Menulis manifest terakhir setelah seluruh file berhasil diunggah atau diverifikasi identik.

Struktur objek yang direncanakan adalah berikut.

```text
final-project/raw/jisdor/ingestion_date=YYYY-MM-DD/run_id=.../response.xml
final-project/processed/jisdor/date=YYYY-MM-DD/run_id=.../record.json
final-project/manifests/jisdor/run_id=.../manifest.json
```

Manifest berisi daftar objek dan hash untuk batch yang lengkap. Job downstream harus mengonsumsi run melalui manifest, agar file dari upload yang terputus tidak dianggap satu batch selesai. Rerun dengan run ID dan isi yang sama tidak menambah objek. Run ingestion baru mempunyai versi raw sendiri; deduplikasi antar-run di warehouse akan dibangun pada tahap berikutnya.

Tahap ini mengunggah hasil transformasi Python dari eksplorasi lokal. Pemrosesan PySpark, penjadwalan Airflow, load BigQuery, dan alert eksternal masih merupakan tahap selanjutnya.

## Konfigurasi yang diperlukan

- Project ID GCP yang digunakan untuk final project.
- Nama bucket GCS yang sudah tersedia pada project tersebut.
- Credential lokal yang mempunyai izin membuat serta membaca objek dalam bucket itu.
- Jika bucket belum tersedia, tentukan nama dan region terlebih dahulu sebelum membuatnya. Region tidak diubah oleh script upload ini.

Gunakan credential lokal yang memang ditujukan untuk project tersebut. Application Default Credentials memeriksa `GOOGLE_APPLICATION_CREDENTIALS` sebelum file ADC standar. Jangan mengganti credential yang ada tanpa memahami penggunaannya. [Cara kerja ADC](https://docs.cloud.google.com/docs/authentication/application-default-credentials)

File `.env.example` merupakan template. Script membaca environment variable shell atau argumen CLI; file `.env` tidak dimuat otomatis. Credential dan isi secret tidak perlu dikirim melalui chat.

## Cara menjalankan

Dry-run tidak membutuhkan credential dan tidak memanggil GCS.

```powershell
.\.venv\Scripts\python.exe scripts\upload_jisdor_gcs.py --source-dir data\source_samples\20260909T214210877646Z
```

Setelah project dan bucket ditentukan, gunakan nama sebenarnya pada perintah berikut.

```powershell
$env:GOOGLE_CLOUD_PROJECT = 'jcdeah-009'
$env:GCS_BUCKET = 'jcdeah-009-daud-finalproject'
.\.venv\Scripts\python.exe scripts\upload_jisdor_gcs.py --source-dir data\source_samples\20260909T214210877646Z --use-local-adc --upload
```

Script memakai library Google Cloud Storage dan ADC lingkungan. Upload menggunakan `if_generation_match=0` serta verifikasi checksum CRC32C. Rerun memeriksa bytes objek pada generation yang dibaca sebelum menyatakan file identik. [Contoh upload resmi Google Cloud](https://docs.cloud.google.com/storage/docs/samples/storage-upload-file)

Hasil disimpan di `logs/gcs_batch_<timestamp>.json` dan ringkasan ditambahkan ke `logs/progress.txt`. Exit code 1 berarti validasi, konfigurasi, atau upload gagal. Log lokal ini belum merupakan alert eksternal sesuai requirement final project.

## Pengujian

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Empat tes menggunakan mock, mencakup objek baru, rerun identik, penolakan konflik isi, dan propagasi kegagalan izin. Pengujian integrasi juga berhasil untuk pembuatan bucket, upload 16 objek, dan rerun yang memverifikasi bytes seluruh objek. Simulasi putus jaringan pada upload belum dijalankan.

Setup bucket dapat dijalankan kembali untuk memeriksa bucket yang sama dalam project ini.

```powershell
.\.venv\Scripts\python.exe scripts\setup_gcs_bucket.py --project jcdeah-009 --bucket jcdeah-009-daud-finalproject --use-local-adc
```

Script setup tidak mengubah konfigurasi bucket yang sudah ada. Metadata non-secret disimpan di `config/gcp.json`. Tahap berikutnya adalah pemrosesan data lake menuju BigQuery serta penjadwalan Airflow.
