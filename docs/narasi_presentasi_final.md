# Narasi final dan urutan demo

Durasi: 35 menit materi/demo + 5 menit Q&A. Angka memakai snapshot terverifikasi 19 September 2026; bila refresh data, perbarui angka slide 2, 7, dan 9. Slide tidak perlu memuat semua narasi ini.

## Persiapan sebelum sesi

PowerShell, Docker Desktop aktif:

```powershell
Set-Location "C:\Purwadhika\Final Project"
.\prepare_demo.cmd
.\dbt_docs.cmd
```

Jalankan persiapan sebelumnya, bukan saat membuka presentasi. Siapkan tab [Airflow](http://localhost:8085), [dbt Docs](http://localhost:8086), [BigQuery](https://console.cloud.google.com/bigquery?project=jcdeah-009), [Mailpit](http://localhost:8025), dan dashboard. Link localhost hanya untuk laptop demo; GCP memerlukan akun berizin. Jangan tampilkan password/credential.

URL Looker Studio: belum diberikan. Gunakan report milik pengguna jika sudah siap; cadangan HTML dibuka dengan `Start-Process .\dashboard\index.html`. HTML tidak di-host sebagai GitHub Pages. Data snapshot lokal tidak ikut repository publik.

## 1. Akar masalah — 3 menit

“Harga saham dalam dolar belum cukup untuk menjelaskan nilainya dalam rupiah. Harga dan kurs berasal dari sumber berbeda, dengan waktu pembaruan berbeda. Kalau tanggalnya digabung sembarangan, angka yang terlihat bisa menyesatkan. Proyek ini menyatukan keduanya dan membuat hasilnya bisa ditelusuri.”

Transisi: “Siapa yang membutuhkan informasi ini, dan keputusan apa yang dibantu?”

## 2. Business context — 3 menit

“Pengguna yang saya bayangkan adalah analis treasury atau market risk. Mereka perlu melihat nilai indikatif rupiah, membandingkan saham, dan menjelaskan faktor perubahannya. Lingkup saya empat saham finansial, 7.797 bar menit dan 30 tanggal JISDOR. Ini skenario pembelajaran dengan quantity satu saham, bukan portofolio institusi nyata.”

“Manfaat yang dituju adalah mengurangi penggabungan manual dan memudahkan review. Saya belum mengukur penghematan waktu atau keuntungan investasi.”

## 3. Arsitektur — 3 menit

“Ada dua jalur. Kurs dijadwalkan Airflow dan dibersihkan Spark. Harga historis diputar ulang melalui Kafka; consumer Python memvalidasinya. Raw dan hasilnya disimpan di GCS, data warehouse menggunakan BigQuery, dan dbt menyiapkan mart untuk Looker.”

“Kafka menampung event; Python memprosesnya. Docker membuat layanan mudah dijalankan. Dataflow tidak diperlukan pada jalur final ini.”

Catatan penting: demo per pesan menulis tabel terpisah. Mart dashboard dibuat dari replay snapshot lengkap melalui load GCS. Jangan menghubungkan demo 40 pesan ke chart utama seolah-olah chart tersebut live.

## 4. Data modelling — 3 menit

“Saya memetakan pengolahan dengan Medallion Architecture. Bronze berisi raw untuk audit. Silver berisi kurs dan harga yang sudah standar. Gold berisi nilai rupiah, return, sinyal, serta mart dashboard.”

“Satu baris kurs mewakili tanggal dan mata uang; satu baris mart harga mewakili saham, mode, tipe data, dan menit. Ini pembagian layer logis, bukan star schema lengkap.”

Tampilkan [diagram Bronze–Silver–Gold](bronze_silver_gold.svg). Detail tabel hanya dibuka jika ditanya.

## 5. Streaming terlebih dahulu — 7 menit termasuk demo

“Saya perlihatkan pesan historis mengalir satu per satu. Waktu harga asli tetap disimpan, sehingga replay tidak dianggap harga terbaru dari pasar.”

PowerShell pertama:

```powershell
.\run_streaming_demo.cmd --limit 40 --interval 2
```

Setelah minimal tiga pesan terkirim, PowerShell kedua:

```powershell
.\show_kafka.cmd
```

Sorot KIRIM, TERIMA, BIGQUERY; lalu topic, partition dan offset. “Pembaca terpisah ini membaca ulang pesan dari Kafka, bukan dari file atau BigQuery.”

Buka [view demo streaming](https://console.cloud.google.com/bigquery?project=jcdeah-009&p=jcdeah-009&d=daud_finalproject_demo&t=demo_streaming_events&page=table). Jalankan berulang saat replay:

```sql
SELECT demo_run_id, COUNT(*) AS pesan_terlihat
FROM `jcdeah-009.daud_finalproject_demo.demo_streaming_events`
GROUP BY demo_run_id ORDER BY demo_run_id DESC LIMIT 3;
```

“Jumlah bertambah sebelum replay selesai. Setiap run baru disimpan terpisah melalui demo_run_id; pembacaan dideduplikasi dalam run. Ini bukan klaim exactly-once lintas semua komponen.”

```sql
SELECT demo_run_id, kafka_offset, symbol, price_usd,
       event_timestamp, replayed_at, consumer_received_at
FROM `jcdeah-009.daud_finalproject_demo.demo_streaming_events`
ORDER BY consumer_received_at DESC LIMIT 8;
```

Transisi: “Harga dapat dikirim sebagai event. Kurs memiliki kebutuhan berbeda, yaitu pembaruan batch harian.”

## 6. Batch Airflow dan rerun — 6 menit termasuk demo

Buka [Airflow](http://localhost:8085), DAG `jisdor_daily`. Tunjukkan run sukses dan task: collect → upload_raw → transform_spark → upload → warehouse → check_dates.

“Batch mengambil interval 45 hari terakhir, sehingga tanggal yang terlewat di dalam window bisa dipulihkan. Spark menormalkan format. BigQuery memakai MERGE tanggal dan mata uang, sehingga rerun tidak menambah baris untuk key yang sama.”

Jalankan sebelum Trigger DAG, lalu jalankan lagi sesudah success:

```sql
SELECT jisdor_date, currency, COUNT(*) AS jumlah_baris,
       MAX(usd_idr) AS kurs, MAX(ingested_at) AS ingestion_terbaru
FROM `jcdeah-009.daud_finalproject.fact_jisdor_daily`
GROUP BY jisdor_date,currency ORDER BY jisdor_date DESC;
```

“Yang tetap satu adalah baris per tanggal/mata uang. Metadata ingestion boleh berubah; jika BI memiliki tanggal baru, tanggal tersebut memang ditambahkan. Hanya task warehouse yang memuat fact BigQuery.”

Jika run lebih lama, lanjutkan slide 7 sambil menunggu dan kembali ke hasil; jangan berhenti bicara. Jadwal 18:00 WIB hanya bekerja saat layanan hidup dan DAG aktif.

## 7. Cleaning dan kualitas — 3 menit

“Pada data BI ada mata uang USD dengan spasi di belakang; Spark membuang spasi, menstandarkan tanggal dan mengubah kurs teks menjadi angka. Waktu saham diseragamkan ke UTC. dbt melakukan deduplikasi dan memberi status ketika data belum cukup.”

“Harga asli tidak saya ubah untuk memaksa sinyal. Gap tidak diisi nol. Pada snapshot ini 24 tests dbt lulus, rekonsiliasi tanpa mismatch dan pasangan FX tersedia untuk semua bar.”

Tampilkan hasil build tersimpan bila ditanya. GUI dbt menjelaskan definisi test, bukan bukti eksekusi baru. Bukti lokal: `data/dbt/target/run_results.json` dan `data/dashboard_readiness.json`. Contoh before/after ada di Excel lokal `docs/before_after_data_cleaning.xlsx` (snapshot 17 September, bukan ekspor terbaru).

Jika ditanya alert, buka [Mailpit](http://localhost:8025): inbox pengujian lokal, bukan email eksternal ke mentor.

## 8. dbt GUI lalu BigQuery — 2 menit

Buka [model monitoring](http://localhost:8086/#!/model/model.daud_market.mart_stock_monitoring), klik ikon lineage kanan bawah.

“Model exposure menggabungkan harga dan kurs dengan aturan waktu. Model fitur menghitung SMA3 dan SMA8. Mart final menyajikan hasil beserta alasan sinyal. Saya bisa menelusuri dari kolom dashboard sampai sumbernya.”

Pindah [mart terbaru di BigQuery](https://console.cloud.google.com/bigquery?project=jcdeah-009&p=jcdeah-009&d=daud_finalproject_demo&t=mart_stock_latest&page=table):

```sql
SELECT symbol, minute_utc, exposure_idr, strategy_signal,
       session_return, peer_average_return, peer_rank
FROM `jcdeah-009.daud_finalproject_demo.mart_stock_latest`
ORDER BY peer_rank;
```

Tidak perlu membedah SQL join. “Berikutnya saya tunjukkan hasil yang dipakai analis.”

## 9. Insight dashboard — 4 menit

“Pada akhir sesi 18 September, return terhadap observasi pertama sesi: JPM sekitar +0,55%, MS +0,05%, BAC -0,59%, dan GS -0,78%. JPM berada di peringkat tertinggi kelompok snapshot ini. Perbandingan menggunakan persentase, bukan harga nominal saham.”

“Saya pilih satu saham untuk melihat harga USD, nilai rupiah, serta faktor perubahannya. Lalu saya periksa sinyal dan kualitas. Return positif tidak otomatis BUY: sinyal memerlukan persilangan baru SMA3/SMA8.”

Untuk chart kontribusi: perubahan dihitung antar-menit berurutan dalam sesi; gap dan overnight tidak termasuk. Efek FX boleh nol karena JISDOR harian. Jangan menyebutnya profit portofolio.

Dashboard memakai histori snapshot lengkap. Demo 40 pesan sebelumnya membuktikan ingestion streaming, bukan menyegarkan chart ini secara otomatis. Angka slide harus diperbarui setelah prepare_demo berikutnya.

## 10. Penutup — 1 menit

“Hasil proyek ini adalah jalur batch dan replay yang teruji, data analitik yang bisa ditelusuri, serta dashboard untuk memahami harga dan nilai rupiah. Batasnya jelas: replay historis dan aturan simulasi, belum live market feed atau backtest keuntungan.”

Repository publik: https://github.com/daudfernando/financial-market-idr-monitoring

## Q&A — 5 menit

- Kafka untuk apa? Antrean/log event yang memisahkan pengirim dan consumer; dapat dibaca ulang selama retensi.
- Mengapa berhenti 40 pesan? Batas demo agar cepat; bukan keterbatasan Kafka atau Spark.
- Apakah rerun duplikat? Batch MERGE per tanggal/mata uang; demo streaming memakai run baru dan dedup per run/event. Keduanya berbeda.
- Mengapa NO_SIGNAL? Window SMA saat ini/sebelumnya belum lengkap, menit tidak berurutan, atau FX kosong.
- Apakah tanggal kosong error? Belum tentu: weekend, libur, belum publikasi. Task check_dates membandingkan tanggal yang benar-benar diberikan BI.
- Mengapa tidak Dataflow? Tidak dibutuhkan pada implementasi final yang memakai Spark dan Kafka/Python; pilihan guideline tidak mewajibkan seluruh tools.

Sumber untuk ditampilkan bila diminta: [web service BI](https://www.bi.go.id/biwebservice/wskursbi.asmx?op=getSubKursJisdor3), [Yahoo JPM](https://finance.yahoo.com/quote/JPM/). Sumber harga yang diputar ulang adalah hasil unduhan historis tersimpan, bukan halaman quote saat demo.
