# Streaming replay, Kafka dan ingestion Google Cloud

Pipeline ini mengirim ulang histori asli saham JPM, BAC, GS, dan MS satu per satu. Sumber diunduh dengan yfinance, kemudian dipilih dari kandidat lokal aktif. Waktu harga tetap mengikuti sumber; waktu replay mengikuti saat command dijalankan. Ini bukan harga pasar live.

## Alur per pesan

| Tahap | Proses dan bukti |
|---|---|
| Sumber | Kandidat aktif ditunjuk oleh `config/current_market_replay.json`, field `input`. Command memilih N bar terbaru, lalu mengirim secara kronologis. |
| Producer Python | Membentuk event dan mengirim ke topic Kafka lokal. Terminal menampilkan `[KIRIM]` setelah acknowledgement broker. |
| Kafka | Menyimpan pesan pada topic `market.incremental.<run_id huruf kecil>`, satu partition, dengan offset berurutan. |
| Consumer Python | Membaca pesan dan memvalidasi schema, symbol, USD, harga positif/finite, timestamp dan event ID. Terminal menampilkan `[TERIMA offset=...]`. |
| GCS | Menyimpan satu JSON raw per offset, sebelum insert BigQuery. Payload pesan tersimpan sebagai `value_base64`, disertai metadata Kafka. |
| BigQuery | `insert_rows_json` memasukkan satu event per request ke tabel `daud_finalproject.demo_streaming_events`. Terminal menampilkan `[BIGQUERY]` saat API menerima. |
| Verifikasi | SELECT saat run dan rekonsiliasi akhir membuktikan baris dapat dibaca. View `daud_finalproject_demo.demo_streaming_events` melakukan deduplikasi per run dan event. |

Kafka dan consumer berjalan di Docker lokal. GCS dan BigQuery berada di Google Cloud project `jcdeah-009`. Tidak memakai Dataflow pada jalur ini.

## 1. Persiapan

Gunakan Windows PowerShell, Docker Desktop aktif, setup README sudah selesai, credential ADC memiliki akses GCS/BigQuery, dan tabel snapshot awal tersedia. Untuk setup data lengkap, jalankan sebelumnya:

```powershell
Set-Location "C:\Purwadhika\Final Project"
.\prepare_demo.cmd
```

Command ini memperbarui kandidat, menjalankan batch, dan membangun mart. Untuk hanya mengambil kandidat saham terbaru pada lingkungan yang sudah siap:

```powershell
.\prepare_demo.cmd --stocks-only
```

Mode stocks-only belum mengubah BigQuery. Tanggal terakhir mengikuti data yang tersedia di sumber, bukan otomatis tanggal kalender hari ini.

## 2. Jalankan replay, terminal pertama

```powershell
.\run_streaming_demo.cmd --limit 40 --interval 2
```

Command menyalakan Kafka, membuat topic khusus run, lalu mengirim 40 bar terbaru. Interval 2 detik mengatur jeda producer; durasi total juga tergantung sink cloud. Producer dapat lebih cepat daripada consumer sehingga output KIRIM dan TERIMA tidak selalu bergantian.

Perhatikan urutan bukti berikut:

1. Salin `RUN_ID` dan lihat `SUMBER TERPILIH` untuk rentang waktu harga.
2. `[KIRIM]` menunjukkan pesan diterima broker.
3. `[TERIMA offset=...]` menunjukkan consumer membaca dan memvalidasi pesan.
4. `[BIGQUERY .../40]` menunjukkan insert diterima API, bukan hasil query.
5. `[CEK SAAT RUN]` menunjukkan jumlah yang sudah terlihat melalui SELECT sebelum consumer selesai.
6. `SUKSES: 40 pesan diverifikasi` menunjukkan rekonsiliasi akhir lulus. Jika error, jangan menganggap run lengkap.

## 3. Lihat pesan langsung di Kafka, terminal kedua

Setelah minimal tiga pesan terkirim:

```powershell
Set-Location "C:\Purwadhika\Final Project"
.\show_kafka.cmd
```

Output memperlihatkan topic, partition, serta tiga pesan dari broker beserta key dan offset. Ini pembaca terpisah, bukan membaca file lokal dan bukan mengirim ulang ke BigQuery. Default memilih topic incremental terbaru. Jika ada beberapa run, gunakan nama topic yang sesuai:

```powershell
.\show_kafka.cmd --topic NAMA_TOPIC
```

## 4. Lihat pertambahan data di BigQuery

Buka [BigQuery editor](https://console.cloud.google.com/bigquery?project=jcdeah-009). Ganti `RUN_ID_DARI_TERMINAL` dengan run yang sedang berjalan, lalu jalankan query berikut berulang saat consumer aktif:

```sql
SELECT demo_run_id, COUNT(*) AS pesan_terlihat,
       MAX(sink_requested_at) AS insert_terakhir
FROM `jcdeah-009.daud_finalproject_demo.demo_streaming_events`
WHERE demo_run_id = 'RUN_ID_DARI_TERMINAL'
GROUP BY demo_run_id;
```

Hasil belum ada jika pesan pertama belum terlihat. Klik Run lagi untuk memperbarui hasil; editor bukan tampilan yang otomatis menyegarkan. Jumlah bisa meloncat beberapa baris karena query lebih lambat daripada ingestion. Jika hasil tampak tertahan, nonaktifkan cached results di Query settings.

Untuk melihat setiap pesan:

```sql
SELECT kafka_offset, symbol, price_usd,
       event_timestamp AS waktu_harga_sumber,
       replayed_at AS waktu_replay,
       consumer_received_at, sink_requested_at, event_id
FROM `jcdeah-009.daud_finalproject_demo.demo_streaming_events`
WHERE demo_run_id = 'RUN_ID_DARI_TERMINAL'
ORDER BY kafka_offset;
```

`event_timestamp` adalah waktu bar historis, sedangkan tiga timestamp berikutnya menunjukkan proses replay sekarang. `sink_requested_at` adalah waktu permintaan insert, bukan timestamp commit server BigQuery. Setelah sukses, query count seharusnya menunjukkan 40 untuk run ini.

Akses objek: [tabel fisik ingestion](https://console.cloud.google.com/bigquery?project=jcdeah-009&p=jcdeah-009&d=daud_finalproject&t=demo_streaming_events&page=table), [view hasil deduplikasi](https://console.cloud.google.com/bigquery?project=jcdeah-009&p=jcdeah-009&d=daud_finalproject_demo&t=demo_streaming_events&page=table).

## 5. Lihat raw per pesan di GCS

Buka [folder incremental GCS](https://console.cloud.google.com/storage/browser/jcdeah-009-daud-finalproject/final-project/demo/incremental?project=jcdeah-009), pilih folder RUN_ID, lalu `raw/`.

File `000000.json`, `000001.json`, dan seterusnya mengikuti offset Kafka. Refresh daftar objek ketika run masih berjalan. Setiap file menyimpan pesan mentah dalam base64 dan topic/partition/offset, untuk ditelusuri ke baris BigQuery. Tidak ada log GCS terpisah di terminal; keberadaan objek menjadi bukti upload. Link cloud memerlukan akun berizin.

## 6. Hasil akhir dan hubungan ke dashboard

Laporan lokal: `data/incremental_streaming/RUN_ID/report.json`. Periksa `status=passed`, `producer_sent=40`, `bq_accepted=40`, `bq_visible=40`, dan `mismatches=0`. Detail baris lokal ada di `events.jsonl` pada folder yang sama.

Setiap run baru menambah data dengan demo_run_id berbeda. View mencegah duplikasi event dalam run yang sama; insertId bersifat best effort. Jangan memakai jumlah seluruh run sebagai jumlah pesan satu run.

Mode per pesan ini memakai tabel terpisah dan tidak otomatis memperbarui mart dashboard. Untuk memperbarui mart dari seluruh kandidat aktif:

```powershell
.\run_demo.cmd
```

Jalur snapshot lengkap: replay Kafka, validasi consumer, upload raw/valid ke GCS, load staging BigQuery, dbt build, ekspor dashboard, audit. Mart `mart_stock_monitoring` dan `mart_stock_latest` menjadi sumber dashboard. Lihat [transformasi dan mart analitik](demo_analytics.md).

## File implementasi

- [Collector histori](../scripts/collect_market_universe.py), mengambil data sumber dan kandidat.
- [Launcher per pesan](../run_streaming_demo.cmd), menjalankan layanan dan container.
- [Pipeline per pesan](../streaming/incremental_demo.py), producer, consumer, GCS, insert dan rekonsiliasi.
- [Kontrak event](../streaming/events.py), aturan validasi dan identitas event.
- [Pembaca Kafka](../scripts/show_kafka.py), bukti pesan tersimpan di broker.
- [Pipeline snapshot lengkap](../scripts/run_demo.py), publication hingga mart dashboard.
