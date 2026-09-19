# Streaming Kafka dan Dataflow

## Pembagian tugas

```text
Yahoo Finance / historical replay
  -> Python producer -> Kafka topic
  -> Apache Beam di Dataflow (target cloud)
       -> GCS raw (payload + topic/partition/offset)
       -> validasi -> BigQuery staging -> view deduplikasi
       -> data invalid / error permanen BigQuery -> GCS deadletter
```

Kafka menampung event dengan offset untuk dibaca ulang. Beam mendefinisikan pemrosesannya; Dataflow menjalankan pipeline Beam di GCP. Spark tetap menangani batch JISDOR. Airflow tidak menjalankan producer streaming sebagai task harian.

## Yang sudah diuji

- Kafka 3.9.1 berjalan lokal di Docker, satu broker KRaft dengan volume persisten. Topic replay dan live dibuat terpisah.
- 100 bar JPM satu menit dari sampel Yahoo asli berhasil dikirim ke topic `market.prices.replay.v1`.
- Pembacaan 100 pesan Kafka dan validasi Beam DirectRunner berhasil: 100 valid, 0 invalid, 100 event ID unik. Bukti: `data/streaming/20260916T120213524464Z/report.json`.
- Lima test kontrak event dan satu test integrasi Beam lulus. Record JSON rusak dan UTF-8 invalid masuk deadletter, bukan hilang diam-diam.
- Rerun producer menghasilkan total 200 pesan valid dengan 100 ID unik: `data/streaming/20260916T120512672133Z/report.json`. Ini menunjukkan duplikasi antar-replay teridentifikasi; smoke test tidak menghapus pesan duplikat.
- Native KafkaIO expansion dan serialisasi graph cloud lulus (`75` transform); tidak ada job Dataflow yang disubmit.
- Tabel kosong `jcdeah-009.daud_finalproject.stg_market_events` dan view `market_events_deduplicated` sudah dibuat di BigQuery, region `asia-southeast2`. Belum ada event yang dimuat ke tabel ini.
- Probe WebSocket belum menghasilkan event valid. Pada percobaan berikutnya terjadi kegagalan DNS dalam container. Keberhasilan replay tidak membuktikan sumber live sudah berfungsi.

Uji lokal menggunakan consumer Kafka terbatas untuk menangkap pesan, lalu `beam.Create` dan DirectRunner. Ini menguji transport Kafka dan transformasi Beam, tetapi **bukan job Dataflow atau uji runtime native KafkaIO**. Pipeline cloud memakai `ReadFromKafka` native; worker membutuhkan konektivitas ke broker.

## Menjalankan manual di CMD

Gunakan CMD baru agar environment Compose hanya berlaku untuk sesi streaming ini.

```bat
cd /d "C:\Purwadhika\Final Project"
set COMPOSE_FILE=compose.yaml;compose.streaming.yaml
set COMPOSE_PROFILES=streaming
docker compose --env-file .env.airflow build market
docker compose --env-file .env.airflow up -d kafka
docker compose --env-file .env.airflow ps
```

Tunggu Kafka berstatus healthy, lalu buat topic (aman dijalankan ulang):

```bat
docker compose --env-file .env.airflow exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic market.prices.replay.v1 --partitions 1 --replication-factor 1
docker compose --env-file .env.airflow exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic market.prices.live.v1 --partitions 1 --replication-factor 1
```

Kirim data historis asli sebagai replay dan periksa hasil:

```bat
docker compose --env-file .env.airflow run --rm market producer.py --mode replay --input /workspace/data/source_samples/20260909T214210877646Z/jpm_replay_candidates.jsonl --limit 100 --interval 0.1
docker compose --env-file .env.airflow run --rm market local_smoke.py --limit 100
docker compose --env-file .env.airflow run --rm market /workspace/tests/test_market_stream.py -v
```

`local_smoke.py` membaca dari offset paling awal, bukan hanya pesan dari pengiriman terakhir. Setiap eksekusi menyimpan capture, output valid/invalid, dan report teknis ke folder baru `data/streaming/<run_id>/`. Topic memiliki retensi 24 jam; jalankan producer lagi jika pesan sudah kedaluwarsa. Rerun producer dapat menambah pesan yang memiliki event ID sama; staging menerima duplikasi dan view menangani deduplikasi.

Untuk mencoba live selama satu menit:

```bat
docker compose --env-file .env.airflow run --rm market producer.py --mode live_websocket --symbols JPM --seconds 60
```

Hanya event dengan simbol, timestamp, harga, dan mata uang USD yang tervalidasi yang diteruskan. Tidak adanya event menghasilkan status `no_events` dan exit nonzero, bukan klaim keberhasilan. Payload yang ditolak disimpan dalam `data/streaming_producer/<run>/rejected.jsonl`. Report teknis producer juga tersimpan di folder tersebut. Pemeriksaan live perlu dilakukan kembali saat koneksi sumber berfungsi dan sumber mengirim pembaruan.

Untuk menghentikan broker lokal:

```bat
docker compose --env-file .env.airflow stop kafka
```

## Kontrak data dan batas jaminan

- `event_timestamp` menyimpan waktu sumber dalam UTC. Pada replay, ini adalah **awal bar historis** dan `price_usd` merupakan close bar satu menit, bukan transaksi individual.
- `ingested_at` mencatat waktu penerimaan producer; `replayed_at` diisi hanya untuk replay. Mode harus ditampilkan pada dashboard kelak.
- `event_id` merupakan hash identitas observasi, stabil untuk replay sumber yang identik. Ini bukan trade ID resmi; update identik dalam timestamp yang sama dianggap observasi yang sama.
- Producer memakai acknowledgement seluruh replica dan idempotent producer. Broker lokal hanya satu replica, sehingga belum memiliki high availability.
- GCS raw menyimpan bytes payload sebagai base64 disertai koordinat Kafka agar JSON rusak tetap bisa diaudit. File cloud memakai window waktu pemrosesan 60 detik; replay lama tidak dibuang karena event date lama.
- BigQuery staging memakai append. View memilih satu baris per event ID; belum ada jaminan exactly-once lintas GCS dan BigQuery. Kedua sink dapat selesai pada waktu berbeda. Offset serta event ID digunakan untuk rekonsiliasi.
- Validasi yang gagal dan kegagalan permanen insert BigQuery memiliki jalur deadletter terpisah. Metrik Beam `market.valid_events` dan `market.invalid_events` disiapkan; alert Cloud Monitoring belum dipasang.

## Persiapan deployment Dataflow berikutnya

Kode di `streaming/beam_pipeline.py` telah disiapkan, tetapi deployment masih membutuhkan broker yang dapat dijangkau worker di GCP. Broker `kafka:19092` dan `localhost:9092` saat ini hanya untuk lokal dan ditolak oleh command submit cloud.

Langkah berikutnya adalah menempatkan Kafka pada jaringan privat GCP atau memakai broker dengan konektivitas yang sesuai, mengonfigurasi advertised listeners, identitas worker, izin GCS/BigQuery/Dataflow, serta akses producer. Jangan membuka port broker lokal ke internet. Pemilihan VM/broker, jaringan, dan durasi job perlu diselesaikan sebelum menjalankan worker berbayar.

Template command berikut **belum dapat dijalankan langsung**: isi alamat broker, service account, dan subnet sesuai deployment yang benar. Worker maksimum satu untuk demo; biaya tetap berjalan sampai job dihentikan.

```bat
docker compose --env-file .env.airflow run --rm market beam_pipeline.py ^
  --bootstrap BROKER_PRIVATE_DNS:9092 ^
  --topic market.prices.replay.v1 --consumer-group daud-replay-v1 ^
  --output-prefix gs://jcdeah-009-daud-finalproject/final-project/market ^
  --table jcdeah-009:daud_finalproject.stg_market_events --run-label UNIQUE_RUN_ID ^
  --runner DataflowRunner --project jcdeah-009 --region asia-southeast2 ^
  --temp_location gs://jcdeah-009-daud-finalproject/final-project/dataflow/temp ^
  --staging_location gs://jcdeah-009-daud-finalproject/final-project/dataflow/staging ^
  --service_account_email WORKER_SERVICE_ACCOUNT ^
  --subnetwork FULL_SUBNETWORK_URL ^
  --num_workers 1 --max_num_workers 1 --machine_type e2-standard-2 ^
  --setup_file /workspace/streaming/setup.py
```

Tambahkan `--validate-only` untuk membangun/serialize graph tanpa submit job. Validasi graph tidak membuktikan jaringan, IAM, atau runtime cloud berhasil. Konfigurasi autentikasi Kafka opsional melalui `--consumer-config` mengacu pada properti Java client; simpan file secret di luar Git dan jangan tempel credential dalam command. Gunakan run label unik untuk setiap deployment agar file GCS tidak berbenturan.

Saat demo cloud selesai, hentikan producer lalu gunakan **Drain** pada job Dataflow agar window yang tersisa dapat selesai. Periksa job benar-benar berhenti dan hentikan resource broker demo sesuai rencana. Streaming yang di-drain tidak menghapus tabel atau data lake.

Referensi teknis: [Kafka Docker](https://kafka.apache.org/39/getting-started/docker/), [Beam KafkaIO 2.67.0](https://beam.apache.org/releases/pydoc/2.67.0/apache_beam.io.kafka.html), [Kafka ke Dataflow](https://docs.cloud.google.com/dataflow/docs/guides/read-from-kafka).
