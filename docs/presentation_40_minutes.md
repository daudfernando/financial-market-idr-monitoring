# Presentasi final project: 40 menit

Berkas lima slide: [final_project_checkpoint.pptx](final_project_checkpoint.pptx). Speaker notes ada pada setiap slide. Angka bersumber dari audit BigQuery 17 September 2026, bukan estimasi keuntungan bisnis.

| Bagian | Durasi | Fokus dan demo |
|---|---:|---|
| 1. Masalah bisnis | 5 menit | Harga USD dan kurs berbeda frekuensi; kebutuhan monitoring nilai rupiah dan keterlacakan |
| 2. Arsitektur | 7 menit | Airflow/Spark batch; Kafka/Python replay; GCS, BigQuery, dbt; alasan tanpa Dataflow |
| 3. Pipeline/data | 10 menit | Airflow run sukses; raw/manifest GCS; 20 kurs, 7.788 bar; view monitoring/latest |
| 4. Analitik | 9 menit | Dashboard dua chart, filter saham/sesi; SMA crossover; return relatif peers |
| 5. Kualitas/status | 4 menit | 24 tests dbt, rekonsiliasi, alert Mailpit, gap yang tidak diimputasi |
| Q&A | 5 menit | Batas replay, FX join, grain data, rerun dan dedup |

## Demo

1. Buka Airflow http://localhost:8085 dan run scheduled__2026-09-17T11:00:00+00:00 yang sukses.
2. Buka GCS final-project/demo/local-python/20260917T123103782750Z/ dan report rekonsiliasi.
3. Buka dataset daud_finalproject_demo di project jcdeah-009: mart_stock_monitoring dan mart_stock_latest.
4. Buka dashboard/index.html. Tunjukkan sinyal histori bervariasi tetapi status terbaru boleh semuanya HOLD.
5. Buka Mailpit http://localhost:8025 untuk bukti alert demo lokal.

Command pengulangan: run_demo.cmd. Gunakan snapshot tervalidasi untuk presentasi agar tidak menunggu download/build.

## Q&A

- Mengapa tanpa Dataflow? Guideline menyediakan pilihan. Spark memenuhi batch, Kafka memenuhi streaming/replay, Python memproses event; jalur ini telah berjalan dengan akses yang ada.
- Apakah streaming live? Sumber adalah histori asli yang diputar ulang, sesuai catatan guideline. Consumer mengolah setiap event saat masuk; warehouse/dashboard diperbarui setelah replay lengkap.
- Apakah data lengkap? Ada 11 gap menit GS dan satu MS yang tidak diimputasi. Semua bar yang terkumpul lolos rekonsiliasi; FX join tersedia untuk seluruh 7.788 bar.
- Apa arti BUY/SELL/HOLD? Persilangan SMA3/SMA8 untuk simulasi; bukan transaksi nyata. HOLD tanpa persilangan, NO_SIGNAL saat riwayat/FX tidak cukup. Belum ada backtest keuntungan.
- Bagaimana menghindari informasi masa depan? SMA memakai bar saat ini/sebelumnya dalam sesi; FX tanpa waktu ketersediaan memakai tanggal sebelum event WIB. Revisi historis sumber belum merupakan arsip point-in-time.
- Apakah exactly-once? Tidak diklaim lintas sink. Producer idempotent, event ID stabil, dbt dedup, dan rekonsiliasi menolak data yang tidak cocok.
- Mengapa satu saham? Quantity satu memberi valuasi indikatif terbandingkan tanpa mengarang kepemilikan institusi.
- Apa yang belum? Link GitHub belum dipublikasikan, slide perlu ditinjau/latihan. Report Looker belum dibuat; sumber BQ dan HTML dua chart siap.

Referensi status: [audit guideline](guideline_readiness.md), [rumus dan tabel](stock_signals.md), data/dashboard_readiness.json.
