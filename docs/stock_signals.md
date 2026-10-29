# Sinyal simulasi dan tabel dashboard

Pipeline menghitung sinyal pada histori menit JPM, BAC, GS, dan MS. Data berasal dari Yahoo Finance melalui yfinance, kemudian diputar ulang melalui Kafka. Hasilnya bukan harga live, rekomendasi investasi, atau bukti profit.

## Aturan sinyal

Versi aturan: `sma_3_8_crossover_v1`. SMA3 dan SMA8 memakai harga saat ini serta bar sebelumnya dalam saham dan sesi yang sama.

| Sinyal | Kondisi |
|---|---|
| BUY | SMA3 sebelumnya tidak di atas SMA8, lalu SMA3 sekarang menembus ke atas SMA8 |
| SELL | SMA3 sebelumnya tidak di bawah SMA8, lalu SMA3 sekarang menembus ke bawah SMA8 |
| HOLD | Window lengkap tetapi tidak ada persilangan baru |
| NO_SIGNAL | Histori belum cukup, menit tidak berurutan, atau kurs belum tersedia |

Sinyal pertama paling cepat muncul pada bar kesembilan. `signal_available_at` adalah akhir bar historis, sehingga rumus tidak memakai harga masa depan. SELL berarti simulasi keluar, bukan short order; HOLD berarti tidak ada aksi baru.

## Model BigQuery

Dataset: `jcdeah-009.daud_finalproject_demo`, lokasi `asia-southeast2`.

| View | Grain dan penggunaan |
|---|---|
| `mart_stock_monitoring` | Satu saham per menit, untuk histori harga, SMA, nilai IDR, sinyal, dan perbandingan peer |
| `mart_stock_latest` | Satu baris terbaru per saham, untuk kartu status |
| `demo_signal_scenarios` | Lima skenario sintetis yang hanya menguji rumus |

Kolom utama: `symbol`, `company_name`, `minute_utc`, `session_date`, `price_usd`, `usd_idr`, `exposure_idr`, `moving_average_short`, `moving_average_long`, `strategy_signal`, `signal_reason`, `session_return`, `peer_average_return`, `excess_return_vs_peers`, dan `peer_rank`.

`comparison_complete` bernilai true hanya ketika empat saham tersedia pada menit dan awal sesi yang konsisten. Harga yang hilang tidak diimputasi. Format return sebagai persen dan jangan menjumlahkan harga, SMA, atau kurs.

## Query distribusi aktual

Jumlah berubah ketika snapshot diperbarui, sehingga dokumentasi tidak mengunci angka lama.

```sql
SELECT symbol, strategy_signal, COUNT(*) AS jumlah
FROM `jcdeah-009.daud_finalproject_demo.mart_stock_monitoring`
GROUP BY symbol, strategy_signal
ORDER BY symbol, strategy_signal;
```

## Menjalankan ulang

```powershell
Set-Location "C:\Purwadhika\Final Project"
.\.venv\Scripts\python.exe jalankan.py replay-lengkap
.\.venv\Scripts\python.exe jalankan.py periksa-mart --use-local-adc
```

Untuk memakai capture Kafka yang sudah lulus validasi:

```powershell
.\.venv\Scripts\python.exe jalankan.py replay-lengkap --source-run-id SOURCE_RUN_ID
```

Bukti build tersimpan lokal di `data/dbt/target/run_results.json`. Dashboard Looker Studio menggunakan dua mart di atas; report Looker tidak dibuat otomatis oleh pipeline.
