# Sinyal simulasi dan tabel dashboard

Snapshot terverifikasi 17 September 2026: 7.788 bar asli JPM, BAC, GS, MS dari Yahoo melalui yfinance. Rentang bersama 10 September 13:30 UTC sampai 16 September 19:59 UTC; lima sesi New York. Data diputar ulang melalui Kafka lokal, diproses Python consumer per event, disimpan GCS, lalu dimuat ke BigQuery dan ditransformasikan dbt. Ini replay historis, bukan harga live atau bukti eksekusi Dataflow cloud.

## Aturan yang dapat dijelaskan

Versi `sma_3_8_crossover_v1`. SMA3 adalah rata-rata harga penutupan tiga menit terakhir, SMA8 delapan menit terakhir, termasuk menit saat ini. Window dipisahkan per saham dan sesi. Parameter pendek dipilih untuk demonstrasi pembelajaran; belum dioptimalkan atau diuji sebagai strategi keuntungan.

| Sinyal | Aturan |
|---|---|
| BUY | SMA3 sebelumnya <= SMA8 sebelumnya, dan SMA3 sekarang > SMA8 sekarang |
| SELL | SMA3 sebelumnya >= SMA8 sebelumnya, dan SMA3 sekarang < SMA8 sekarang |
| HOLD | Tidak ada persilangan baru, termasuk rata-rata yang sama |
| NO_SIGNAL | Window delapan menit saat ini/sebelumnya belum lengkap, menit tidak berurutan, atau FX tidak tersedia |

Sinyal pertama paling cepat pada bar ke-9 sesi. Gap membuat sinyal tidak tersedia sampai kedua window kembali lengkap. Tidak memakai harga masa depan. Untuk bar historis, `signal_available_at` adalah awal bar + satu menit karena harga close baru diketahui setelah bar selesai. Waktu ini adalah waktu teoretis ketersediaan sinyal historis, bukan waktu penerimaan replay.

SELL merupakan simulasi sinyal keluar, bukan perintah short. HOLD berarti tidak ada aksi baru, bukan bukti memiliki posisi. Belum ada simulasi kepemilikan, biaya transaksi, atau backtest keuntungan. FX menjadi syarat kelengkapan valuasi; arah crossover dihitung dari harga USD.

## Hasil aktual dari data asli

| Saham | BUY | SELL | HOLD | NO_SIGNAL | Total |
|---|---:|---:|---:|---:|---:|
| JPM | 139 | 137 | 1.634 | 40 | 1.950 |
| BAC | 137 | 137 | 1.636 | 40 | 1.950 |
| GS | 123 | 117 | 1.580 | 119 | 1.939 |
| MS | 131 | 131 | 1.639 | 48 | 1.949 |
| Total | 530 | 522 | 6.489 | 247 | 7.788 |

Label mengikuti rumus, tanpa memaksa jumlah BUY/SELL/HOLD. Empat bar terbaru kebetulan semuanya HOLD; gunakan histori untuk melihat variasi. Lima skenario sintetis terpisah menguji cross up, cross down, tren berlanjut, rata-rata sama, dan riwayat kurang dengan macro SQL yang sama. Skenario tidak dimasukkan ke tabel harga asli.

## Sumber BigQuery untuk Looker Studio

Project `jcdeah-009`, dataset `daud_finalproject_demo`, lokasi `asia-southeast2`. Berikut adalah **view BigQuery** yang dapat langsung dipilih oleh konektor:

| View | Penggunaan |
|---|---|
| `mart_stock_monitoring` | 7.788 baris histori: grafik harga, SMA, nilai IDR, sinyal, dan perbandingan |
| `mart_stock_latest` | Empat baris terbaru berdasarkan waktu sumber: kartu/status per saham |
| `demo_signal_scenarios` | Lima contoh sintetis untuk menjelaskan rumus, tampilkan di halaman simulasi terpisah |

Kolom utama: `symbol`, `institution_name`, `minute_utc`, `session_date`, `price_usd`, `usd_idr`, `exposure_idr`, `moving_average_short`, `moving_average_long`, `strategy_signal`, `signal_reason`, `signal_rule_version`, `signal_available_at`, `data_status`, `data_kind`, `last_updated_at`.

Perbandingan menggunakan `session_return = price_usd / session_first_price - 1`. `peer_average_return` adalah rata-rata return tiga saham lainnya pada menit yang sama. `excess_return_vs_peers` adalah return saham dikurangi rata-rata tersebut. `peer_rank` mengurutkan return terbesar; nilai seri dapat memiliki peringkat sama. Benchmark ini kelompok pembanding proyek, bukan indeks pasar resmi.

`comparison_complete` hanya true ketika empat saham tersedia dengan waktu awal sesi yang sama. GS kehilangan 11 menit dan MS satu menit; harga tidak diisi perkiraan. Ada 36 baris tersedia pada menit yang perbandingannya tidak lengkap; return pembanding dan rank dibuat null. `aligned_with_latest` pada view latest menunjukkan apakah waktu saham sejajar dengan menit terbaru seluruh kelompok.

Rekomendasi komponen dashboard:

- Tabel latest: saham, harga USD, nilai IDR, sinyal, alasan, return sesi, return pembanding, rank, dan waktu sumber.
- Grafik dari monitoring: `minute_utc` sebagai waktu, harga/SMA3/SMA8 sebagai metrik; filter satu saham dan sesi.
- Distribusi sinyal: dimensi `strategy_signal`, metrik jumlah baris. Label sebagai jumlah sinyal, bukan jumlah transaksi.
- Format return sebagai persen (nilai 0,01 berarti 1%). Harga/SMA/kurs menggunakan AVG atau MAX pada grain satu saham/menit, bukan SUM.
- Tampilkan mode replay dan tanggal sumber. `last_updated_at` adalah waktu ingestion, bukan bukti harga pasar saat ini.

## Mengulang dan memeriksa

```bat
cd /d "C:\Purwadhika\Final Project"
run_demo.cmd
```

Input ditentukan `config/current_market_replay.json`. Untuk mengulang dari capture yang sudah valid:

```bat
run_demo.cmd --source-run-id 20260917T123103782750Z
```

Build terverifikasi: delapan view, 21 data tests, tiga unit tests; seluruhnya lulus. Rekonsiliasi 7.788 Kafka/Python/BigQuery records cocok tanpa duplikasi atau mismatch; FX kosong nol. Metadata unduhan: `data/market_universe/20260917T121614070331Z/report.json`. Bukti build: `data/dbt/target/run_results.json`.

Pengambilan data baru tersedia melalui `scripts/collect_market_universe.py` di environment yang memiliki yfinance. Setelah report berstatus passed, arahkan path dan jumlah records di `config/current_market_replay.json` ke hasil baru sebelum menjalankan replay. Jumlah dan distribusi sinyal dapat berubah pada snapshot berikutnya.

Dataflow tidak digunakan pada jalur final. Dashboard Looker Studio dapat dibuat sekarang menggunakan view demo; report Looker belum dibuat oleh pipeline ini.
