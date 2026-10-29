# Transformasi dan mart analitik

Replay snapshot lengkap menyimpan raw dan valid ke GCS, memuat `stg_market_replay_demo`, lalu menjalankan dbt dan ekspor dashboard. Harga historis tidak diubah agar sinyal terlihat seimbang.

| Model | Fungsi |
|---|---|
| dim_saham | Referensi manual symbol, perusahaan dan mata uang |
| stg_prices | Deduplikasi event_id |
| int_market_minutes | Grain saham per menit dan tanggal sesi |
| fact_market_exposure | Join kurs sesuai waktu, price_usd x usd_idr |
| mart_market_risk | Return dan kontribusi harga/kurs/interaksi |
| int_stock_features | SMA3/SMA8 dan kelengkapan window |
| mart_stock_monitoring | Sinyal serta perbandingan saham |
| mart_stock_latest | Observasi sumber terbaru tiap saham |

Untuk membangun model dari staging yang sudah tersedia:

```powershell
docker compose --env-file .env.airflow -f compose.yaml -f compose.analytics.yaml --profile analytics run --rm dbt build
.\.venv\Scripts\python.exe jalankan.py ekspor-dashboard --use-local-adc
.\.venv\Scripts\python.exe jalankan.py periksa-mart --use-local-adc
```

Lineage dan metadata model:

```powershell
.\.venv\Scripts\python.exe jalankan.py lihat-lineage --refresh
```

Buka http://localhost:8086. Untuk Looker gunakan project jcdeah-009, dataset daud_finalproject_demo, model mart_stock_monitoring untuk histori dan mart_stock_latest untuk status terakhir. Harga memakai AVG/MAX sesuai grain, bukan SUM lintas waktu. Filter symbol dan periode sumber. Nama company_name berasal dari dimensi manual.

Model adalah view; dashboard HTML adalah snapshot lokal yang harus diekspor ulang setelah dbt. Link report Looker belum tersedia. Aturan lengkap berada di [definisi sinyal](stock_signals.md).
