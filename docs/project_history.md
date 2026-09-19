# Riwayat pengembangan (snapshot lama)

Arsip README sebelum submission. Angka dan status di sini bukan status final; gunakan README utama. Link relatif lama dapat merujuk struktur root.

# Financial Market Risk and IDR Exposure Monitoring

Final Project Job Connector Data Engineering Purwadhika JCDEAH-009

**Status 17 September 2026 ? jalur final sesuai pilihan teknologi guideline sudah berjalan.** Batch: Airflow + Spark ? GCS ? BigQuery. Streaming replay: Kafka lokal + Python ? GCS ? BigQuery. Transformasi: dbt. Dataflow tidak menjadi prasyarat. Batch terbaru sukses; 20 tanggal JISDOR dan 7.788 bar empat saham siap untuk dashboard. Delapan view dan 24 pengujian dbt lulus.

**Persiapan D-1:** jalankan `.\prepare_demo.cmd` di PowerShell untuk mengunduh saham terbaru, menggabungkan arsip tanpa duplikasi, menjalankan batch JISDOR (window 45 hari + audit tanggal), lalu memperbarui mart/dashboard. `.\run_streaming_demo.cmd` memakai bar terbaru dari kandidat aktif. Panduan bagian 10 menjelaskan tanggal kosong dan perbedaan arsip dengan snapshot aktif.

**Demo per pesan:** jalankan `run_streaming_demo.cmd --limit 40 --interval 2` untuk Kafka -> Python -> GCS -> streaming insert BigQuery satu baris per pesan. Tabel/view `demo_streaming_events` terpisah dari mart dashboard. Panduan command dan query ada di bagian 9 [panduan demo](docs/panduan_demo_project.txt).

**Mulai di sini:** [audit guideline dan arsitektur final](docs/guideline_readiness.md), [tabel dashboard dan rumus sinyal](docs/stock_signals.md), [dashboard HTML](dashboard/index.html), [panduan demo](docs/demo_analytics.md).

Jalankan `run_demo.cmd`. Replay diproses per event; GCS/BigQuery/dashboard diperbarui setelah replay selesai. Histori menghasilkan 530 BUY, 522 SELL, 6.489 HOLD, dan 247 NO_SIGNAL. Ini sinyal simulasi, bukan hasil backtest atau harga live. Progress TXT tidak ditulis.

Link GitHub belum dipublikasikan. Report Looker Studio dapat dibuat dari view BigQuery yang sudah tersedia; dashboard HTML dua chart sudah siap. Bagian riwayat implementasi di bawah memuat snapshot lama; status final mengacu audit guideline di atas.

## 1. Business Context

Institusi finansial Indonesia dapat memiliki posisi pada instrumen global yang dinilai dalam USD. Nilai posisi dalam rupiah dipengaruhi oleh perubahan harga instrumen dan perubahan USD/IDR.

Dalam skenario proyek ini, tim treasury, investment, atau market risk membutuhkan tampilan terintegrasi untuk memahami kedua faktor tersebut. Harga saham yang meningkat dalam USD dapat mengalami kenaikan lebih kecil dalam rupiah ketika rupiah menguat. Saat harga saham stabil dan rupiah melemah, nilai indikatif saham dalam rupiah dapat meningkat.

Proyek menggunakan saham institusi finansial sebagai instrumen pengamatan. Kebutuhan ini merupakan skenario pembelajaran dan belum didasarkan pada data kepemilikan institusi tertentu.

## 2. Problem Statement

> Bagaimana membangun pipeline batch dan streaming yang mengintegrasikan harga saham institusi finansial global dengan kurs referensi JISDOR, sehingga pengguna dapat memantau nilai indikatif dalam rupiah, memisahkan kontribusi perubahan harga saham dan kurs, serta mengenali pergerakan pasar yang memerlukan pemeriksaan lebih lanjut?

Masalah yang perlu diselesaikan meliputi berikut.

- Sumber harga pasar dan kurs mempunyai format serta frekuensi pembaruan berbeda.
- JISDOR mengikuti hari publikasi, sementara event saham mengikuti sesi perdagangan pasar asal.
- Penggabungan waktu yang keliru dapat memakai kurs yang belum tersedia ketika event terjadi.
- Duplikasi, data terlambat, dan kegagalan ingestion dapat memengaruhi analisis.
- Perubahan nilai rupiah perlu diuraikan agar kontribusi harga saham dan kurs dapat dijelaskan.

Dashboard ditujukan untuk menjawab nilai instrumen dalam rupiah, perubahannya dari waktu ke waktu, kontribusi masing-masing faktor, serta instrumen dengan volatilitas atau pergerakan abnormal relatif terhadap historinya.

## 3. Solusi dan Alasan Pemilihannya

Solusi yang diusulkan adalah pipeline end-to-end untuk mengambil JISDOR secara batch dan event harga saham secara streaming. Kedua sumber disimpan ke data lake, diproses menuju warehouse, lalu diubah menjadi analytical mart untuk monitoring.

| Keputusan | Alasan pemilihan |
|---|---|
| JISDOR sebagai benchmark USD/IDR | Memberikan referensi kurs harian yang konsisten untuk valuasi indikatif |
| Saham JPM, BAC, GS, dan MS | Menjaga konteks institusi finansial dengan jumlah instrumen yang terbatas |
| Batch untuk kurs dan streaming untuk harga | Mengikuti frekuensi masing-masing sumber |
| Simpan raw sebelum transformasi | Mendukung audit, debugging, dan pemrosesan ulang |
| Dekomposisi perubahan nilai rupiah | Menjelaskan kontribusi harga saham, FX, dan interaksi keduanya |
| Indikator berbasis distribusi historis | Memberikan dasar terukur untuk menandai pergerakan yang tidak biasa |
| Mart diperbarui berkala | Membatasi kompleksitas agar realistis untuk proyek individual |

Manfaat yang dituju adalah dataset konsisten untuk analis, berkurangnya penggabungan manual, serta keterlacakan angka dashboard ke sumber dan aturan perhitungannya. Manfaat tersebut belum diukur.

## 4. Sumber Data

### Batch JISDOR Bank Indonesia

JISDOR digunakan sebagai benchmark USD/IDR. Web service BI menyediakan operasi berdasarkan tanggal dan interval tanggal. Endpoint dan struktur XML aktual perlu diuji pada tahap eksplorasi. [Dokumentasi web service BI](https://www.bi.go.id/biwebservice/wskursbi.asmx)

Simpan raw response sebelum parsing. Field bersih minimum adalah `jisdor_date`, `currency`, `usd_idr`, `source`, dan `ingested_at`. Tambahkan metadata waktu ketersediaan kurs untuk mendukung join historis.

Pipeline harus menangani akhir pekan, hari tanpa publikasi, pengambilan historis, dan kegagalan akses. Tidak adanya kurs baru pada hari tanpa jadwal publikasi merupakan kondisi yang diharapkan.

### Streaming harga saham melalui yfinance

Dokumentasi yfinance menyediakan `WebSocket` dan `AsyncWebSocket` untuk pembaruan harga Yahoo Finance berdasarkan simbol. Koneksi, ketersediaan event, dan latensi simbol proyek masih perlu diuji. [Dokumentasi yfinance WebSocket](https://ranaroussi.github.io/yfinance/reference/yfinance.websocket.html)

| Simbol | Institusi |
|---|---|
| JPM | JPMorgan Chase |
| BAC | Bank of America |
| GS | Goldman Sachs |
| MS | Morgan Stanley |

Field normalisasi minimum adalah `event_timestamp`, `event_date`, `symbol`, `price_usd`, `source`, dan `ingested_at`. Verifikasi mata uang instrumen USD sebelum valuasi. Simpan payload sumber serta field tambahan untuk pemeriksaan.

Event harga tidak otomatis menunjukkan transaksi individual. Volume hanya digunakan setelah makna dan konsistensinya terbukti, termasuk apakah nilainya kumulatif. Model awal tidak menghitung buy/sell pressure.

Jika WebSocket terkendala, polling atau controlled replay menjadi fallback dan tetap mengirim event melalui Kafka. Setiap event membawa `ingestion_mode` berupa `live_websocket`, `polling`, atau `replay`. Pertahankan waktu kejadian sumber saat replay dan catat waktu replay secara terpisah. Tampilkan mode pada dashboard dan demo. Polling dan replay tidak disebut native exchange streaming.

## 5. Arsitektur dan Teknologi

Diagram berikut adalah target cloud. Jalur demo yang sudah diuji menggunakan Beam DirectRunner lokal sebelum GCS/BigQuery; lihat panduan demo untuk diagram status aktual.

```text
BI JISDOR -> Airflow scheduled ingestion -> GCS raw/jisdor

yfinance WebSocket -> Python producer -> Kafka
                   -> Dataflow (Apache Beam) -> GCS raw/market + BigQuery staging

GCS raw/jisdor -> PySpark validation + transformation -> GCS processed/jisdor
        -> BigQuery staging -> dbt transformation dan tests
        -> Analytical mart -> Looker Studio
```

Airflow mengatur pekerjaan terjadwal, dependensi, retry, dan alert. Producer berjalan sebagai service tersendiri; Dataflow membaca Kafka, memvalidasi event, dan menulis ke data lake serta staging. Broker harus dapat dijangkau worker Dataflow melalui jaringan yang dikonfigurasi; Kafka localhost saja belum cukup untuk deployment cloud.

Target awal penyimpanan file streaming sekitar satu menit dan pembaruan mart sekitar lima menit. Ini adalah target yang perlu diukur. Refresh dashboard juga dipengaruhi pengaturan dan cache.

| Teknologi | Fungsi |
|---|---|
| GCP | Cloud utama |
| Python dan yfinance | Ingestion, normalisasi, producer, dan consumer |
| Kafka | Buffer event, offset, dan replay streaming |
| Dataflow / Apache Beam | Pemrosesan streaming, validasi event, dan penulisan sink |
| GCS | Data lake dengan area raw dan processed |
| Airflow | Penjadwalan, pemuatan file, transformasi, pemeriksaan, dan alert |
| PySpark | Pemrosesan batch dari data lake |
| BigQuery | Staging, model data, dan analytical mart |
| dbt | Transformasi SQL, dependensi model, dan data tests |
| Looker Studio | Dashboard |
| Docker dan Docker Compose | Lingkungan pengembangan lokal |
| GitHub | Versi kode dan dokumentasi |

Kafka menyimpan dan menyalurkan event; Dataflow menjalankan pemrosesan streaming. PySpark menangani batch JISDOR, sehingga peran ketiganya berbeda. Volume JISDOR kecil; Spark lokal digunakan untuk pembelajaran dan kesesuaian guideline, belum membutuhkan cluster. Deployment streaming perlu verifikasi jaringan Kafka ke Dataflow dan batas biaya sebelum dijalankan.

### Data lake

```text
raw/jisdor/year=YYYY/month=MM/day=DD/run_id=.../
raw/market/year=YYYY/month=MM/day=DD/hour=HH/run_id=.../
processed/jisdor/year=YYYY/month=MM/day=DD/
processed/market/year=YYYY/month=MM/day=DD/
```

Pertahankan respons asli, ingestion timestamp, source, dan identitas run. Untuk respons historis dengan banyak tanggal, simpan juga record yang dipisahkan per tanggal agar penyimpanan harian terpenuhi. Pemisahan partisi tidak menciptakan observasi yang tidak tersedia pada sumber. Hindari overwrite raw antar-run dan catat file yang telah diproses agar rerun idempotent.

## 6. Hubungan Data dan Definisi Exposure

### Valuasi indikatif

```text
price_idr = price_usd × usd_idr
exposure_idr = quantity × price_usd × usd_idr
```

Versi awal memakai **quantity tetap satu saham per instrumen**. Exposure yang ditampilkan merupakan nilai indikatif per saham. Data ini tidak menyatakan posisi aktual institusi. Jika posisi simulasi ditambahkan, quantity dan label simulasi harus eksplisit.

Sebagai ilustrasi, harga USD 300 dan JISDOR Rp17.000 menghasilkan Rp5.100.000 per saham. Angka tersebut hanya contoh perhitungan, tidak digunakan sebagai hasil pasar atau hasil pipeline.

### Join latest available JISDOR

Simpan event timestamp dalam UTC dan turunkan tanggal WIB untuk pemeriksaan tanggal kurs. Pertahankan tanggal sesi pasar asal secara terpisah untuk agregasi saham.

Pilih kurs terakhir yang memenuhi kedua syarat berikut.

1. `jisdor_date <= event_date_wib`.
2. `fx_available_at <= event_timestamp`.

Syarat tanggal saja belum cukup untuk event sebelum publikasi pada hari yang sama. `fx_available_at` berasal dari waktu publikasi yang terverifikasi atau waktu pertama kurs diamati pipeline untuk data live. Untuk historis tanpa waktu publikasi, gunakan aturan konservatif kurs bertanggal sebelum tanggal event dan dokumentasikan kebijakannya. Waktu ingestion backfill tidak diperlakukan sebagai waktu publikasi historis.

Gunakan kurs terakhir yang memenuhi aturan ketika kurs baru belum tersedia. Tampilkan tanggal dan umur kurs. Jika pasangan valid tidak tersedia, hasil valuasi ditandai tidak tersedia dan dicatat sebagai masalah kualitas. Hari tanpa publikasi tidak otomatis memicu kegagalan.

### Dekomposisi perubahan exposure

Gunakan harga `P`, kurs `F`, dan quantity tetap `q` pada dua waktu pembanding yang sama. Subskrip `0` adalah awal periode dan `1` adalah akhir periode.

```text
asset_effect       = q × (P1 - P0) × F0
fx_effect          = q × P0 × (F1 - F0)
interaction_effect = q × (P1 - P0) × (F1 - F0)

total_change_idr = asset_effect + fx_effect + interaction_effect
                = q × P1 × F1 - q × P0 × F0

asset_return    = P1 / P0 - 1
fx_return       = F1 / F0 - 1
exposure_return = (1 + asset_return) × (1 + fx_return) - 1
```

Efek interaksi ditampilkan terpisah agar seluruh perubahan dapat direkonsiliasi. Quantity harus tetap selama perbandingan. Perubahan jumlah posisi memerlukan komponen tambahan dan berada di luar scope awal.

JISDOR merupakan benchmark harian. FX return antar-event akan nol selama kurs yang dipakai belum berubah. Analisis kontribusi FX membutuhkan beberapa hari publikasi dan tidak menggambarkan pergerakan FX intraday. Kenaikan nilai rupiah tidak otomatis berarti kenaikan risiko atau keuntungan terealisasi.

### Volatilitas dan risk indicator

Rencana awal menggunakan return harga pada interval satu menit yang konsisten, lalu rolling standard deviation per instrumen. Jendela, minimum observasi, dan penanganan sesi ditetapkan setelah eksplorasi. Pisahkan return overnight dari intraday dan tangani stock split sebelum menafsirkan lonjakan sebagai stress.

Risk indicator direncanakan menggunakan percentile historis volatilitas atau z-score return. Baseline hanya memakai data sebelum observasi yang dinilai dengan interval dan sesi sebanding. Pilihan batas harus menjelaskan frekuensi alert yang dituju dan diperiksa terhadap historis. Jika data belum cukup, gunakan status `insufficient_data`.

Abnormal movement menjadi sinyal pemeriksaan lebih lanjut. Interpretasi market stress memerlukan dukungan beberapa metrik. Dashboard tidak menyimpulkan kondisi institusi hanya dari harga sahamnya.

## 7. Rancangan Data Model

| Model | Grain atau arti satu baris | Kolom utama |
|---|---|---|
| `dim_instrument` | Satu instrumen | symbol, institution_name, currency, exchange_timezone |
| `fact_jisdor_daily` | Satu tanggal referensi dan mata uang pada versi terpilih | jisdor_date, currency, usd_idr, fx_available_at, source, ingested_at |
| `fact_market_event` | Satu event harga yang telah dideduplikasi | event_key, event_timestamp, event_date, symbol, price_usd, source, ingestion_mode, ingested_at |
| `fact_market_exposure` | Satu instrumen dan menit pengamatan | symbol, event_timestamp, quantity, price_usd, jisdor_date, usd_idr, price_idr, exposure_idr, returns, contribution fields |
| `mart_market_risk` | Satu instrumen dan interval analisis | valuation, contributions, volatility, risk_level, baseline_count, freshness |

Interval analisis dan aturan pemilihan harga terakhir harus eksplisit. Dimensi tanggal ditambahkan jika membantu query kalender. Raw menyimpan revisi sumber; pemilihan versi kurs harus mempertahankan keterlacakan.

Business key event ditetapkan setelah melihat payload. Prioritaskan identitas sumber jika tersedia. Jika tidak ada, gunakan fingerprint deterministik field sumber yang relevan dan evaluasi risiko menggabungkan event berbeda. Jangan mengasumsikan simbol dan timestamp selalu unik atau memakai ingestion timestamp untuk mendeteksi pengiriman ulang.

## 8. Data Quality dan Failure Handling

| Pemeriksaan | Tujuan |
|---|---|
| Kurs tidak null, positif, dan tanggal valid | Memastikan input valuasi layak |
| Simbol sesuai daftar dan currency USD | Mencegah penggunaan instrumen atau unit yang salah |
| Harga tidak null, positif, dan timestamp valid | Memastikan event dapat diproses |
| Business key unik pada hasil bersih | Mencegah duplikasi hasil |
| Tanggal dan waktu ketersediaan kurs memenuhi join | Mencegah future lookup |
| Freshness harga mengikuti sesi pasar | Mendeteksi ingestion berhenti tanpa menganggap penutupan pasar sebagai kegagalan |
| JISDOR baru sesuai kalender publikasi | Membedakan hari tanpa publikasi dan keterlambatan sumber |
| Jumlah kontribusi sama dengan total perubahan | Memastikan dekomposisi benar dalam toleransi numerik |
| Baseline cukup dan tidak memuat masa depan | Menjaga interpretasi risk indicator |

Technical failure mencakup koneksi terputus, timeout, dan load gagal. Data quality failure mencakup record invalid, duplikasi hasil bersih, serta join FX yang salah. Keduanya mempunyai kategori log dan alert berbeda.

Penanganan yang direncanakan meliputi timeout, retry dengan backoff terbatas, reconnect, acknowledgment setelah raw tersimpan, pencatatan file terproses, dan load idempotent. Data invalid dipisahkan untuk pemeriksaan. Critical failure menghentikan publikasi hasil terdampak dan menghasilkan alert melalui kanal yang dikonfigurasi. Error tidak hanya dicetak lalu diabaikan.

Logging menyertakan run ID, source, dan konteks kegagalan. Credential tidak disimpan di repository; gunakan environment variable atau mekanisme credential lingkungan.

## 9. Dashboard yang Direncanakan

| Visual | Pertanyaan bisnis |
|---|---|
| Nilai indikatif IDR dari waktu ke waktu | Bagaimana nilai per saham berubah? |
| Tren JISDOR | Bagaimana benchmark USD/IDR berubah? |
| Kontribusi aset, FX, dan interaksi | Faktor mana yang menjelaskan perubahan nilai rupiah? |
| Rolling volatility dan status indikator | Instrumen mana yang mengalami pergerakan relatif tidak biasa? |

Minimum dua chart mengikuti guideline. Nilai IDR dan kontribusi perubahan menjadi prioritas, kemudian tren kurs dan volatilitas. Tampilkan periode pembanding, quantity, tanggal kurs, waktu data terbaru, mode ingestion, dan kecukupan baseline. Perbandingan return serta volatilitas menggunakan interval yang sama untuk semua instrumen.

## 10. Kesesuaian dengan Guideline JCDEAH-009

| Requirement | Rencana pemenuhan |
|---|---|
| Batch dan streaming | Batch JISDOR serta event harga melalui Kafka |
| Sumber masuk data lake | Kedua jalur menyimpan raw di GCS |
| Penyimpanan harian untuk sumber file bulanan | Partisi harian ketika sumber mencakup banyak tanggal, dengan respons asli tetap disimpan |
| Data lake menuju data warehouse | Pemrosesan GCS ke BigQuery staging |
| Transformasi warehouse | dbt untuk exposure, kontribusi, dan risk mart |
| Data quality | Validasi, dbt tests, freshness, dan rekonsiliasi kontribusi |
| Alert pipeline gagal | Alert technical failure dan critical data quality failure |
| Dashboard minimal dua chart dan tujuan bisnis | Monitoring nilai rupiah dan faktor perubahannya |
| Arsitektur, programming, dan data modeling | Diagram, kode modular, grain tabel, dan alasan keputusan |
| Pengumpulan | Link GitHub dan berkas slide |

JISDOR termasuk rekomendasi dataset. Saham melalui yfinance merupakan pengajuan dataset sendiri dan perlu mengikuti mekanisme bootcamp. Guideline memperbolehkan data dummy atau replay sebagian dataset jika streaming tidak ditemukan. Proyek memprioritaskan live dan mendokumentasikan fallback secara transparan.

Bobot penilaian adalah teknis 40% dan presentasi 60%. Di dalam teknis, arsitektur/dokumentasi 20%, programming 20%, data modeling 20%, batch 25%, streaming 10%, dan dashboard 5%. Di dalam presentasi, penyampaian 35%, slide 15%, dan Q&A 50%.

Deadline pada dokumen adalah **20 September 2026**. Persiapan penjelasan dan demo mengikuti alokasi presentasi 40 menit yang disampaikan pengguna; pembagian Q&A perlu mengikuti ketentuan sesi.

## 11. Tahapan Pengerjaan

1. **Validasi sumber dan scope.** Uji JISDOR serta satu simbol saham. Dokumentasikan schema, timezone, currency, ketersediaan historis, dan mekanisme pengajuan dataset.
2. **Batch end-to-end.** Simpan raw kurs, proses ke BigQuery, dan buktikan rerun tidak menggandakan hasil.
3. **Streaming end-to-end.** Kirim event melalui Kafka ke GCS, uji reconnect dan duplikasi, lalu muat file baru ke warehouse.
4. **Integrasi dan analitik.** Implementasikan join tanpa future lookup, valuasi satu saham, kontribusi perubahan, dan uji rekonsiliasi.
5. **Quality dan indikator.** Simulasikan kegagalan, buktikan alert, kumpulkan baseline, kemudian evaluasi risk indicator.
6. **Dashboard dan dokumentasi.** Buat minimal dua chart, petunjuk menjalankan proyek, serta demo perjalanan satu event.
7. **Slide dan latihan.** Jelaskan problem, teknologi, data model, kualitas, hasil yang benar-benar diperoleh, dan keterbatasan.

Pengerjaan dilakukan per tahap sesuai task berikutnya. Hasil pipeline, benchmark, insight pasar, dan status production ready hanya dapat dituliskan setelah dibuktikan.

## Validasi Sumber Lokal

Script eksplorasi tersedia di `scripts/validate_sources.py`. Script mengambil sampel JISDOR dan mencoba WebSocket satu saham dalam waktu terbatas, menyimpan bukti ke `data/source_validation/<run_id>/`, serta menyimpan report teknis JSON. Tahap ini belum memuat data ke GCS atau BigQuery.

Jalankan dari root proyek menggunakan PowerShell.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe scripts\validate_sources.py --symbol JPM --seconds 45
```

`report.json` memuat hasil pemeriksaan dan versi library. Exit code 1 menandakan salah satu sumber gagal atau belum menghasilkan sampel valid. Tidak adanya event dalam jendela uji belum membuktikan sumber rusak; periksa koneksi, sesi pasar, dan waktu observasi. File raw lokal dan virtual environment dikecualikan dari Git. Requirements mencatat versi lingkungan yang digunakan pada eksplorasi ini.

Pengambilan sampel historis dan pemrosesan harian lokal dapat dijalankan dengan perintah berikut.

```powershell
.\.venv\Scripts\python.exe scripts\collect_source_samples.py
```

Hasil tersimpan di `data/source_samples/<run_id>/`. Pada run `20260909T214210877646Z`, diperoleh 14 record JISDOR dan 1.949 bar JPM interval satu menit. Kurs telah dipisahkan ke partisi harian. File historis JPM merupakan kandidat input controlled replay dan belum dikirim melalui Kafka. Detail hasil dan batas validasi terdapat di `docs/source_validation.md`.

## Persiapan Batch GCS

`scripts/upload_jisdor_gcs.py` mengunggah sampel JISDOR yang sudah tervalidasi. Bucket `jcdeah-009-daud-finalproject` telah dibuat di project `jcdeah-009`, region Jakarta (`asia-southeast2`), kelas Standard, dengan uniform bucket-level access dan public access prevention. Upload nyata berhasil untuk 16 objek, terdiri dari XML sumber, 14 record harian, dan manifest batch. Rerun memverifikasi seluruh 16 objek identik dan melewatinya. Empat unit test upload juga lulus.

```powershell
.\.venv\Scripts\python.exe scripts\upload_jisdor_gcs.py --source-dir data\source_samples\20260909T214210877646Z
```

Detail konfigurasi, cara upload, dan batas pengujian tersedia di [panduan batch GCS](docs/gcs_batch_setup.md). Penulisan `logs/progress.txt` kini dinonaktifkan secara default. Tahap ini belum mencakup Airflow, PySpark, BigQuery, dan alert eksternal.

Konfigurasi non-secret dicatat di `config/gcp.json`. File tersebut menjadi referensi konfigurasi dan belum dimuat otomatis oleh script. Jalankan upload menggunakan argumen eksplisit berikut.

```powershell
.\.venv\Scripts\python.exe scripts\upload_jisdor_gcs.py --source-dir data\source_samples\20260909T214210877646Z --project jcdeah-009 --bucket jcdeah-009-daud-finalproject --use-local-adc --upload
```

Flag `--use-local-adc` memilih login pengguna lokal untuk project ini tanpa mengubah environment credential project lain pada komputer.

## Batch GCS ke BigQuery

Tabel `jcdeah-009.daud_finalproject.fact_jisdor_daily` sudah berisi 14 record JISDOR pada region `asia-southeast2`. Script membaca manifest dan objek dari GCS, memverifikasi checksum, memuat staging, lalu menjalankan SQL `MERGE` berdasarkan tanggal kurs dan mata uang.

```powershell
.\.venv\Scripts\python.exe scripts\load_jisdor_bigquery.py --project jcdeah-009 --bucket jcdeah-009-daud-finalproject --run-id 20260909T214210877646Z --use-local-adc
```

Pemuatan pertama memengaruhi 14 baris. Rerun memengaruhi 0 baris, dengan jumlah akhir tetap 14, duplikasi 0, record invalid 0, dan selisih terhadap batch sumber 0. Delapan unit test lokal lulus. Tabel staging per eksekusi akan kedaluwarsa setelah satu hari; tabel fact tetap tersedia.

Command script tidak lagi otomatis dicatat ke `logs/progress.txt`. Detail proses, schema, dan batas implementasi tersedia di [panduan batch BigQuery](docs/bigquery_batch.md). Pemuatan staging menggunakan BigQuery load job. Alur terbaru menggunakan PySpark untuk transformasi batch dan Airflow untuk orkestrasi.

## Airflow dan Alert Lokal

Stack Docker berisi Airflow 3.3.0, PostgreSQL, dan Mailpit. DAG `jisdor_daily` menjalankan `collect -> upload_raw -> transform_spark -> upload -> warehouse`, dengan dua retry, `catchup=False`, dan maksimal satu run aktif. Run manual `manual_validation_20260912` sukses. Batch baru memproses 14 record; tabel BigQuery kini mempunyai 16 tanggal unik sampai 11 September 2026, tanpa duplikasi atau record invalid.

DAG `failure_alert_demo` sengaja gagal dan menghasilkan email di Mailpit. Bukti callback ada di `logs/alerts.jsonl`. Ini merupakan alert email lokal untuk demo, belum pengiriman eksternal. Sepuluh unit test lulus.

- Airflow: http://localhost:8085, username `daud`.
- Password lokal: `data/airflow-state/simple_auth_manager_passwords.json`.
- Inbox alert: http://localhost:8025.

```powershell
.\.venv\Scripts\python.exe scripts\compose.py ps
.\.venv\Scripts\python.exe scripts\compose.py up -d
.\.venv\Scripts\python.exe scripts\compose.py stop
```

Command di atas masing-masing memeriksa status, menyalakan, dan menghentikan stack. Wrapper Python menjalankan Docker Compose; penulisan progress TXT dinonaktifkan. Panduan lengkap tersedia di [Airflow lokal](docs/airflow_local.md). Port 8085 dipilih karena port 8080 sudah digunakan aplikasi lain.

## Batch PySpark

Raw XML dikirim ke GCS lebih dahulu. Task `transform_spark` mengunduhnya kembali, memverifikasi checksum, lalu memakai pembaca XML Spark 4.0.1 untuk normalisasi tanggal Jakarta, mata uang, dan kurs. Batch kosong, tanggal/rate invalid, serta key duplikat menggagalkan task sebelum publikasi manifest. `fx_available_at` tetap null karena waktu publikasi belum terverifikasi.

Panduan command dan validasi tersedia di [Batch PySpark](docs/spark_batch.md). Riwayat angka pada bagian sebelumnya merupakan snapshot pengujian terdahulu.

## Riwayat eksperimen Kafka + Dataflow (bukan jalur final)

Kafka lokal sudah menerima 100 bar historis JPM yang diberi label `replay`. Validasi Apache Beam DirectRunner menghasilkan 100 record valid dan 0 invalid. Pengiriman ulang menghasilkan 200 pesan dengan 100 identitas observasi unik. Topic `market.prices.live.v1` terpisah dari `market.prices.replay.v1`.

Pipeline native KafkaIO untuk Dataflow telah lolos validasi graph; deployment cloud belum dijalankan karena broker saat ini hanya berada di Docker lokal. Tabel staging `stg_market_events` dan view `market_events_deduplicated` sudah dibuat di BigQuery, masih kosong. Probe live belum berhasil menghasilkan event valid; percobaan terakhir mengalami kegagalan DNS.

Command CMD, bukti pengujian, dan pembagian peran tools terdapat pada [panduan streaming Kafka + Dataflow](docs/streaming_kafka_dataflow.md). Persiapan cloud sudah dilanjutkan: preflight mengonfirmasi izin job Dataflow tersedia, tetapi izin provisioning VM/jaringan/service account belum tersedia dan resource khusus belum ada. [Panduan deployment cloud](docs/cloud_deployment.md) memuat script rencana admin, pesan untuk mentor, bundle replay, launcher validasi/submit, dan cara menghentikan demo. Belum ada VM atau job Dataflow baru yang dibuat. Tidak ada penambahan catatan ke `logs/progress.txt`.

## Analitik dan dashboard yang sudah diuji

Empat model dbt tersedia di `analytics/models/`: deduplikasi event, observasi per menit, valuasi dengan aturan waktu FX, dan mart kontribusi/risk indicator. Seluruh 1.949 record demo mendapat pasangan FX, tanpa future lookup menurut kebijakan konservatif yang diuji. Dashboard menyediakan dua grafik, filter sesi, kontribusi perubahan nilai, dan CSV. Threshold z-score merupakan parameter demo yang belum dikalibrasi.

Command satu langkah: `run_demo.cmd`. Rerun yang menggunakan capture lama: `run_demo.cmd --source-run-id 20260917T123103782750Z`. Command mengganti snapshot tabel demo dan menjalankan dbt tests sebelum ekspor dashboard; tabel streaming cloud tidak ditimpa. Detail batas data, hasil, dan struktur model tersedia di [panduan analitik](docs/demo_analytics.md).

## Referensi

- Dokumen lokal `Guideline & Standar Penilaian Final Project - JCDEAH-009.docx` sebagai acuan requirement dan penilaian.
- [Bank Indonesia web service JISDOR](https://www.bi.go.id/biwebservice/wskursbi.asmx).
- [Dokumentasi yfinance WebSocket](https://ranaroussi.github.io/yfinance/reference/yfinance.websocket.html).
- [NSE Stock Tracker](https://github.com/llmengineer-commits/nse-stock-tracker) sebagai referensi pola engineering yang diajukan pengguna. Business problem, model, kode, dan logika analitik dikembangkan sendiri. Klaim peringkat atau kelulusan referensi tidak dijadikan dasar requirement.
