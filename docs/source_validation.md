# Catatan Validasi Sumber

Tahap ini menguji akses sumber dari komputer pengembangan. Belum ada ingestion GCS, Pub/Sub, atau BigQuery.

## Hasil pengamatan 10 September 2026 WIB

- Lingkungan Python 3.14.5 dibuat dalam `.venv`, dengan versi dependency tersimpan di `requirements.txt`.
- Percobaan pertama terhalang izin jaringan sandbox Windows. Percobaan berikutnya dijalankan dengan izin jaringan yang disetujui.
- Koneksi yfinance WebSocket dan subscription JPM berhasil. Jendela pengamatan 45 detik pada percobaan pertama di luar sandbox menghasilkan nol event.
- Observasi berlangsung sekitar 04.36 WIB atau 17.36 America/New_York pada hari sebelumnya. Belum dilakukan validasi kalender sesi melalui sumber kalender pasar. Tidak ada kesimpulan bahwa sumber rusak berdasarkan nol event ini.
- Request ke endpoint data JISDOR mengalami `ConnectionResetError 10054`. Percobaan berikutnya memakai GET lalu POST dengan timeout dan retry terbatas, tetapi masih mengalami reset.
- Tidak ada contoh harga atau kurs yang dibuat secara manual untuk menggantikan hasil sumber.
- Percobaan WebSocket berikutnya juga menerima nol event dan mencatat keepalive ping timeout serta percobaan reconnect dari library. Konektivitas stabil belum terbukti. Status `no_events` pada report perlu dibaca bersama log eksekusi; script kini menyimpan log library pada run berikutnya.

## Schema kandidat yang masih perlu dibuktikan

Bagian ini merekam kondisi awal sebelum pengambilan sampel berhasil. Pembaruan hasil tersedia di bagian akhir dokumen.

| Sumber | Field sumber kandidat | Field tujuan | Catatan |
|---|---|---|---|
| BI | tgl_subkursasing | jisdor_date | Parse tanggal dan timezone setelah payload diterima |
| BI | mts_subkursasing | currency | Harus USD untuk model awal |
| BI | jual_subkursasing | usd_idr | Nama field mengikuti prototipe, perlu konfirmasi payload JISDOR |
| yfinance | id | symbol | Harus sesuai simbol yang diminta |
| yfinance | price | price_usd | Harga positif; mata uang harus dikonfirmasi terpisah |
| yfinance | time | event_timestamp | Kandidat timestamp milidetik, perlu bukti event aktual |

Schema ini belum dinyatakan terverifikasi. Business key, currency, volume, tanggal publikasi, dan kedalaman historis belum dapat disimpulkan dari uji koneksi saja.

## Cara membaca hasil

Setiap run mempunyai folder unik di `data/source_validation/`. `report.json` mencatat status, error, jumlah record, dan versi library. Respons HTTP disimpan sebelum parsing jika respons diterima. Event yang masuk disimpan sebagai JSONL dengan ingestion timestamp. File raw bisa tidak ada jika sumber tidak memberi respons atau event.

Log ringkas otomatis ditambahkan ke `logs/progress.txt`. Status `no_events` menunjukkan tidak ada sampel selama jendela pengamatan; status tersebut tidak dianggap validasi data berhasil. Script mengembalikan exit code 1 jika salah satu sumber belum menghasilkan sampel valid.

## Pekerjaan lanjutan

1. Ulangi pengambilan JPM saat sesi aktif dan periksa payload, currency, timestamp, serta business key aktual.
2. Periksa akses endpoint BI dari jaringan/browser pengguna untuk membedakan reset sumber dan masalah jalur jaringan. Uji respons historis setelah akses data berhasil.
3. Jika live tetap terkendala, ambil data historis yang nyata untuk controlled replay. Simpan asal dan waktu kejadian, lalu tambahkan jalur Pub/Sub pada tahap streaming.
4. Verifikasi pengajuan dataset yfinance kepada pengajar. Hal ini belum diselesaikan oleh pemeriksaan teknis.
5. Mulai batch GCS setelah satu sampel JISDOR yang valid tersedia.

## Referensi teknis

- [BI getSubKursJisdor1](https://www.bi.go.id/biwebservice/wskursbi.asmx?op=getSubKursJisdor1) untuk operasi GET dan POST.
- [yfinance AsyncWebSocket](https://ranaroussi.github.io/yfinance/reference/api/yfinance.AsyncWebSocket.html) untuk subscribe dan listener.

## Pembaruan hasil pengambilan sampel

Run `20260909T214210877646Z` berhasil pada 10 September 2026 sekitar 04.42 WIB. Bukti tersedia di `data/source_samples/20260909T214210877646Z/report.json`.

| Sumber | Hasil yang diperoleh | Pemeriksaan yang lulus |
|---|---|---|
| JISDOR | 14 record, tanggal 20 Agustus sampai 9 September 2026 | Tanggal terbaca, mata uang USD, kurs positif dan finite, keunikan tanggal/mata uang |
| JPM historis 1 menit | 1.949 bar, 2 sampai 9 September 2026 | Metadata currency USD, timezone America/New_York, timestamp unik dan timezone-aware, close positif dan finite |

HTTP GET BI berhasil dengan `curl_cffi` dan opsi `impersonate='chrome'`. Keberhasilan ini menunjukkan jalur akses alternatif bekerja pada pengujian; penyebab pasti reset pada klien sebelumnya belum ditentukan. Verifikasi TLS tetap aktif. Validator sumber awal juga sudah memakai klien tersebut.

Payload BI disimpan sebelum parsing. Field aktual sesuai kandidat `tgl_subkursasing`, `mts_subkursasing`, dan `jual_subkursasing`. Nilai mata uang sumber memiliki spasi di akhir yang dibersihkan. Semua record lolos validasi sebelum hasil ditulis ke `processed/jisdor/date=YYYY-MM-DD/record.json` di folder run lokal. Waktu publikasi belum terverifikasi, sehingga `fx_available_at` dibiarkan null.

Pengunduhan saham menyimpan DataFrame sebagaimana dikembalikan yfinance ke CSV, lalu metadata pilihan dan kandidat replay ke JSONL. CSV ini merupakan hasil library, bukan respons HTTP asli Yahoo. Metadata yfinance versi terpasang berupa Mapping; field yang diperlukan dipilih eksplisit agar dapat disimpan sebagai JSON.

Kandidat replay berisi bar historis, bukan event WebSocket. `bar_start_utc` adalah awal interval; nilai close baru dapat dianggap tersedia setelah interval berakhir. Saat membuat event replay, gunakan akhir bar sebagai waktu ketersediaan dan pertahankan awal bar. Kelengkapan seluruh menit perdagangan belum diverifikasi terhadap kalender pasar. Volume adalah volume bar historis dan belum divalidasi untuk analitik volume.

Belum dilakukan join FX, pengujian live yang sukses, pengiriman Pub/Sub, upload GCS, atau pemuatan BigQuery. Tahap berikutnya adalah menyiapkan batch cloud dengan project ID, bucket, region, dan credential GCP yang sesuai. Sampel lokal dapat dipakai untuk melanjutkan pemrosesan sambil menunggu konfigurasi cloud.
