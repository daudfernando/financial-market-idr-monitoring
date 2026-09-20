# Deployment cloud: Kafka + Dataflow

## Hasil pemeriksaan akses

Pada 16 September 2026, user ADC yang dipakai project ini memiliki izin membuat, membaca, cancel, dan drain/update job Dataflow. API Dataflow sudah enabled dan tabel `stg_market_events` tersedia.

User ADC tidak memiliki izin provisioning berikut pada project `jcdeah-009`:

- `compute.instances.create`
- `compute.networks.create`
- `compute.subnetworks.create`
- `compute.firewalls.create`
- `iam.serviceAccounts.create`
- `resourcemanager.projects.setIamPolicy`

Resource khusus yang direncanakan (VM broker, subnet, dan worker service account) belum ada: pemeriksaan API mengembalikan HTTP 404. Ini merupakan hasil pemeriksaan IAM/resource GCP, bukan penolakan approval Codex. Script tidak mencoba meningkatkan izin sendiri.

Validasi lokal tahap ini: lima unit test preflight lulus; kedua script Bash lolos pemeriksaan sintaks; mode default script admin hanya mencetak rencana; package worker berhasil dibuat dari direktori temporary writable. Launcher CMD `daud-replay-20260916-02` berhasil memvalidasi graph 75 transform dengan `cloud_dataflow_executed=false`. Belum ada provisioning VM atau submit job cloud.

Bukti pemeriksaan tersimpan di `data/deployment_checks/`. Pemeriksaan tidak dapat membuktikan izin efektif worker atau konektivitas runtime; kedua hal tersebut diuji setelah resource tersedia. Gangguan DNS dilaporkan sebagai unknown, bukan disimpulkan sebagai penolakan IAM.

```bat
.venv\Scripts\python.exe scripts\cloud_streaming_preflight.py --use-local-adc
```

## Rencana resource untuk admin

Konfigurasi non-secret: `config/streaming_cloud.json`. Nama dan IP pada script provisioning serta launcher mengikuti konfigurasi ini; jika memakai resource existing, sesuaikan **ketiganya** sebelum menjalankan.

| Resource | Rencana |
|---|---|
| VPC / subnet | `daud-market` / `daud-market-jakarta`, `10.90.0.0/24` |
| VM Kafka | `daud-market-kafka`, `asia-southeast2-a`, e2-medium, COS, boot disk 20 GB |
| Broker | `10.90.0.10:9092`, 1 broker, 1 replica, Kafka 3.9.1 |
| Worker identity | `daud-dataflow-worker@jcdeah-009.iam.gserviceaccount.com` |
| Dataflow | Jakarta, maksimum 1 worker e2-standard-2, disk 30 GB |
| Data | Bucket dan tabel staging project yang sudah tersedia |

Kafka hanya bind ke IP privat. Firewall port 9092 menerima worker bertag `daud-market-worker`; SSH hanya menerima rentang IAP `35.235.240.0/20`. VM menggunakan external IP untuk download image, namun tidak membuka port Kafka publik. Worker demo menggunakan external IP untuk egress dependency/API; belum memakai Cloud NAT. Konfigurasi plaintext Kafka hanya untuk jaringan demo terisolasi ini, belum deployment produksi dengan TLS/SASL.

Pada Cloud Shell, dari root salinan repository:

```bash
# Tampilkan command, tanpa mengubah cloud:
bash deployment/admin_plan.sh

# Hanya admin yang sudah meninjau rencana dan durasi resource:
bash deployment/admin_plan.sh --apply
```

Script berhenti pada error dan tidak menimpa resource bernama sama. Jika sebagian provisioning sudah berhasil, admin perlu memeriksa resource lalu melanjutkan command yang belum selesai. Script belum diuji provisioning aktual karena izin tidak tersedia; yang telah diuji adalah sintaks Bash dan mode rencana.

Hak worker yang disiapkan:

- `roles/dataflow.worker` pada project.
- `roles/storage.objectAdmin` hanya pada bucket project `jcdeah-009-daud-finalproject`.
- `roles/bigquery.dataEditor` hanya pada tabel `stg_market_events`.

Admin juga perlu memastikan Dataflow service agent bawaan memiliki role `roles/dataflow.serviceAgent`. Role service agent tidak diberikan ke user atau worker biasa.

Untuk principal user yang akan submit, admin memberikan `roles/iam.serviceAccountUser` pada **worker service account tersebut**. Tidak diperlukan key JSON baru. Untuk menyalin sampel dan menjalankan producer di broker, admin menyediakan `roles/iap.tunnelResourceAccessor` terbatas ke VM dan `roles/compute.osAdminLogin` pada VM (atau menjalankan tahap producer sendiri). Akses baca metadata project/VM yang dibutuhkan IAP/OS Login perlu diverifikasi. Jangan memberikan Owner sebagai pengganti konfigurasi ini.

## Sesudah provisioning: kirim replay dari VM

Bundle lokal yang memuat producer, kontrak event, dan 100 record historis asli disiapkan dengan:

```bat
.venv\Scripts\python.exe scripts\prepare_cloud_replay.py
gcloud compute scp data/cloud_replay/replay_bundle.tar.gz daud-market-kafka:replay_bundle.tar.gz --project=jcdeah-009 --zone=asia-southeast2-a --tunnel-through-iap
gcloud compute ssh daud-market-kafka --project=jcdeah-009 --zone=asia-southeast2-a --tunnel-through-iap
```

`gcloud` memakai login CLI, sedangkan script Python memakai user ADC dengan flag `--use-local-adc`; pastikan principal keduanya sesuai. Setelah masuk terminal VM:

```bash
mkdir -p "$HOME/daud-replay"
tar -xzf "$HOME/replay_bundle.tar.gz" -C "$HOME/daud-replay"
sudo docker exec daud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server 10.90.0.10:9092 --list
sudo docker run --rm --network host -v "$HOME/daud-replay:/app:ro" python:3.11-slim-bookworm \
  sh -c 'pip install --no-cache-dir confluent-kafka==2.11.1 && python /app/producer.py --bootstrap 10.90.0.10:9092 --mode replay --input /app/replay.jsonl --limit 100 --interval 0.1 --output-root /tmp/producer-report'
```

Command ini baru dijalankan setelah topic siap. Retensi Kafka 24 jam memungkinkan Dataflow membaca dari earliest sesudah producer selesai. Waktu pengiriman dibuat ulang oleh producer; timestamp sumber dan mode replay tetap dipertahankan. Report di container ephemeral hilang saat container selesai, tetapi ringkasan tercetak pada terminal dan event tersimpan di Kafka.

## Submit dan validasi Dataflow

Dari CMD lokal, validasi graph dulu (belum submit):

```bat
deployment\submit_dataflow.cmd daud-replay-20260916-01
```

Setelah preflight lolos dan konektivitas/izin worker siap:

```bat
deployment\submit_dataflow.cmd daud-replay-20260916-01 --submit
```

Gunakan label baru untuk setiap run. Launcher memeriksa kesiapan resource sebelum submit, menetapkan maksimum satu worker dan tag firewall yang sesuai. Package Python disalin ke temporary directory writable sebelum staging, karena source code Docker di-mount read-only.

Periksa [job Dataflow](https://console.cloud.google.com/dataflow/jobs?project=jcdeah-009), task errors, metrik valid/invalid, file raw GCS di `final-project/market/<run-label>/raw`, lalu jumlah event pada staging dan view. Jangan menyatakan end-to-end berhasil hanya berdasarkan graph validation atau status RUNNING: record harus benar-benar muncul di sink dan direkonsiliasi ke Kafka.

Verifier berikut membandingkan setiap record raw GCS yang sudah final dengan staging berdasarkan topic/partition/offset, event identity, harga, timestamp, dan payload. Empat unit test memeriksa data kosong, record hilang, harga berbeda, dan duplikasi transport.

```bat
.venv\Scripts\python.exe scripts\verify_market_cloud.py --run-label daud-replay-20260916-01 --use-local-adc
```

Jika raw belum ada, hasilnya `not_ready` dengan exit nonzero. Jalankan setelah window final atau job selesai drain. Report teknis tersimpan di `data/streaming_validation/`; verifier dibatasi 10.000 record dan query maksimum 100 MiB. Duplikasi staging dihitung terpisah dan tidak otomatis menggagalkan pemeriksaan karena view downstream melakukan deduplikasi. Pemeriksaan ini membuktikan raw yang sudah tertangkap cocok dengan staging; jumlah raw tetap perlu dicocokkan dengan jumlah Kafka yang dikirim agar event yang belum tertangkap tidak terlewat.

## Selesai demo

Hentikan producer, drain job, lalu periksa state terminal:

```bat
gcloud dataflow jobs drain JOB_ID --project=jcdeah-009 --region=asia-southeast2
gcloud dataflow jobs describe JOB_ID --project=jcdeah-009 --region=asia-southeast2 --format="value(currentState)"
gcloud compute instances stop daud-market-kafka --project=jcdeah-009 --zone=asia-southeast2-a
```

Menunggu state `JOB_STATE_DRAINED` diperlukan; jika drain bermasalah, diagnosis lalu cancel bila dibutuhkan dengan mencatat kemungkinan output yang belum selesai. Jika user belum diberi izin stop VM, minta admin menjalankan command terakhir. Auto-stop broker dua jam **tidak menghentikan Dataflow** dan disk VM tetap tersimpan serta dapat dikenai biaya. Angka biaya belum diestimasi; jangan biarkan job aktif setelah sesi demo.

Referensi: [IAM Dataflow](https://docs.cloud.google.com/dataflow/docs/concepts/security-and-permissions), [firewall worker](https://docs.cloud.google.com/dataflow/docs/guides/routes-firewall), [batas durasi VM](https://docs.cloud.google.com/compute/docs/instances/limit-vm-runtime).
