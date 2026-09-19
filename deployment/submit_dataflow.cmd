@echo off
setlocal
cd /d "%~dp0.."
rem Broker and worker identity must first be provisioned by the project administrator.
rem Syntax: deployment\submit_dataflow.cmd UNIQUE_RUN_LABEL [--submit]
if "%~1"=="" (
  echo Supply a unique run label, for example daud-replay-20260916-01
  exit /b 2
)
set "ACTION=--validate-only"
if "%~2"=="--submit" set "ACTION="
if not "%~2"=="" if not "%~2"=="--submit" exit /b 2
if "%~2"=="--submit" (
  .venv\Scripts\python.exe scripts\cloud_streaming_preflight.py --use-local-adc
  if errorlevel 1 exit /b 1
)
docker compose --env-file .env.airflow -f compose.yaml -f compose.streaming.yaml --profile streaming run --rm --no-deps market beam_pipeline.py ^
  %ACTION% --bootstrap 10.90.0.10:9092 --topic market.prices.replay.v1 ^
  --consumer-group daud-replay-v1 --run-label "%~1" ^
  --output-prefix gs://jcdeah-009-daud-finalproject/final-project/market ^
  --table jcdeah-009:daud_finalproject.stg_market_events ^
  --runner DataflowRunner --project jcdeah-009 --region asia-southeast2 ^
  --job_name "%~1" ^
  --temp_location gs://jcdeah-009-daud-finalproject/final-project/dataflow/temp ^
  --staging_location gs://jcdeah-009-daud-finalproject/final-project/dataflow/staging ^
  --service_account_email daud-dataflow-worker@jcdeah-009.iam.gserviceaccount.com ^
  --subnetwork https://www.googleapis.com/compute/v1/projects/jcdeah-009/regions/asia-southeast2/subnetworks/daud-market-jakarta ^
  --num_workers 1 --max_num_workers 1 --machine_type e2-standard-2 ^
  --disk_size_gb 30 --experiments use_network_tags=daud-market-worker
exit /b %errorlevel%
