@echo off
cd /d "%~dp0"
docker compose --env-file .env.airflow -f compose.yaml -f compose.streaming.yaml --profile streaming up -d --wait kafka
if errorlevel 1 exit /b 1
docker compose --env-file .env.airflow -f compose.yaml -f compose.streaming.yaml --profile streaming run --rm market incremental_demo.py %*
exit /b %errorlevel%
