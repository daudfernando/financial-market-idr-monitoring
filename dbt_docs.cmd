@echo off
cd /d "%~dp0"
.venv\Scripts\python.exe scripts\serve_dbt_docs.py %*
exit /b %errorlevel%
