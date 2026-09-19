@echo off
cd /d "%~dp0"
.venv\Scripts\python.exe scripts\show_kafka.py %*
exit /b %errorlevel%
