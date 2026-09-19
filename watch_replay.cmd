@echo off
cd /d "%~dp0"
.venv\Scripts\python.exe scripts\watch_replay.py
exit /b %errorlevel%
