@echo off
cd /d "%~dp0"
.venv\Scripts\python.exe scripts\prepare_demo.py %*
exit /b %errorlevel%
