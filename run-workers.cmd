@echo off
pwsh -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-workers.ps1"
exit /b %errorlevel%
