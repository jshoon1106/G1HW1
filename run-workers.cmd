@echo off
"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-workers.ps1" -NoPause %*
set "exitCode=%errorlevel%"
echo.
pause
exit /b %exitCode%
