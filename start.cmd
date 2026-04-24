@echo off
setlocal
PowerShell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1" %*
endlocal
