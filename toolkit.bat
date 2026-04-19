@echo off
cd /d "%~dp0"
:: Start PowerShell 7 direct
pwsh -NoProfile -ExecutionPolicy Bypass -File "%~dp0toolkit.ps1"
pause

