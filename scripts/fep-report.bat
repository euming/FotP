@echo off
setlocal

:: FEP Tool Health Report — shows tool success rates and trends.
::
:: Usage:
::   fep-report.bat              (default: 7d vs 30d comparison)
::   fep-report.bat 3 14         (3d recent vs 14d baseline)

set SCRIPT_DIR=%~dp0
set DB=%SCRIPT_DIR%output\all-agents-sessions\all-agents-sessions.memory.jsonl

set RECENT=7
set BASELINE=30
if not "%~1"=="" set RECENT=%~1
if not "%~2"=="" set BASELINE=%~2

python "%SCRIPT_DIR%fep-tool-health-report.py" --db "%DB%" --recent-days %RECENT% --baseline-days %BASELINE%
