@echo off
setlocal

set SCRIPT_DIR=%~dp0
if not exist "%SCRIPT_DIR%start-agent-memory-watch.bat" set SCRIPT_DIR=%CD%\scripts\

set WATCH_BAT=%SCRIPT_DIR%watch-all-agent-memory.bat
if defined AMS_OUTPUT_ROOT (
    set "OUTDIR=%AMS_OUTPUT_ROOT%\all-agents-sessions"
) else if defined LOCALAPPDATA (
    set "OUTDIR=%LOCALAPPDATA%\NetworkGraphMemory\agent-memory\all-agents-sessions"
) else (
    set "OUTDIR=%SCRIPT_DIR%output\all-agents-sessions"
)
set LOG_DIR=%OUTDIR%\logs
set LOG_FILE=%LOG_DIR%\watch-all-agent-memory.log

if not exist "%WATCH_BAT%" (
    echo ERROR: watcher script not found: %WATCH_BAT%
    exit /b 1
)

if not exist "%LOG_DIR%\" mkdir "%LOG_DIR%" >nul 2>&1

start "NetworkGraphMemory Agent Memory Watch" /min cmd /c ""%WATCH_BAT%" --initial-sync >> "%LOG_FILE%" 2>&1"
if errorlevel 1 (
    echo ERROR: failed to start watcher process.
    exit /b 1
)

echo Started watcher process. Log: %LOG_FILE%
exit /b 0
