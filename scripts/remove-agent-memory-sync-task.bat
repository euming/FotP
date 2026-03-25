@echo off
setlocal

set TASK_NAME=NetworkGraphMemory-AgentMemoryWatch
set STARTUP_FILE=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\NetworkGraphMemory-AgentMemoryWatch.cmd
if defined AMS_OUTPUT_ROOT (
    set "OUTDIR=%AMS_OUTPUT_ROOT%\all-agents-sessions"
) else if defined LOCALAPPDATA (
    set "OUTDIR=%LOCALAPPDATA%\NetworkGraphMemory\agent-memory\all-agents-sessions"
) else (
    set "OUTDIR=%~dp0output\all-agents-sessions"
)
set WATCH_LOCK=%OUTDIR%\.watch.lock

schtasks /End /TN "%TASK_NAME%" >nul 2>&1
schtasks /Delete /F /TN "%TASK_NAME%" >nul 2>&1
set REMOVED_ANY=0

if not errorlevel 1 (
    set REMOVED_ANY=1
    echo Removed scheduled task: %TASK_NAME%
)

if exist "%STARTUP_FILE%" (
    del /F /Q "%STARTUP_FILE%" >nul 2>&1
    if not errorlevel 1 (
        set REMOVED_ANY=1
        echo Removed Startup launcher: %STARTUP_FILE%
    )
)

if exist "%WATCH_LOCK%" (
    for /f "usebackq delims=" %%P in ("%WATCH_LOCK%") do set WATCH_PID=%%P
)

if defined WATCH_PID (
    powershell -NoProfile -ExecutionPolicy Bypass -Command ^
      "$pidValue = [int]'%WATCH_PID%';" ^
      "try { Stop-Process -Id $pidValue -Force -ErrorAction Stop; exit 0 } catch { exit 1 }"
    if not errorlevel 1 (
        set REMOVED_ANY=1
        echo Stopped watcher process: %WATCH_PID%
    )
)

if "%REMOVED_ANY%"=="0" (
    echo No scheduled task, Startup launcher, or watcher process found.
    exit /b 1
)

exit /b 0
