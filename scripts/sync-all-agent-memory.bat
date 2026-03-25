@echo off
setlocal

set SCRIPT_DIR=%~dp0
if not exist "%SCRIPT_DIR%sync-all-agent-memory.bat" set SCRIPT_DIR=%CD%\scripts\
for %%I in ("%SCRIPT_DIR%..") do set "PROJECT_NAME=%%~nxI"

if defined AMS_OUTPUT_ROOT (
    set "OUTDIR=%AMS_OUTPUT_ROOT%\all-agents-sessions"
) else if defined LOCALAPPDATA (
    set "OUTDIR=%LOCALAPPDATA%\%PROJECT_NAME%\agent-memory\all-agents-sessions"
) else (
    set "OUTDIR=%SCRIPT_DIR%output\all-agents-sessions"
)
set NO_BROWSER=0
set MEMORYCTL_EXE=%SCRIPT_DIR%..\tools\memoryctl\bin\Release\net9.0\MemoryCtl.exe
if not exist "%MEMORYCTL_EXE%" set MEMORYCTL_EXE=%SCRIPT_DIR%..\tools\memoryctl\bin\Debug\net9.0\MemoryCtl.exe
set DB=%OUTDIR%\all-agents-sessions.memory.jsonl

:parse_args
if "%~1"=="" goto args_done
if /i "%~1"=="--no-browser" (
    set NO_BROWSER=1
    shift
    goto parse_args
)
if /i "%~1"=="--help" goto usage
if /i "%~1"=="-h" goto usage
if not "%~1"=="" if "%~1:~0,2%"=="--" (
    echo ERROR: Unknown option "%~1".
    goto usage_fail
)
set OUTDIR=%~1
shift
goto parse_args

:args_done
if "%OUTDIR:~-1%"=="\" set OUTDIR=%OUTDIR:~0,-1%
if not exist "%OUTDIR%\" mkdir "%OUTDIR%"

set LOCKDIR=%OUTDIR%\.sync.lock
mkdir "%LOCKDIR%" >nul 2>&1
if errorlevel 1 (
    echo Sync already running for %OUTDIR% - skipping.
    exit /b 0
)

set AMS_SKIP_HTML=1
set AMS_NO_BROWSER=1
if "%NO_BROWSER%"=="0" set AMS_NO_BROWSER=0

echo.
echo === Sync All Agent Memory ===
echo OutDir       : %OUTDIR%
echo Browser      : %AMS_NO_BROWSER%
echo.

call "%SCRIPT_DIR%ingest-all-sessions.bat" all "%OUTDIR%"
if errorlevel 1 goto sync_fail

call "%SCRIPT_DIR%dream-all-sessions.bat" all "%OUTDIR%"
if errorlevel 1 goto sync_fail

echo Refreshing agent-memory summaries and freshness...
if exist "%MEMORYCTL_EXE%" (
    "%MEMORYCTL_EXE%" agent-maintain --db "%DB%"
) else (
    dotnet run --project "%SCRIPT_DIR%..\tools\memoryctl\MemoryCtl.csproj" -- agent-maintain --db "%DB%"
)
if errorlevel 1 goto sync_fail

echo Running FEP tool bootstrap + anomaly detection...
set AMS_KERNEL=%SCRIPT_DIR%..\rust\ams-core-kernel\target\release\ams-core-kernel.exe
if not exist "%AMS_KERNEL%" set AMS_KERNEL=%SCRIPT_DIR%..\rust\ams-core-kernel\target\debug\ams-core-kernel.exe
if exist "%AMS_KERNEL%" (
    "%AMS_KERNEL%" fep-bootstrap-agent-tools --input "%DB%"
    if errorlevel 1 echo WARNING: Tool prior bootstrap failed ^(non-fatal^).
    "%AMS_KERNEL%" fep-detect-tool-anomalies --input "%DB%" --since last-run --threshold 2.0
    if errorlevel 1 echo WARNING: Tool anomaly detection failed ^(non-fatal^).
) else (
    echo SKIP: ams-core-kernel binary not found, skipping FEP pipeline.
)

echo Running FEP repair trigger...
python "%SCRIPT_DIR%fep-repair-trigger.py" --db "%DB%" 2>nul
if errorlevel 1 echo WARNING: FEP repair trigger failed ^(non-fatal^).

echo Generating FEP tool health report...
python "%SCRIPT_DIR%fep-tool-health-report.py" --db "%DB%" 2>nul
if errorlevel 1 echo WARNING: FEP tool health report failed ^(non-fatal^).

echo Sync complete.
rd "%LOCKDIR%" >nul 2>&1
exit /b 0

:sync_fail
set EXIT_CODE=%ERRORLEVEL%
echo ERROR: sync-all-agent-memory failed with exit code %EXIT_CODE%.
rd "%LOCKDIR%" >nul 2>&1
exit /b %EXIT_CODE%

:usage
echo Usage: sync-all-agent-memory.bat [output-dir] [--no-browser]
echo   output-dir   Optional. Defaults to %%AMS_OUTPUT_ROOT%%\all-agents-sessions or %%LOCALAPPDATA%%\%PROJECT_NAME%\agent-memory\all-agents-sessions
echo   --no-browser Suppress opening the final HTML browser window
exit /b 0

:usage_fail
exit /b 1
