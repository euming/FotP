@echo off
setlocal

set SCRIPT_DIR=%~dp0
if not exist "%SCRIPT_DIR%sync-short-term-agent-memory.bat" set SCRIPT_DIR=%CD%\scripts\
for %%I in ("%SCRIPT_DIR%..") do set "PROJECT_NAME=%%~nxI"

if defined AMS_OUTPUT_ROOT (
    set "OUTDIR=%AMS_OUTPUT_ROOT%\all-agents-sessions"
) else if defined LOCALAPPDATA (
    set "OUTDIR=%LOCALAPPDATA%\%PROJECT_NAME%\agent-memory\all-agents-sessions"
) else (
    set "OUTDIR=%SCRIPT_DIR%output\all-agents-sessions"
)
set NO_BROWSER=0

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

set LOCKDIR=%OUTDIR%\.short-term-sync.lock
mkdir "%LOCKDIR%" >nul 2>&1
if errorlevel 1 (
    echo Short-term sync already running for %OUTDIR% - skipping.
    exit /b 0
)

set AMS_SKIP_HTML=1
set AMS_NO_BROWSER=1
if "%NO_BROWSER%"=="0" set AMS_NO_BROWSER=0

echo.
echo === Sync Short-Term Agent Memory ===
echo OutDir       : %OUTDIR%
echo Browser      : %AMS_NO_BROWSER%
echo.

call "%SCRIPT_DIR%ingest-all-sessions.bat" all "%OUTDIR%"
if errorlevel 1 goto sync_fail

echo Short-term sync complete.
rd "%LOCKDIR%" >nul 2>&1
exit /b 0

:sync_fail
set EXIT_CODE=%ERRORLEVEL%
echo ERROR: sync-short-term-agent-memory failed with exit code %EXIT_CODE%.
rd "%LOCKDIR%" >nul 2>&1
exit /b %EXIT_CODE%

:usage
echo Usage: sync-short-term-agent-memory.bat [output-dir] [--no-browser]
echo   output-dir   Optional. Defaults to %%AMS_OUTPUT_ROOT%%\all-agents-sessions or %%LOCALAPPDATA%%\%PROJECT_NAME%\agent-memory\all-agents-sessions
echo   --no-browser Suppress opening the final HTML browser window
exit /b 0

:usage_fail
exit /b 1
