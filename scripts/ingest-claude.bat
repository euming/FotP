@echo off
setlocal

:: ============================================================
:: ingest-claude.bat
::
:: Convert Claude Code session JSONL(s) into an AMS memory browser.
::
:: Usage:
::   ingest-claude.bat <session.jsonl>          (single session)
::   ingest-claude.bat <sessions-dir>           (whole project directory)
::   ingest-claude.bat <input> <output-dir>     (custom output location)
::
:: The sessions-dir for a project is typically:
::   %USERPROFILE%\.claude\projects\<project-slug>\
::
:: Output files (named after the input stem):
::   <stem>.chat.raw.jsonl   - intermediate chat_event file
::   <stem>.memory.jsonl     - AMS memory database
::   <stem>.cursor.json      - ingest cursor (safe to re-run)
::   <stem>.ams-debug.html   - HTML memory browser (auto-opened)
:: ============================================================

:: -- Paths you may need to adjust ----------------------------
:: MEMORYCTL is resolved relative to this script's location (scripts\ -> ..\tools\memoryctl).
set MEMORYCTL=%~dp0..\tools\memoryctl\MemoryCtl.csproj
set PYTHON=python
:: ------------------------------------------------------------

if "%~1"=="" (
    echo Usage: ingest-claude.bat ^<session.jsonl^|sessions-dir^> [output-dir]
    exit /b 1
)

set INPUT=%~1

:: Derive a stem name for output files.
:: For a file: use the filename without extension.
:: For a directory: use the directory name itself.
set STEM=%~n1
if exist "%INPUT%\" (
    for %%D in ("%INPUT%\.") do set STEM=%%~nxD
)

:: Determine output directory.
if not "%~2"=="" (
    set OUTDIR=%~2
) else (
    if exist "%INPUT%\" (
        set OUTDIR=%INPUT%
    ) else (
        set OUTDIR=%~dp1
    )
)
if "%OUTDIR:~-1%"=="\" set OUTDIR=%OUTDIR:~0,-1%

set RAW=%OUTDIR%\%STEM%.chat.raw.jsonl
set DB=%OUTDIR%\%STEM%.memory.jsonl
set CURSOR=%OUTDIR%\%STEM%.cursor.json
set HTML=%OUTDIR%\%STEM%.ams-debug.html

echo.
echo === Claude Code ingest pipeline ===
echo Input  : %INPUT%
echo OutDir : %OUTDIR%
echo.
if not exist "%OUTDIR%\" mkdir "%OUTDIR%"

:: -- Step 1: Convert Claude Code JSONL(s) to chat_event format ----
echo [1/3] Converting Claude Code session(s)...
%PYTHON% "%~dp0convert-claude.py" "%INPUT%" "%RAW%"
if errorlevel 1 (
    echo ERROR: Conversion failed.
    exit /b 1
)
echo.

:: -- Step 2: Ingest into AMS ----------------------------------
:: gap-min=120 ensures 2-hour gaps separate distinct sessions;
:: Claude Code sessions are always farther apart than that.
echo [2/3] Ingesting into AMS memory graph...
dotnet run --project "%MEMORYCTL%" -- ingest-chatlog ^
    --db "%DB%" ^
    --chatlog "%RAW%" ^
    --cursor "%CURSOR%" ^
    --max 2000 --gap-min 120
if errorlevel 1 (
    echo ERROR: Ingest failed.
    exit /b 1
)
echo.

:: -- Step 3: Generate HTML memory browser ---------------------
echo [3/3] Building HTML memory browser...
dotnet run --project "%MEMORYCTL%" -- debug-ams ^
    --db "%DB%" ^
    --out "%HTML%"
if errorlevel 1 (
    echo ERROR: HTML generation failed.
    exit /b 1
)
echo.

echo === Done! ===
echo HTML browser: %HTML%
echo.
start "" "%HTML%"
