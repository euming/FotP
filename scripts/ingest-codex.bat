@echo off
setlocal

:: ============================================================
:: ingest-codex.bat
::
:: Convert a Codex session JSONL log into an AMS memory browser.
::
:: Usage:
::   ingest-codex.bat <codex-session.jsonl>
::   ingest-codex.bat <codex-session.jsonl> <output-dir>
::
:: Output files are placed alongside the input file (or in
:: <output-dir> if supplied):
::   <stem>.chat.raw.jsonl   - intermediate chat_event file
::   <stem>.memory.jsonl     - AMS memory database
::   <stem>.cursor.json      - ingest cursor (resume on re-run)
::   <stem>.ams-debug.html   - HTML memory browser (auto-opened)
:: ============================================================

:: -- Paths you may need to adjust ----------------------------
:: MEMORYCTL is resolved relative to this script's location (scripts\ -> ..\tools\memoryctl).
set MEMORYCTL=%~dp0..\tools\memoryctl\MemoryCtl.csproj
set PYTHON=python
:: ------------------------------------------------------------

if "%~1"=="" (
    echo Usage: ingest-codex.bat ^<codex-session.jsonl^> [output-dir]
    exit /b 1
)

set INPUT=%~1
set STEM=%~n1

:: Output dir: use %2 if given, otherwise same folder as input
if not "%~2"=="" (
    set OUTDIR=%~2
) else (
    set OUTDIR=%~dp1
)

:: Strip trailing backslash from OUTDIR so paths stay clean
if "%OUTDIR:~-1%"=="\" set OUTDIR=%OUTDIR:~0,-1%

set RAW=%OUTDIR%\%STEM%.chat.raw.jsonl
set DB=%OUTDIR%\%STEM%.memory.jsonl
set CURSOR=%OUTDIR%\%STEM%.cursor.json
set HTML=%OUTDIR%\%STEM%.ams-debug.html

echo.
echo === Codex ingest pipeline ===
echo Input  : %INPUT%
echo OutDir : %OUTDIR%
echo.
if not exist "%OUTDIR%\" mkdir "%OUTDIR%"

:: -- Step 1: Convert Codex JSONL to chat_event format --------
echo [1/3] Converting Codex log to chat_event format...
%PYTHON% "%~dp0convert-codex.py" "%INPUT%" "%RAW%"
if errorlevel 1 (
    echo ERROR: Conversion failed.
    exit /b 1
)
echo.

:: -- Step 2: Ingest into AMS ---------------------------------
echo [2/3] Ingesting into AMS memory graph...
dotnet run --project "%MEMORYCTL%" -- ingest-chatlog ^
    --db "%DB%" ^
    --chatlog "%RAW%" ^
    --cursor "%CURSOR%" ^
    --max 500 --gap-min 30
if errorlevel 1 (
    echo ERROR: Ingest failed.
    exit /b 1
)
echo.

:: -- Step 3: Generate HTML memory browser --------------------
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
