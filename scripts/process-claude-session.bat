@echo off
setlocal

:: ============================================================
:: process-claude-session.bat
::
:: Full pipeline: single .jsonl chat log -> ingest -> dream -> HTML.
::
:: Usage:
::   process-claude-session.bat <session.jsonl>
::   process-claude-session.bat <session.jsonl> <output-dir>
::   process-claude-session.bat <session.jsonl> <output-dir> --dry-run
::   process-claude-session.bat <session.jsonl> <output-dir> "" <relax-steps>
::
:: Arguments:
::   session.jsonl   Claude Code raw session JSONL (required)
::   output-dir      Where to write output files (default: same dir as input)
::   --dry-run       3rd arg: run analysis but do not write AMS changes to disk
::   relax-steps     4th arg: energy relaxation steps after dreaming (default: 0 = skip)
::
:: Output files (named after the input stem):
::   <stem>.chat.raw.jsonl   - intermediate chat_event file
::   <stem>.memory.jsonl     - AMS memory database
::   <stem>.cursor.json      - ingest cursor (safe to re-run)
::   <stem>.ams-debug.html   - HTML memory browser (auto-opened)
:: ============================================================

set MEMORYCTL=%~dp0..\tools\memoryctl\MemoryCtl.csproj
set PYTHON=python

if "%~1"=="" goto :usage

set INPUT=%~1
set STEM=%~n1

if not "%~2"=="" (set OUTDIR=%~2) else (set OUTDIR=%~dp1)
if "%OUTDIR:~-1%"=="\" set OUTDIR=%OUTDIR:~0,-1%

set DRY_RUN=
set RELAX_STEPS=0
if /i "%~3"=="--dry-run" set DRY_RUN=1
if not "%~4"=="" set RELAX_STEPS=%~4

set RAW=%OUTDIR%\%STEM%.chat.raw.jsonl
set DB=%OUTDIR%\%STEM%.memory.jsonl
set CURSOR=%OUTDIR%\%STEM%.cursor.json
set HTML=%OUTDIR%\%STEM%.ams-debug.html

echo.
echo === Claude session pipeline ===
echo Input        : %INPUT%
echo OutDir       : %OUTDIR%
if defined DRY_RUN echo Mode         : DRY RUN (no writes)
if not "%RELAX_STEPS%"=="0" echo Relax steps  : %RELAX_STEPS%
echo.

if not exist "%INPUT%" goto :err_no_input
if not exist "%OUTDIR%\" mkdir "%OUTDIR%"

:: ----------------------------------------------------------------
:: Step 1: Convert
:: ----------------------------------------------------------------
echo [1/5] Converting session JSONL to chat_event format...
%PYTHON% "%~dp0convert-claude.py" "%INPUT%" "%RAW%"
if errorlevel 1 goto :err_convert
echo.

:: ----------------------------------------------------------------
:: Step 2: Ingest
:: ----------------------------------------------------------------
echo [2/5] Ingesting into AMS memory graph...
dotnet run --project "%MEMORYCTL%" -- ingest-chatlog --db "%DB%" --chatlog "%RAW%" --cursor "%CURSOR%" --max 2000 --gap-min 120
if errorlevel 1 goto :err_ingest
echo.

:: ----------------------------------------------------------------
:: Step 3: Dreaming
:: ----------------------------------------------------------------
echo [3/5] Running Dreaming pipeline...
if defined DRY_RUN goto :dream_dry
dotnet run --project "%MEMORYCTL%" -- dream --db "%DB%"
if errorlevel 1 goto :err_dream
echo.
goto :step4

:dream_dry
dotnet run --project "%MEMORYCTL%" -- dream --db "%DB%" --dry-run
if errorlevel 1 goto :err_dream
echo.

:: ----------------------------------------------------------------
:: Step 4: Energy relaxation (optional)
:: ----------------------------------------------------------------
:step4
if "%RELAX_STEPS%"=="0" goto :step4_skip
echo [4/5] Running energy relaxation (%RELAX_STEPS% steps)...
if defined DRY_RUN goto :relax_dry
dotnet run --project "%MEMORYCTL%" -- dream-relax --db "%DB%" --steps %RELAX_STEPS%
if errorlevel 1 goto :err_relax
echo.
goto :step5

:relax_dry
dotnet run --project "%MEMORYCTL%" -- dream-relax --db "%DB%" --steps %RELAX_STEPS% --dry-run
if errorlevel 1 goto :err_relax
echo.
goto :step5

:step4_skip
echo [4/5] Energy relaxation skipped (pass relax-steps as 4th arg to enable).
echo.

:: ----------------------------------------------------------------
:: Step 5: HTML
:: ----------------------------------------------------------------
:step5
if defined DRY_RUN goto :done_dry
echo [5/5] Building HTML memory browser...
dotnet run --project "%MEMORYCTL%" -- debug-ams --db "%DB%" --out "%HTML%"
if errorlevel 1 goto :err_html
echo.
echo === Done! ===
echo HTML browser : %HTML%
echo.
start "" "%HTML%"
exit /b 0

:done_dry
echo [5/5] Skipped HTML generation (dry-run).
echo.
echo === Done (dry-run) ===
exit /b 0

:: ----------------------------------------------------------------
:: Errors
:: ----------------------------------------------------------------
:usage
echo Usage: process-claude-session.bat ^<session.jsonl^> [output-dir] [--dry-run] [relax-steps]
exit /b 1

:err_no_input
echo ERROR: Input file not found: %INPUT%
exit /b 1

:err_convert
echo ERROR: Conversion failed.
exit /b 1

:err_ingest
echo ERROR: Ingest failed.
exit /b 1

:err_dream
echo ERROR: Dreaming failed.
exit /b 1

:err_relax
echo ERROR: Energy relaxation failed.
exit /b 1

:err_html
echo ERROR: HTML generation failed.
exit /b 1
