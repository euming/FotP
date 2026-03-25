@echo off
setlocal EnableDelayedExpansion

:: ============================================================
:: ingest-all-sessions.bat
::
:: Generic all-sessions ingest wrapper.
::
:: Usage:
::   ingest-all-sessions.bat [source] [output-dir] [root-dir] [project-filter]
::
:: Source:
::   all     -> combine claude + codex into all-agents-sessions.*
::   claude  -> ingest-all-claude-projects.py over %USERPROFILE%\.claude\projects
::   codex   -> ingest-all-codex.py over %USERPROFILE%\.codex\sessions
:: ============================================================

set SCRIPT_DIR=%~dp0
if not exist "%SCRIPT_DIR%ingest-all-sessions.bat" set SCRIPT_DIR=%CD%\scripts\
set MEMORYCTL=%SCRIPT_DIR%..\tools\memoryctl\MemoryCtl.csproj
set PYTHON=python
set NO_BROWSER=%AMS_NO_BROWSER%
set SKIP_HTML=%AMS_SKIP_HTML%

set SOURCE=all
if /i "%~1"=="all" (
    set SOURCE=all
    shift
) else if /i "%~1"=="claude" (
    set SOURCE=claude
    shift
) else if /i "%~1"=="codex" (
    set SOURCE=codex
    shift
)

if /i "%SOURCE%"=="claude" (
    set STEM=all-claude-projects
    set CONVERTER=%SCRIPT_DIR%ingest-all-claude-projects.py
    set ROOT_ARG=--projects-dir
    set DEFAULT_ROOT=%USERPROFILE%\.claude\projects
    set INGEST_MAX=5000
) else if /i "%SOURCE%"=="codex" (
    set STEM=all-codex-sessions
    set CONVERTER=%SCRIPT_DIR%ingest-all-codex.py
    set ROOT_ARG=--sessions-dir
    set DEFAULT_ROOT=%USERPROFILE%\.codex\sessions
    set INGEST_MAX=20000
) else if /i "%SOURCE%"=="all" (
    set STEM=all-agents-sessions
    set DEFAULT_ROOT=%USERPROFILE%
    set INGEST_MAX=25000
) else (
    echo ERROR: Unknown source "%SOURCE%". Use "all", "claude", or "codex".
    goto usage_fail
)

if not "%~1"=="" (
    set OUTDIR=%~1
) else (
    set OUTDIR=%SCRIPT_DIR%output\%STEM%
)

if not "%~2"=="" (
    set ROOT_DIR=%~2
) else (
    set ROOT_DIR=%DEFAULT_ROOT%
)

if not "%~3"=="" (
    set PROJECT_FILTER=--project "%~3"
) else (
    set PROJECT_FILTER=
)

if "%OUTDIR:~-1%"=="\" set OUTDIR=%OUTDIR:~0,-1%

set RAW=%OUTDIR%\%STEM%.chat.raw.jsonl
set DB=%OUTDIR%\%STEM%.memory.jsonl
set CURSOR=%OUTDIR%\%STEM%.cursor.json
set HTML=%OUTDIR%\%STEM%.ams-debug.html

echo.
echo === All-sessions ingest pipeline ===
echo Source        : %SOURCE%
echo Root dir      : %ROOT_DIR%
echo Project filter: %PROJECT_FILTER%
echo OutDir        : %OUTDIR%
echo.
if not exist "%OUTDIR%\" mkdir "%OUTDIR%"

if /i "%SOURCE%"=="all" (
    set RAW_CLAUDE=%OUTDIR%\%STEM%.claude.chat.raw.jsonl
    set RAW_CODEX=%OUTDIR%\%STEM%.codex.chat.raw.jsonl
    set CURSOR_CLAUDE=%OUTDIR%\%STEM%.claude.cursor.json
    set CURSOR_CODEX=%OUTDIR%\%STEM%.codex.cursor.json
    set HAVE_CLAUDE=0
    set HAVE_CODEX=0

    set CLAUDE_ROOT=%ROOT_DIR%\.claude\projects
    set CODEX_ROOT=%ROOT_DIR%\.codex\sessions

    echo [1/3] Discovering and converting sessions for source=all...
    if exist "!CLAUDE_ROOT!\" (
        echo   [claude] root: !CLAUDE_ROOT!
        %PYTHON% "%SCRIPT_DIR%ingest-all-claude-projects.py" ^
            --projects-dir "!CLAUDE_ROOT!" ^
            --out "!RAW_CLAUDE!" ^
            %PROJECT_FILTER%
        if errorlevel 1 (
            echo   [claude] WARNING: conversion failed - skipping claude input.
        ) else (
            set HAVE_CLAUDE=1
        )
    ) else (
        echo   [claude] WARNING: root not found - skipping: !CLAUDE_ROOT!
    )

    if exist "!CODEX_ROOT!\" (
        echo   [codex] root: !CODEX_ROOT!
        %PYTHON% "%SCRIPT_DIR%ingest-all-codex.py" ^
            --sessions-dir "!CODEX_ROOT!" ^
            --out "!RAW_CODEX!" ^
            %PROJECT_FILTER%
        if errorlevel 1 (
            echo   [codex] WARNING: conversion failed - skipping codex input.
        ) else (
            set HAVE_CODEX=1
        )
    ) else (
        echo   [codex] WARNING: root not found - skipping: !CODEX_ROOT!
    )

    if "!HAVE_CLAUDE!"=="0" if "!HAVE_CODEX!"=="0" (
        echo ERROR: No usable claude/codex input found for source=all.
        exit /b 1
    )

    echo.

    :: Ingest each source independently into the shared DB, with per-source cursors.
    :: This preserves cursor validity across incremental runs (each raw file only grows
    :: by appending; the per-source cursor always points to valid line positions).
    echo [2/3] Ingesting into AMS memory graph...
    if "!HAVE_CLAUDE!"=="1" (
        echo   [claude] ingesting...
        dotnet run --project "%MEMORYCTL%" -- ingest-chatlog ^
            --db "%DB%" ^
            --chatlog "!RAW_CLAUDE!" ^
            --cursor "!CURSOR_CLAUDE!" ^
            --max %INGEST_MAX% --gap-min 120
        if errorlevel 1 (
            echo ERROR: Claude ingest failed.
            exit /b 1
        )
    )
    if "!HAVE_CODEX!"=="1" (
        echo   [codex] ingesting...
        dotnet run --project "%MEMORYCTL%" -- ingest-chatlog ^
            --db "%DB%" ^
            --chatlog "!RAW_CODEX!" ^
            --cursor "!CURSOR_CODEX!" ^
            --max %INGEST_MAX% --gap-min 120
        if errorlevel 1 (
            echo ERROR: Codex ingest failed.
            exit /b 1
        )
    )
    echo.

    if /i "%SKIP_HTML%"=="1" (
        echo [3/3] Skipping HTML memory browser rebuild ^(AMS_SKIP_HTML=1^).
        echo.
        echo === Done! ===
        echo DB           : %DB%
        if "!HAVE_CLAUDE!"=="1" echo Chat raw     : !RAW_CLAUDE!
        if "!HAVE_CODEX!"=="1"  echo Chat raw     : !RAW_CODEX!
        exit /b 0
    )

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
    if /i not "%NO_BROWSER%"=="1" start "" "%HTML%"
    exit /b 0
) else (
    echo [1/3] Discovering and converting sessions...
    %PYTHON% "%CONVERTER%" ^
        %ROOT_ARG% "%ROOT_DIR%" ^
        --out "%RAW%" ^
        %PROJECT_FILTER%
    if errorlevel 1 (
        echo ERROR: Conversion failed.
        exit /b 1
    )
)
echo.

echo [2/3] Ingesting into AMS memory graph...
dotnet run --project "%MEMORYCTL%" -- ingest-chatlog ^
    --db "%DB%" ^
    --chatlog "%RAW%" ^
    --cursor "%CURSOR%" ^
    --max %INGEST_MAX% --gap-min 120
if errorlevel 1 (
    echo ERROR: Ingest failed.
    exit /b 1
)
echo.

if /i "%SKIP_HTML%"=="1" (
    echo [3/3] Skipping HTML memory browser rebuild ^(AMS_SKIP_HTML=1^).
    echo.
    echo === Done! ===
    echo DB           : %DB%
    echo Chat raw     : %RAW%
    exit /b 0
)

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
if /i not "%NO_BROWSER%"=="1" start "" "%HTML%"
exit /b 0

:usage
echo Usage: ingest-all-sessions.bat [source] [output-dir] [root-dir] [project-filter]
echo   source = all ^| claude ^| codex   ^(default: all^)
echo   root-dir for source=all is the user root that contains .claude and .codex
exit /b 1

:usage_fail
exit /b 1
