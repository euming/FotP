@echo off
setlocal

:: ============================================================
:: update-memory.bat
::
:: One-command refresh of ALL Claude project memory files.
::
:: Runs the full pipeline:
::   1. Delete per-project cursor files (forces complete re-ingest)
::   2. maintain-claude-memory.bat  -> writes CLAUDE.local.md to each repo
::   3. ingest-all-claude-projects.bat -> combined global AMS database
::   4. dream-all-claude-projects.bat  -> dream nodes on global database
::
:: Steps 3-4 are optional (global HTML browser only) and can be
:: skipped by passing --no-global-browser.
::
:: Usage:
::   update-memory.bat
::   update-memory.bat --no-global-browser
::   update-memory.bat --project AMS
::   update-memory.bat --dry-run
::   update-memory.bat --no-delete-cursors
::
:: Options forwarded to maintain-claude-memory.bat:
::   --project <name>    Limit to one project
::   --dry-run           Analyse but do not write CLAUDE.local.md files
::   --topic-k <n>       Max topic winners (default: 5)
::   --thread-k <n>      Max thread winners (default: 3)
::   --decision-k <n>    Max decision winners (default: 3)
::   --invariant-k <n>   Max invariant winners (default: 3)
::
:: Own options (consumed here, not forwarded):
::   --no-global-browser   Skip steps 3-4 (global HTML browser)
::   --no-delete-cursors   Keep existing cursor files (faster but may miss
::                         new sessions; see README.md for details)
:: ============================================================

set SCRIPT_DIR=%~dp0
set MAINTAIN=%SCRIPT_DIR%maintain-claude-memory.bat
set INGEST_ALL=%SCRIPT_DIR%ingest-all-claude-projects.bat
set DREAM_ALL=%SCRIPT_DIR%dream-all-claude-projects.bat
set PER_PROJECT_OUTPUT=%SCRIPT_DIR%output\per-project
set ALL_PROJECTS_OUTPUT=%SCRIPT_DIR%output\all-claude-projects

:: -- Parse own flags, collect pass-through args ------------------
set RUN_GLOBAL_BROWSER=1
set DELETE_CURSORS=1
set PASSTHROUGH=

:parse_args
if "%~1"=="" goto :end_parse
if /i "%~1"=="--no-global-browser" (
    set RUN_GLOBAL_BROWSER=0
    shift
    goto :parse_args
)
if /i "%~1"=="--no-wipe" (
    set DELETE_CURSORS=0
    shift
    goto :parse_args
)
:: Pass everything else through to maintain-claude-memory.bat
set PASSTHROUGH=%PASSTHROUGH% %1
shift
goto :parse_args
:end_parse
:: ----------------------------------------------------------------

echo.
echo ================================================================
echo  update-memory  ^|  Full Claude memory refresh
echo ================================================================
echo.

:: -- Step 0: Delete cursor files so all sessions are re-ingested --
if "%DELETE_CURSORS%"=="1" (
    echo [0/4] Wiping per-project databases for clean re-ingest...
    echo.
    echo   Reason: the raw JSONL is sorted by timestamp, but old databases may
    echo   contain stale merged sessions from before this fix was applied.
    echo   A fresh ingest guarantees correct session boundaries every time.
    echo.
    if exist "%PER_PROJECT_OUTPUT%" (
        for /r "%PER_PROJECT_OUTPUT%" %%f in (*.cursor.json) do (
            echo   Deleted cursor : %%f
            del "%%f"
        )
        for /r "%PER_PROJECT_OUTPUT%" %%f in (*.memory.jsonl) do (
            echo   Deleted memory : %%f
            del "%%f"
        )
    )
    echo   Done.
    echo.
) else (
    echo [0/4] Skipping database wipe ^(--no-wipe^).
    echo.
)

:: -- Step 1-4 of maintain pipeline: per-project CLAUDE.local.md --
echo [1/4] Running per-project pipeline ^(ingest -^> dream -^> CLAUDE.local.md^)...
echo       Args: %PASSTHROUGH%
echo.
call "%MAINTAIN%" %PASSTHROUGH%
if errorlevel 1 (
    echo.
    echo ERROR: maintain-claude-memory failed.
    exit /b 1
)
echo.

:: -- Step 2: Global combined ingest --------------------------------
if "%RUN_GLOBAL_BROWSER%"=="0" (
    echo [2/4] Skipping global browser ^(--no-global-browser^).
    echo [3/4] Skipping global dream   ^(--no-global-browser^).
    echo [4/4] Skipping global HTML    ^(--no-global-browser^).
    goto :done
)

echo [2/4] Ingesting all projects into combined AMS database...
call "%INGEST_ALL%" "%ALL_PROJECTS_OUTPUT%"
if errorlevel 1 (
    echo.
    echo ERROR: ingest-all-claude-projects failed.
    exit /b 1
)
echo.

:: -- Step 3: Global dream ------------------------------------------
echo [3/4] Running Dreaming on combined database...
call "%DREAM_ALL%" "%ALL_PROJECTS_OUTPUT%"
if errorlevel 1 (
    echo.
    echo ERROR: dream-all-claude-projects failed.
    exit /b 1
)
echo.

:done
echo ================================================================
echo  All done!
echo.
echo  CLAUDE.local.md has been written to each project repository.
echo  Recovery snapshots were also written under memory.archive\claude-local\.
echo  Claude Code loads it automatically alongside CLAUDE.md.
echo ================================================================
echo.
