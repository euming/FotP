@echo off
setlocal

:: ============================================================
:: dream-all-sessions.bat
::
:: Generic all-sessions dream wrapper.
::
:: Usage:
::   dream-all-sessions.bat [source] [output-dir] [topic-k] [thread-k] [decision-k] [invariant-k] [--dry-run] [relax-steps]
::
:: Source:
::   all    -> stem all-agents-sessions
::   claude -> stem all-claude-projects
::   codex  -> stem all-codex-sessions
:: ============================================================

set SCRIPT_DIR=%~dp0
if not exist "%SCRIPT_DIR%dream-all-sessions.bat" set SCRIPT_DIR=%CD%\scripts\
set MEMORYCTL=%SCRIPT_DIR%..\tools\memoryctl\MemoryCtl.csproj
set SCRIPTS=%SCRIPT_DIR%
set NO_BROWSER=%AMS_NO_BROWSER%

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

if /i "%SOURCE%"=="all" (
    set STEM=all-agents-sessions
) else if /i "%SOURCE%"=="claude" (
    set STEM=all-claude-projects
) else if /i "%SOURCE%"=="codex" (
    set STEM=all-codex-sessions
) else (
    echo ERROR: Unknown source "%SOURCE%". Use "all", "claude", or "codex".
    goto :usage_fail
)

if not "%~1"=="" (
    set OUTDIR=%~1
) else (
    set OUTDIR=%SCRIPT_DIR%output\%STEM%
)

if "%OUTDIR:~-1%"=="\" set OUTDIR=%OUTDIR:~0,-1%

set TOPIC_K=5
set THREAD_K=3
set DECISION_K=3
set INVARIANT_K=3
set DRY_RUN_FLAG=
set RELAX_STEPS=0

if not "%~2"=="" set TOPIC_K=%~2
if not "%~3"=="" set THREAD_K=%~3
if not "%~4"=="" set DECISION_K=%~4
if not "%~5"=="" set INVARIANT_K=%~5
if /i "%~6"=="--dry-run" set DRY_RUN_FLAG=--dry-run
if not "%~7"=="" set RELAX_STEPS=%~7

set DB=%OUTDIR%\%STEM%.memory.jsonl
set AMS_JSON=%OUTDIR%\%STEM%.memory.ams.json
set EMBEDDINGS_JSON=%OUTDIR%\%STEM%.memory.embeddings.json
set HTML=%OUTDIR%\%STEM%.ams-debug.html

echo.
echo === Dreaming pipeline for %SOURCE% sessions ===
echo DB           : %DB%
echo Topic-K      : %TOPIC_K%
echo Thread-K     : %THREAD_K%
echo Decision-K   : %DECISION_K%
echo Invariant-K  : %INVARIANT_K%
if not "%DRY_RUN_FLAG%"=="" echo Mode         : DRY RUN (no writes)
echo.

if not exist "%DB%" (
    echo ERROR: Database not found: %DB%
    echo Run ingest-all-sessions.bat %SOURCE% first.
    exit /b 1
)

if not "%RELAX_STEPS%"=="0" (
    echo Relax steps  : %RELAX_STEPS%
)
echo.

if exist "%AMS_JSON%" (
    echo [pre] Repairing any missing AMS backing objects...
    python "%SCRIPTS%repair-ams-backing-objects.py" --ams "%AMS_JSON%"
    if errorlevel 1 echo Warning: AMS repair failed, continuing...
    echo.
)

set DREAM_LOG=%TEMP%\memoryctl-dream-%RANDOM%%RANDOM%.log
set DREAMRELAX_LOG=%TEMP%\memoryctl-dream-relax-%RANDOM%%RANDOM%.log

echo [1/2] Running Dreaming pipeline...
dotnet run --project "%MEMORYCTL%" -- dream ^
    --db "%DB%" ^
    --topic-k %TOPIC_K% ^
    --thread-k %THREAD_K% ^
    --decision-k %DECISION_K% ^
    --invariant-k %INVARIANT_K% ^
    %DRY_RUN_FLAG% > "%DREAM_LOG%" 2>&1
set DREAM_EXIT=%ERRORLEVEL%
if "%DREAM_EXIT%"=="0" (
    type "%DREAM_LOG%"
) else (
    findstr /C:"error: dream is unavailable in the current AMS.Core-only build." "%DREAM_LOG%" >nul
    if not errorlevel 1 if exist "%AMS_JSON%" (
        echo WARNING: memoryctl dream is unavailable in the current AMS.Core-only build.
        echo WARNING: Reusing existing AMS snapshot instead: %AMS_JSON%
        echo.
    ) else (
        type "%DREAM_LOG%"
        echo ERROR: Dream pipeline failed and no existing AMS snapshot is available.
        echo ERROR: Current build does not support ^`memoryctl dream^`; generate %AMS_JSON% via a compatible build first.
        del "%DREAM_LOG%" >nul 2>&1
        exit /b 1
    )
)
del "%DREAM_LOG%" >nul 2>&1
echo.

if "%RELAX_STEPS%"=="0" goto :skip_relax
if not "%DRY_RUN_FLAG%"=="" (
    echo [2/3] Running energy relaxation ^(dry-run^)...
    dotnet run --project "%MEMORYCTL%" -- dream-relax ^
        --db "%DB%" ^
        --steps %RELAX_STEPS% ^
        --dry-run > "%DREAMRELAX_LOG%" 2>&1
    set DREAMRELAX_EXIT=%ERRORLEVEL%
    if "%DREAMRELAX_EXIT%"=="0" (
        type "%DREAMRELAX_LOG%"
    ) else (
        findstr /C:"error: dream-relax is unavailable in the current AMS.Core-only build." "%DREAMRELAX_LOG%" >nul
        if not errorlevel 1 (
            echo WARNING: dream-relax is unavailable in the current AMS.Core-only build - skipping.
        ) else (
            type "%DREAMRELAX_LOG%"
            echo ERROR: Energy relaxation failed.
            del "%DREAMRELAX_LOG%" >nul 2>&1
            exit /b 1
        )
    )
) else (
    echo [2/3] Running energy relaxation ^(%RELAX_STEPS% steps^)...
    dotnet run --project "%MEMORYCTL%" -- dream-relax ^
        --db "%DB%" ^
        --steps %RELAX_STEPS% > "%DREAMRELAX_LOG%" 2>&1
    set DREAMRELAX_EXIT=%ERRORLEVEL%
    if "%DREAMRELAX_EXIT%"=="0" (
        type "%DREAMRELAX_LOG%"
    ) else (
        findstr /C:"error: dream-relax is unavailable in the current AMS.Core-only build." "%DREAMRELAX_LOG%" >nul
        if not errorlevel 1 (
            echo WARNING: dream-relax is unavailable in the current AMS.Core-only build - skipping.
        ) else (
            type "%DREAMRELAX_LOG%"
            echo ERROR: Energy relaxation failed.
            del "%DREAMRELAX_LOG%" >nul 2>&1
            exit /b 1
        )
    )
)
del "%DREAMRELAX_LOG%" >nul 2>&1
echo.

:skip_relax

if not "%DRY_RUN_FLAG%"=="" (
    echo [3/7] Skipped LLM title enrichment ^(dry-run^).
    echo.
    echo === Done dry-run ===
    goto :eof
)

set DO_ENRICH=0
if /i "%SOURCE%"=="claude" set DO_ENRICH=1

if "%DO_ENRICH%"=="1" (
    set ENRICH_MODEL_OPENAI=gpt-4o-mini
    set ENRICH_MODEL_ANTHROPIC=claude-haiku-4-5-20251001

    if defined OPENAI_API_KEY (
        echo [3/7] Enriching thread titles via LLM ^(OpenAI^)...
        python "%SCRIPTS%enrich-titles.py" ^
            --ams "%AMS_JSON%" ^
            --provider openai ^
            --api-key "%OPENAI_API_KEY%" ^
            --model "%ENRICH_MODEL_OPENAI%" ^
            --batch-size 10
        if errorlevel 1 echo [%STEM%] Warning: title enrichment failed, continuing...
    ) else if defined ANTHROPIC_API_KEY (
        echo [3/7] Enriching thread titles via LLM ^(Anthropic^)...
        python "%SCRIPTS%enrich-titles.py" ^
            --ams "%AMS_JSON%" ^
            --provider anthropic ^
            --api-key "%ANTHROPIC_API_KEY%" ^
            --model "%ENRICH_MODEL_ANTHROPIC%" ^
            --batch-size 10
        if errorlevel 1 echo [%STEM%] Warning: title enrichment failed, continuing...
    ) else (
        echo [3/7] Neither OPENAI_API_KEY nor ANTHROPIC_API_KEY set - using claude-cli ^(OAuth^)...
        python "%SCRIPTS%enrich-titles.py" ^
            --ams "%AMS_JSON%" ^
            --provider claude-cli ^
            --batch-size 10
        if errorlevel 1 echo [%STEM%] Warning: title enrichment failed, continuing...
    )
) else (
    if /i "%SOURCE%"=="all" (
        echo [3/7] Repairing thread titles deterministically for mixed-source corpus...
        python "%SCRIPTS%enrich-titles.py" ^
            --ams "%AMS_JSON%" ^
            --provider deterministic ^
            --repair-threads ^
            --skip-dream-nodes
        if errorlevel 1 echo [%STEM%] Warning: deterministic thread-title repair failed, continuing...
    ) else (
        echo [3/7] Skipping LLM title enrichment for source=%SOURCE%.
    )
)
echo.

if defined OPENAI_API_KEY (
    echo [4/7] Segmenting sessions into sprints ^(OpenAI^)...
    python "%SCRIPTS%segment-sprints.py" ^
        --ams "%AMS_JSON%" ^
        --provider openai ^
        --api-key "%OPENAI_API_KEY%" ^
        --gap-days 7
    if errorlevel 1 echo [%STEM%] Warning: sprint segmentation failed, continuing...
) else if defined ANTHROPIC_API_KEY (
    echo [4/7] Segmenting sessions into sprints ^(Anthropic^)...
    python "%SCRIPTS%segment-sprints.py" ^
        --ams "%AMS_JSON%" ^
        --provider anthropic ^
        --api-key "%ANTHROPIC_API_KEY%" ^
        --gap-days 7
    if errorlevel 1 echo [%STEM%] Warning: sprint segmentation failed, continuing...
) else (
    echo [4/7] Segmenting sessions into sprints ^(claude-cli^)...
    python "%SCRIPTS%segment-sprints.py" ^
        --ams "%AMS_JSON%" ^
        --provider claude-cli ^
        --gap-days 7
    if errorlevel 1 echo [%STEM%] Warning: sprint segmentation failed, continuing...
)
echo.

echo [5/7] Building working memory (recency-weighted dream objects)...
python "%SCRIPTS%build-working-memory.py" ^
    --ams "%AMS_JSON%" ^
    --top 10 ^
    --half-life-days 14
if errorlevel 1 echo [%STEM%] Warning: working memory build failed, continuing...
echo.

echo [6/8] Generating semantic embeddings...
python "%SCRIPTS%embed-dream-cards.py" --ams-json "%AMS_JSON%" --out "%EMBEDDINGS_JSON%"
if errorlevel 1 (
    echo WARNING: Embedding generation failed - continuing without embeddings.
)
echo.

echo [7/8] Running Watts-Strogatz dream shortcut linker...
set AMS_KERNEL=%SCRIPTS%..\rust\ams-core-kernel\target\release\ams-core-kernel.exe
if not exist "%AMS_KERNEL%" set AMS_KERNEL=%SCRIPTS%..\rust\ams-core-kernel\target\debug\ams-core-kernel.exe
if exist "%AMS_KERNEL%" (
    if exist "%AMS_JSON%" (
        if exist "%EMBEDDINGS_JSON%" (
            "%AMS_KERNEL%" dream-shortcut --input "%AMS_JSON%" --embeddings "%EMBEDDINGS_JSON%"
            if errorlevel 1 echo WARNING: Dream shortcut failed ^(non-fatal^) - continuing.
        ) else (
            echo SKIP: Embeddings not found ^(%EMBEDDINGS_JSON%^) - skipping dream-shortcut.
        )
    ) else (
        echo SKIP: AMS snapshot not found ^(%AMS_JSON%^) - skipping dream-shortcut.
    )
) else (
    echo SKIP: ams-core-kernel binary not found - skipping dream-shortcut.
)
echo.

echo [8/8] Regenerating HTML memory browser...
dotnet run --project "%MEMORYCTL%" -- debug-ams ^
    --db "%DB%" ^
    --out "%HTML%"
if errorlevel 1 (
    echo ERROR: HTML generation failed.
    exit /b 1
)
echo.

echo === Done! ===
echo HTML browser : %HTML%
echo.
if /i not "%NO_BROWSER%"=="1" start "" "%HTML%"
exit /b 0

:usage
echo Usage: dream-all-sessions.bat [source] [output-dir] [topic-k] [thread-k] [decision-k] [invariant-k] [--dry-run] [relax-steps]
echo   source = all ^| claude ^| codex   ^(default: all^)
exit /b 1

:usage_fail
exit /b 1
