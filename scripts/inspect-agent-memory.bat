@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fI"

set "PROJECT_NAME=NetworkGraphMemory"
set "DB_PATH=%REPO_ROOT%\scripts\output\per-project\%PROJECT_NAME%\%PROJECT_NAME%.memory.jsonl"
set "OUT_HTML=%REPO_ROOT%\scripts\output\per-project\%PROJECT_NAME%\agent-memory-inspector.html"
set "MEMORYCTL_PROJECT=%REPO_ROOT%\tools\memoryctl\MemoryCtl.csproj"

if not exist "%DB_PATH%" (
    echo ERROR: DB not found:
    echo   %DB_PATH%
    echo Run maintain/ingest first, then rerun this script.
    exit /b 1
)

echo.
echo [1/3] Refreshing lesson memory...
dotnet run --project "%MEMORYCTL_PROJECT%" -- agent-maintain --db "%DB_PATH%"
if errorlevel 1 (
    echo ERROR: agent-maintain failed.
    exit /b 1
)

echo.
echo [2/3] Building HTML inspector...
dotnet run --project "%MEMORYCTL_PROJECT%" -- debug-ams --db "%DB_PATH%" --out "%OUT_HTML%" --include-structural >nul
if errorlevel 1 (
    echo ERROR: debug-ams failed.
    exit /b 1
)

echo.
echo [3/3] Quick text summary (decay ladder + summaries + groups)...
dotnet run --project "%MEMORYCTL_PROJECT%" -- debug-ams --db "%DB_PATH%" --include-structural ^
  | findstr /i /c:"  [ctr]" ^
  | findstr /i "agent-memory agent-summary-index lesson-freshness: lesson-semantic-theme: lesson-semantic-node: agent-memory:semantic:"

echo.
echo Done.
echo HTML inspector:
echo   %OUT_HTML%
echo.
echo Tip: open it with:
echo   start "" "%OUT_HTML%"
echo.

exit /b 0
