@echo off
setlocal ENABLEDELAYEDEXPANSION

rem Path to the real memory DB
set "DB=C:\Users\eumin\.openclaw\workspace\memory\memory_graph\memory.jsonl"

rem Output directory (memAnchor HTML pages)
set "OUTDIR=C:\Users\eumin\.openclaw\workspace\memory\memanchor_pages"

rem Ensure we're running from the tools/memoryctl directory
pushd %~dp0\..

rem Generate a page for every memAnchor name in the DB
for /f "usebackq delims=" %%M in (`dotnet run -- list-memanchors --db "%DB%"`) do (
    set "NAME=%%M"
    if not "!NAME!"=="" (
        echo Generating memanchor-page for: !NAME!
        dotnet run -- memanchor-page --db "%DB%" --memAnchor "!NAME!"
    )
)

popd

endlocal
