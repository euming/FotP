@echo off
setlocal
cd /d "%~dp0tools\memoryctl" || exit /b 1

echo [validate]
dotnet run -- validate --db tests\sample.memory.jsonl || exit /b 1

echo [query]
dotnet run -- query --db tests\sample.memory.jsonl --q opus --top 5 --explain || exit /b 1

echo [prompt]
dotnet run -- prompt --db tests\sample.memory.jsonl --q opus --top 5 || exit /b 1

echo Done.
