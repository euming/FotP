@echo off
setlocal

cd /d C:\Users\eumin\.openclaw\workspace\memory\memory_graph

dotnet run --project C:\Users\eumin\wkspaces\git\NetworkGraphMemory\tools\memoryctl\MemoryCtl.csproj -- prompt --db memory.jsonl --binder "Topic: roadmap" --top 20

endlocal
