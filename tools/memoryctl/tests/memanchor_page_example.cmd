@echo off
setlocal

set DB=C:\Users\eumin\.openclaw\workspace\memory\memory_graph\memory.jsonl
set OUT=C:\Users\eumin\.openclaw\workspace\memory\memanchor_pages\Topic_roadmap.html

cd /d %~dp0\..

dotnet run -- memanchor-page --db %DB% --memAnchor "Topic: roadmap" --out %OUT%

endlocal
