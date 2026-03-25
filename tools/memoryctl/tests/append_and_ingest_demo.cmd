@echo off
setlocal
cd /d "%~dp0.." || exit /b 1

set TMPCHAT=%~dp0tmp.chatlog.jsonl
set TMPCURSOR=%~dp0tmp.cursor.json
set TMPMEM=%~dp0tmp.memory.jsonl

del /q "%TMPCHAT%" 2>nul
del /q "%TMPCURSOR%" 2>nul

echo {"type":"format","name":"card-memAnchor","version":1} > "%TMPMEM%"

dotnet run -- append-chat-event --chatlog "%TMPCHAT%" --channel telegram --chat-id -5229501860 --message-id 1 --direction in --author "Ming" --text "hello memory" --ts "2026-02-02T23:50:00-08:00"
dotnet run -- append-chat-event --chatlog "%TMPCHAT%" --channel telegram --chat-id -5229501860 --message-id 2 --direction out --author "Rocky" --text "logged" --ts "2026-02-02T23:51:00-08:00"

dotnet run -- ingest-chatlog --db "%TMPMEM%" --chatlog "%TMPCHAT%" --cursor "%TMPCURSOR%" --max 200 --gap-min 10

dotnet run -- query --db "%TMPMEM%" --q "Chat:" --top 5 --explain
