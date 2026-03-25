@echo off
setlocal
cd /d "%~dp0.." || exit /b 1

set DB=C:\Users\eumin\.openclaw\workspace\memory\memory_graph\memory.jsonl

dotnet run -- add --db "%DB%" --title "Example: ADD topic switch" --text "We switched topics; store as separate cards linked to topic memAnchors." --memAnchor "Conversations,Topic: Memory System" --source "manual" --key "example-add-topic-switch"

dotnet run -- query --db "%DB%" --q "topic switch" --top 5 --explain
