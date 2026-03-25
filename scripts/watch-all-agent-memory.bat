@echo off
setlocal

set SCRIPT_DIR=%~dp0
if not exist "%SCRIPT_DIR%watch-all-agent-memory.bat" set SCRIPT_DIR=%CD%\scripts\

python "%SCRIPT_DIR%watch-all-agent-memory.py" %*
