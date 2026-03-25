@echo off
setlocal

set PYTHON=python
set SCRIPT=%~dp0restore-claude-memory.py

if "%~1"=="" (
    echo Usage:
    echo   restore-claude-memory.bat ^<repo-dir^>
    echo   restore-claude-memory.bat ^<repo-dir^> --list
    echo   restore-claude-memory.bat ^<repo-dir^> --name CLAUDE.local.YYYYMMDD-HHMMSS.hash.md
    exit /b 1
)

set REPO_DIR=%~1
shift

%PYTHON% "%SCRIPT%" --repo-dir "%REPO_DIR%" %*
exit /b %ERRORLEVEL%
