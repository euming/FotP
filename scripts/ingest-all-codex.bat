@echo off
setlocal

:: Compatibility wrapper. Prefer ingest-all-sessions.bat.
call "%~dp0ingest-all-sessions.bat" codex %*
exit /b %errorlevel%
