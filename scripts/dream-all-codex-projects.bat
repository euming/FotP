@echo off
setlocal

:: Compatibility wrapper for Codex global dream pipeline.
call "%~dp0dream-all-sessions.bat" codex %*
exit /b %errorlevel%
