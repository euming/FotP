@echo off
setlocal

:: Compatibility wrapper. Prefer dream-all-sessions.bat.
call "%~dp0dream-all-sessions.bat" claude %*
exit /b %errorlevel%
