@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "AMS_PY=%SCRIPT_DIR%ams.py"

if not exist "%AMS_PY%" (
  echo ERROR: ams.py not found at "%AMS_PY%".
  exit /b 1
)

where py >nul 2>nul
if not errorlevel 1 (
  py -3 "%AMS_PY%" %*
  exit /b %ERRORLEVEL%
)

where python >nul 2>nul
if not errorlevel 1 (
  python "%AMS_PY%" %*
  exit /b %ERRORLEVEL%
)

echo ERROR: Python launcher not found. Install Python or run the wrapper via py -3.
exit /b 1
