@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "INIT_PY=%SCRIPT_DIR%init-swarm-project.py"

if not exist "%INIT_PY%" (
  echo ERROR: init-swarm-project.py not found at "%INIT_PY%".
  exit /b 1
)

where py >nul 2>nul
if not errorlevel 1 (
  py -3 "%INIT_PY%" %*
  exit /b %ERRORLEVEL%
)

where python >nul 2>nul
if not errorlevel 1 (
  python "%INIT_PY%" %*
  exit /b %ERRORLEVEL%
)

echo ERROR: Python launcher not found. Install Python or run the script with py -3.
exit /b 1
