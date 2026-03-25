@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "SHADOW_PY=%SCRIPT_DIR%run-rust-shadow-validation.py"

if not exist "%SHADOW_PY%" (
  echo ERROR: run-rust-shadow-validation.py not found at "%SHADOW_PY%".
  exit /b 1
)

where py >nul 2>nul
if not errorlevel 1 (
  py -3 "%SHADOW_PY%" %*
  exit /b %ERRORLEVEL%
)

where python >nul 2>nul
if not errorlevel 1 (
  python "%SHADOW_PY%" %*
  exit /b %ERRORLEVEL%
)

echo ERROR: Python launcher not found. Install Python or run the wrapper via py -3.
exit /b 1
