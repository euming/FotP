@echo off
setlocal

if defined AMS_OUTPUT_ROOT (
  set "HTML=%AMS_OUTPUT_ROOT%\all-agents-sessions\all-agents-sessions.ams-debug.html"
) else if defined LOCALAPPDATA (
  set "HTML=%LOCALAPPDATA%\NetworkGraphMemory\agent-memory\all-agents-sessions\all-agents-sessions.ams-debug.html"
) else (
  set "HTML=%~dp0output\all-agents-sessions\all-agents-sessions.ams-debug.html"
)
if not exist "%HTML%" (
  echo ERROR: AMS debug HTML not found at "%HTML%"
  exit /b 1
)

for %%I in ("%HTML%") do set "HTML_URI=file:///%%~fI"
set "HTML_URI=%HTML_URI:\=/%"

if "%~1"=="--snapshot" (
  call "%~dp0agent-browser-wrapper.bat" open "%HTML_URI%" --headed
  if errorlevel 1 exit /b %errorlevel%
  call "%~dp0agent-browser-wrapper.bat" snapshot -i --json
  exit /b %errorlevel%
)

if "%~1"=="--annotate" (
  call "%~dp0agent-browser-wrapper.bat" open "%HTML_URI%" --headed
  if errorlevel 1 exit /b %errorlevel%
  call "%~dp0agent-browser-wrapper.bat" screenshot --annotate
  exit /b %errorlevel%
)

call "%~dp0agent-browser-wrapper.bat" open "%HTML_URI%" --headed
exit /b %errorlevel%
