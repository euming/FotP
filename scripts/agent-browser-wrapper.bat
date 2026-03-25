@echo off
setlocal

where node >nul 2>nul
if errorlevel 1 (
  echo ERROR: node is required.
  exit /b 1
)

where npx >nul 2>nul
if errorlevel 1 (
  echo ERROR: npx is required.
  exit /b 1
)

set "SESSION=network-graph-memory"
if not "%AGENT_BROWSER_SESSION%"=="" set "SESSION=%AGENT_BROWSER_SESSION%"
if "%AGENT_BROWSER_ALLOW_FILE_ACCESS%"=="" set "AGENT_BROWSER_ALLOW_FILE_ACCESS=true"

npx agent-browser --session "%SESSION%" %*
exit /b %errorlevel%
