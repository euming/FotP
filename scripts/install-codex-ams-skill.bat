@echo off
setlocal

if "%CODEX_HOME%"=="" (
  set "CODEX_HOME=%USERPROFILE%\.codex"
)

set "SRC=%~dp0..\codex-skills\ams"
set "DEST=%CODEX_HOME%\skills\ams"

if not exist "%SRC%\SKILL.md" (
  echo ERROR: repo skill source not found at "%SRC%"
  exit /b 1
)

if exist "%DEST%" (
  rmdir /s /q "%DEST%"
)

mkdir "%DEST%" >nul 2>nul
xcopy "%SRC%\*" "%DEST%\" /E /I /Y >nul

echo Installed Codex skill to "%DEST%"
echo Restart Codex to pick up new skills.
