@echo off
setlocal

set TASK_NAME=NetworkGraphMemory-AgentMemoryWatch
set SCRIPT_DIR=%~dp0
if not exist "%SCRIPT_DIR%install-agent-memory-sync-task.bat" set SCRIPT_DIR=%CD%\scripts\
set WATCH_BAT=%SCRIPT_DIR%watch-all-agent-memory.bat
set START_BAT=%SCRIPT_DIR%start-agent-memory-watch.bat
set STARTUP_FILE=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\NetworkGraphMemory-AgentMemoryWatch.cmd

if not exist "%WATCH_BAT%" (
    echo ERROR: watcher script not found: %WATCH_BAT%
    exit /b 1
)

if not exist "%START_BAT%" (
    echo ERROR: launcher script not found: %START_BAT%
    exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$taskName = '%TASK_NAME%';" ^
  "$watchBat = '%WATCH_BAT%';" ^
  "$action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument ('/c ""' + $watchBat + '"" --initial-sync');" ^
  "$trigger = New-ScheduledTaskTrigger -AtLogOn;" ^
  "$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries;" ^
  "Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Description 'Auto-sync Claude and Codex AMS memory.' -Force | Out-Null;" >nul 2>&1
if errorlevel 1 (
    echo WARNING: failed to install scheduled task %TASK_NAME%. Falling back to Startup folder autostart.
    copy /Y "%START_BAT%" "%STARTUP_FILE%" >nul
    if errorlevel 1 (
        echo ERROR: failed to install Startup launcher %STARTUP_FILE%.
        exit /b 1
    )
    call "%START_BAT%"
    if errorlevel 1 (
        echo ERROR: Startup launcher installed but failed to start watcher.
        exit /b 1
    )
    echo Installed Startup launcher: %STARTUP_FILE%
    echo Started watcher immediately.
    exit /b 0
)

schtasks /Run /TN "%TASK_NAME%" >nul 2>&1

echo Installed scheduled task: %TASK_NAME%
echo Started watcher task.
exit /b 0
