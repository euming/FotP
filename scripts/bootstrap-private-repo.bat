@echo off
setlocal
set SCRIPT_DIR=%~dp0
bash "%SCRIPT_DIR%bootstrap-private-repo.sh" %*
