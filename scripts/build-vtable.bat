@echo off
:: Build ams-sqlite-vtable (Windows) and copy artifact to dist/
setlocal

set REPO_ROOT=%~dp0..
set CRATE_DIR=%REPO_ROOT%\rust\ams-sqlite-vtable
set DIST_DIR=%REPO_ROOT%\dist

echo Building ams-sqlite-vtable (release)...
pushd "%CRATE_DIR%"
cargo build --release
if errorlevel 1 (
    echo Build failed.
    popd
    exit /b 1
)
popd

if not exist "%DIST_DIR%" mkdir "%DIST_DIR%"

copy /y "%CRATE_DIR%\target\release\ams_vtable.dll" "%DIST_DIR%\libams_vtable.dll"
echo Artifact copied to dist\libams_vtable.dll

echo Done.
