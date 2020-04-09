@ECHO off

set LIB_TARGET_FOLDER=..\..\longtaillib

pushd %LIB_TARGET_FOLDER%
call build_longtail.bat
popd

ECHO Building longtail executable
go build -ldflags="-s -w" .
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
ECHO Success
