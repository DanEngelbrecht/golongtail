@ECHO off

set LIB_TARGET_FOLDER=..\..\longtaillib
set LIB_TARGET=%LIB_TARGET_FOLDER%\longtail_lib.a

IF NOT EXIST "%LIB_TARGET%" (
	pushd %LIB_TARGET_FOLDER%
	call build_longtail.bat
	popd
)

ECHO Building longtail executable
go build -ldflags="-s -w" .
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
ECHO Success
