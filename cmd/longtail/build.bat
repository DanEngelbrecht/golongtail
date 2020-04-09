@ECHO off

set LIB_TARGET_FOLDER=..\longtaillib
set LIB_TARGET=%LIB_TARGET_FOLDER%\longtail_lib.a

IF NOT EXIST "%LIB_TARGET%" (
	ECHO Building longtail library, this takes a couple of minutes, hold on...
	pushd ..\..\longtaillib\
	call build_longtail.bat
	popd
)

ECHO Building longtail executable
go build -ldflags="-s -w" .
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
ECHO Success
