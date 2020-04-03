@ECHO off
SetLocal EnableDelayedExpansion

set LIB_TARGET_FOLDER=import\clib
set LIB_TARGET=%LIB_TARGET_FOLDER%\longtail_lib.a

IF NOT EXIST "%LIB_TARGET%" (
	ECHO Building longtail library, this takes a couple of minutes, hold on...
	pushd import\
	call build_lib.bat
	popd
)

ECHO Running test
go test .
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
ECHO Success
