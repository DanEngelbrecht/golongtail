@ECHO off

set LIB_TARGET_FOLDER=..\lib\import\clib
set LIB_TARGET=%LIB_TARGET_FOLDER%\longtail_lib.a

IF NOT EXIST "%LIB_TARGET%" (
	ECHO Building longtail library, this takes a couple of minutes, hold on...
	pushd ..\lib\import\
	call build_lib.bat
	popd
)

ECHO Building longtail executable
go build .
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
ECHO Success
