@ECHO off

IF NOT EXIST "..\golongtail\import\longtail_lib.a" (
	ECHO Building longtail library, this takes a couple of minutes, hold on...
	pushd ..\golongtail\import\
	call build_lib.bat
	if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
	popd
)

ECHO Building longtail executable
go build .
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
ECHO Success
