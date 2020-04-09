@ECHO off
SetLocal EnableDelayedExpansion

set LIB_TARGET_FOLDER=.
set LIB_TARGET=%LIB_TARGET_FOLDER%\longtail_lib.a

IF NOT EXIST "%LIB_TARGET%" (
	call build_longtail.bat
)

ECHO Running test
go test .
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
ECHO Success
