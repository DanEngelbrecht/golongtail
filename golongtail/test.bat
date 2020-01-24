@ECHO off
SetLocal EnableDelayedExpansion

reg Query "HKLM\Hardware\Description\System\CentralProcessor\0" | find /i "x86" > NUL && set ARCH_NAME=386 || set ARCH_NAME=amd64
set OS_NAME=windows

set LIB_TARGET_FOLDER=import\%OS_NAME%_%ARCH_NAME%
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
