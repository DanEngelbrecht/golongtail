@ECHO off

reg Query "HKLM\Hardware\Description\System\CentralProcessor\0" | find /i "x86" > NUL && set ARCH_NAME=386 || set ARCH_NAME=amd64
set OS_NAME=windows

set LIB_TARGET_FOLDER=..\lib\import\%OS_NAME%_%ARCH_NAME%
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
