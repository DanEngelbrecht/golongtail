@ECHO off

IF NOT EXIST "import\longtail_lib.a" (
	ECHO Building longtail library, this takes a couple of minutes, hold on...
	pushd import\
	call build_lib.bat
	if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
	popd
)

ECHO Running test
go test .
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
ECHO Success
