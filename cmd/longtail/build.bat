@ECHO off

ECHO Building longtail executable
go build -ldflags="-s -w" .
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
ECHO Success
