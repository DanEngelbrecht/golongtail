@ECHO off
SetLocal EnableDelayedExpansion

call build_longtail.bat

ECHO Running test
go test .
if %ERRORLEVEL% neq 0 exit /b %ERRORLEVEL%
ECHO Success
