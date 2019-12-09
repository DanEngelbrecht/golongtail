set THIRDPARTY_DIR=..\longtail\third-party
echo %THIRDPARTY_DIR%
mkdir obj
pushd obj
gcc -c -m64 -maes -mssse3 -pthread -Isrc -DWINVER=0x0A00 -D_WIN32_WINNT=0x0A00 ..\longtail\src\*.c ..\longtail\lib\*.c %THIRDPARTY_DIR%\lizard\lib\*.c %THIRDPARTY_DIR%\lizard\lib\entropy\*.c %THIRDPARTY_DIR%\lizard\lib\xxhash\*.c
ar rc ../longtail_lib.a *.o
popd
