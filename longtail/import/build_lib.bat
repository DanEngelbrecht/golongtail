set THIRDPARTY_DIR=..\third-party
echo %THIRDPARTY_DIR%
mkdir obj
pushd obj
gcc -c -m64 -O3 -pthread -Isrc -DWINVER=0x0A00 -D_WIN32_WINNT=0x0A00 ..\src\*.c ..\src\ext\*.c ..\lib\*.c ..\lib\filestorage\*.c ..\lib\memstorage\*.c ..\lib\lizard\*.c ..\lib\blake2\*.c ..\lib\bikeshed\*.c ..\lib\xxhash\*.c
ar rc ../longtail_lib.a *.o
popd
