set THIRDPARTY_DIR=..\third-party
echo %THIRDPARTY_DIR%
mkdir obj
pushd obj
set BIKESHED_SRC=..\lib\bikeshed\*.c
set BLAKE2_SRC=..\lib\blake2\*.c ..\lib\blake2\ext\*.c
set FILESTORAGE_SRC=..\lib\filestorage\*.c
set MEMSTORAGE_SRC=..\lib\memstorage\*.c
set LIZARD_SRC=..\lib\lizard\*.c ..\lib\lizard\ext\*.c ..\lib\lizard\ext\entropy\*.c ..\lib\lizard\ext\xxhash\*.c
set BROTLI_SRC=..\lib\brotli\ext\common\*.c ..\lib\brotli\ext\dec\*.c ..\lib\brotli\ext\enc\*.c ..\lib\brotli\ext\fuzz\*.c
gcc -c -m64 -O3 -pthread -Isrc -DWINVER=0x0A00 -D_WIN32_WINNT=0x0A00 ..\src\*.c ..\src\ext\*.c ..\lib\*.c %BIKESHED_SRC% %BLAKE2_SRC% %FILESTORAGE_SRC% %MEMSTORAGE_SRC% %LIZARD_SRC% %BROTLI_SRC%
ar rc ../longtail_lib.a *.o
popd
