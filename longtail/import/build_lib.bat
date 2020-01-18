set THIRDPARTY_DIR=..\third-party
pushd obj
set BIKESHED_SRC=..\lib\bikeshed\*.c
set BLAKE2_SRC=..\lib\blake2\*.c ..\lib\blake2\ext\*.c
set FILESTORAGE_SRC=..\lib\filestorage\*.c
set MEMSTORAGE_SRC=..\lib\memstorage\*.c
set MEOWHASH_SRC=..\lib\meowhash\*.c
set LIZARD_SRC=..\lib\lizard\*.c ..\lib\lizard\ext\*.c ..\lib\lizard\ext\entropy\*.c ..\lib\lizard\ext\xxhash\*.c
set BROTLI_SRC=..\lib\brotli\*.c ..\lib\brotli\ext\common\*.c ..\lib\brotli\ext\dec\*.c ..\lib\brotli\ext\enc\*.c ..\lib\brotli\ext\fuzz\*.c
set ZLIB_SRC=..\lib\zstd\*.c ..\lib\zstd\ext\common\*.c ..\lib\zstd\ext\compress\*.c ..\lib\zstd\ext\decompress\*.c
del /Q *.o
gcc -c -std=gnu99 -m64 -O3 -pthread -msse4.1 -maes -Isrc -DWINVER=0x0A00 -D_WIN32_WINNT=0x0A00 ..\src\*.c ..\src\ext\*.c ..\lib\*.c %BIKESHED_SRC% %BLAKE2_SRC% %FILESTORAGE_SRC% %MEMSTORAGE_SRC% %MEOWHASH_SRC% %LIZARD_SRC% %BROTLI_SRC% %ZLIB_SRC%
ar rc ../longtail_lib.a *.o
popd
