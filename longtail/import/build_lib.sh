#!/bin/bash

export THIRDPARTY_DIR=../third-party
echo $THIRDPARTY_DIR
mkdir obj
pushd obj
export BIKESHED_SRC=../lib/bikeshed/*.c
export BLAKE2_SRC=../lib/blake2/*.c ../lib/blake2/ext/*.c
export FILESTORAGE_SRC=../lib/filestorage/*.c
export MEMSTORAGE_SRC=../lib/memstorage/*.c
export LIZARD_SRC=../lib/lizard/*.c ../lib/lizard/ext/*.c ../lib/lizard/ext/entropy/*.c ../lib/lizard/ext/xxhash/*.c
export BROTLI_SRC=../lib/brotli/ext/common/*.c ../lib/brotli/ext/dec/*.c ../lib/brotli/ext/enc/*.c ../lib/brotli/ext/fuzz/*.c
gcc -c -std=gnu99 -m64 -O3 -pthread -Isrc  ../src/*.c ../src/ext/*.c ../lib/*.c $BIKESHED_SRC $BLAKE2_SRC $FILESTORAGE_SRC $MEMSTORAGE_SRC $LIZARD_SRC $BROTLI_SRC
ar rc ../longtail_lib.a *.o
popd
