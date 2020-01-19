#!/bin/bash

THIRDPARTY_DIR=../third-party
pushd obj
BIKESHED_SRC="../lib/bikeshed/*.c"
BLAKE2_SRC="../lib/blake2/*.c ../lib/blake2/ext/*.c"
BLAKE3_SRC="../lib/blake3/*.c ../lib/blake3/ext/*.c"
FILESTORAGE_SRC="../lib/filestorage/*.c"
MEMSTORAGE_SRC="../lib/memstorage/*.c"
MEOWHASH_SRC="../lib/meowhash/*.c"
LIZARD_SRC="../lib/lizard/*.c ../lib/lizard/ext/*.c ../lib/lizard/ext/entropy/*.c ../lib/lizard/ext/xxhash/*.c"
BROTLI_SRC="../lib/brotli/*.c ../lib/brotli/ext/common/*.c ../lib/brotli/ext/dec/*.c ../lib/brotli/ext/enc/*.c ../lib/brotli/ext/fuzz/*.c"
ZLIB_SRC="../lib/zstd/*.c ../lib/zstd/ext/common/*.c ../lib/zstd/ext/compress/*.c ../lib/zstd/ext/decompress/*.c"
rm *.o
gcc -c -std=gnu99 -m64 -O3 -pthread -msse4.1 -maes -Isrc ../src/*.c ../src/ext/*.c ../lib/*.c $BIKESHED_SRC $BLAKE2_SRC $BLAKE3_SRC $FILESTORAGE_SRC $MEMSTORAGE_SRC $MEOWHASH_SRC $LIZARD_SRC $BROTLI_SRC $ZLIB_SRC
ar rc ../longtail_lib.a *.o
popd
