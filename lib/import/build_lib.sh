#!/bin/bash

arch=$(uname -p)
if [[ $arch == x86_64* ]]; then
	ARCH_NAME=amd64
elif [[ $arch == i*86 ]]; then
	ARCH_NAME=386
elif  [[ $arch == arm* ]]; then
	ARCH_NAME=arm
fi

if [[ "$OSTYPE" == "linux-gnu" ]]; then
	OS_NAME=linux
elif [[ "$OSTYPE" == "darwin"* ]]; then
	OS_NAME=darwin
	ARCH_NAME=amd64
elif [[ "$OSTYPE" == "win32" ]]; then
	OS_NAME=windows
fi

LIB_TARGET_FOLDER=${OS_NAME}_${ARCH_NAME}
LIB_TARGET=${LIB_TARGET_FOLDER}/longtail_lib.a

echo Building $LIB_TARGET

mkdir -p obj
mkdir -p $LIB_TARGET_FOLDER
pushd obj >>/dev/null
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
gcc -c -std=gnu99 -m64 -O3 -pthread -msse4.1 -maes ../src/*.c ../src/ext/*.c ../lib/*.c $BIKESHED_SRC $BLAKE2_SRC $BLAKE3_SRC $FILESTORAGE_SRC $MEMSTORAGE_SRC $MEOWHASH_SRC $LIZARD_SRC $BROTLI_SRC $ZLIB_SRC
popd
ar cr -v $LIB_TARGET obj/*.o
