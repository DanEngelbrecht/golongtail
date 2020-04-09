#!/usr/bin/env bash

LIB_TARGET_FOLDER=.
LIB_TARGET=${LIB_TARGET_FOLDER}/longtail_lib.a

if [ ! -e $LIB_TARGET ]
then
	echo Building $LIB_TARGET

	mkdir -p longtail/obj
	mkdir -p $LIB_TARGET_FOLDER
	pushd longtail/obj >>/dev/null
	BIKESHED_SRC="../lib/bikeshed/*.c"
	BLAKE2_SRC="../lib/blake2/*.c ../lib/blake2/ext/*.c"
	BLAKE3_SRC="../lib/blake3/*.c ../lib/blake3/ext/*.c"
	CACHEBLOCKSTORE_SRC="../lib/cacheblockstore/*.c"
	COMPRESSBLOCKSTORE_SRC="../lib/compressblockstore/*.c"
	FULL_COMPRESSION_REGISTRY_SRC="../lib/compressionregistry/longtail_full_compression_registry.c"
	FILESTORAGE_SRC="../lib/filestorage/*.c"
	FSBLOCKSTORE_SRC="../lib/fsblockstore/*.c"
	MEMSTORAGE_SRC="../lib/memstorage/*.c"
	MEOWHASH_SRC="../lib/meowhash/*.c"
	LIZARD_SRC="../lib/lizard/*.c ../lib/lizard/ext/*.c ../lib/lizard/ext/entropy/*.c"
	LZ4_SRC="../lib/lz4/*.c ../lib/lz4/ext/*.c"
	BROTLI_SRC="../lib/brotli/*.c ../lib/brotli/ext/common/*.c ../lib/brotli/ext/dec/*.c ../lib/brotli/ext/enc/*.c ../lib/brotli/ext/fuzz/*.c"
	ZSTD_SRC="../lib/zstd/*.c ../lib/zstd/ext/common/*.c ../lib/zstd/ext/compress/*.c ../lib/zstd/ext/decompress/*.c"
	rm *.o
	gcc -c $GCC_EXTRA -std=gnu99 -m64 -O3 -g -pthread -msse4.1 -maes ../src/*.c ../src/ext/*.c ../lib/*.c $BIKESHED_SRC $BLAKE2_SRC $BLAKE3_SRC $CACHEBLOCKSTORE_SRC $COMPRESSBLOCKSTORE_SRC $FULL_COMPRESSION_REGISTRY_SRC $FILESTORAGE_SRC $FSBLOCKSTORE_SRC $MEMSTORAGE_SRC $MEOWHASH_SRC $LIZARD_SRC $LZ4_SRC $BROTLI_SRC $ZSTD_SRC
	popd
	ar cr -v $LIB_TARGET longtail/obj/*.o
fi
