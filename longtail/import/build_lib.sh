#!/bin/bash

export THIRDPARTY_DIR=../third-party
echo $THIRDPARTY_DIR
mkdir obj
pushd obj
gcc -c -std=gnu99 -m64 -O3 -pthread -Isrc  ../src/*.c ../lib/*.c ../lib/filestorage/*.c ../lib/lizard/*.c ../lib/blake2/*.c ../lib/bikeshed/*.c
ar rc ../longtail_lib.a *.o
popd
