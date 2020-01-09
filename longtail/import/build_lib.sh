#!/bin/bash

export THIRDPARTY_DIR=../third-party
echo $THIRDPARTY_DIR
mkdir obj
pushd obj
gcc -c -std=gnu99 -m64 -maes -msse4.1 -O3 -pthread -Isrc ../src/*.c ../lib/*.c $THIRDPARTY_DIR/lizard/lib/*.c $THIRDPARTY_DIR/lizard/lib/entropy/*.c $THIRDPARTY_DIR/lizard/lib/xxhash/*.c
ar rc ../longtail_lib.a *.o
popd
