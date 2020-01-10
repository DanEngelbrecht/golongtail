#!/bin/bash

export THIRDPARTY_DIR=../third-party
echo $THIRDPARTY_DIR
mkdir obj
pushd obj
gcc -c -std=gnu99 -m64 -O3 -pthread -Isrc ../src/*.c ../lib/*.c $THIRDPARTY_DIR/lizard/lib/longtail_lib.c $THIRDPARTY_DIR/lizard/lib/longtail_platform.c $THIRDPARTY_DIR/lizard/lib/entropy/*.c $THIRDPARTY_DIR/lizard/lib/xxhash/*.c
ar rc ../longtail_lib.a *.o
popd
