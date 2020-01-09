#!/bin/bash

export THIRDPARTY_DIR=../third-party
echo $THIRDPARTY_DIR
mkdir obj
pushd obj
clang++ -c -m64 -maes -msse4.1 -O3 -pthread -fPIC -Isrc ../src/*.c ../lib/*.c $THIRDPARTY_DIR/lizard/lib/*.c $THIRDPARTY_DIR/lizard/lib/entropy/*.c $THIRDPARTY_DIR/lizard/lib/xxhash/*.c
ar rc ../longtail_lib.a *.o
popd
