#!/bin/bash

set -e

cd lib
./test.sh
cd ..
cd cmd
./build.sh
cd ..
