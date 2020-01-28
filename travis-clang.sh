#!/bin/bash

set -e

cd lib
bash ./test.sh
cd ..
cd cmd
bash ./build.sh
cd ..
