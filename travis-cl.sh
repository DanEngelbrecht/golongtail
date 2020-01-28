#!/bin/bash

set -e

cd lib
test.bat
cd ..
cd cmd
build.bat
cd ..
