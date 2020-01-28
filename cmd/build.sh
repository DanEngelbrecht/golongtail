
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
elif [[ "$OSTYPE" == "win32" ]]; then
	OS_NAME=windows
fi

LIB_TARGET_FOLDER=../lib/import/${OS_NAME}_${ARCH_NAME}
LIB_TARGET=${LIB_TARGET_FOLDER}/longtail_lib.a

if [ ! -e $LIB_TARGET ]
then
	echo Building longtail library, this takes a couple of minutes, hold on...
	pushd ../lib/import >>/dev/null
	./build_lib.sh
	popd
fi

echo Building longtail executable
go build .
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
echo Success
