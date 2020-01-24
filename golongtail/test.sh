
#!/bin/bash

arch=$(uname -i)
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

LIB_TARGET_FOLDER=import/${OS_NAME}_${ARCH_NAME}
LIB_TARGET=${LIB_TARGET_FOLDER}/longtail_lib.a

if [ ! -e $LIB_TARGET ]
then
	echo Building longtail library, this takes a couple of minutes, hold on...
	pushd import >>/dev/null
	./build_lib.sh
	popd
fi

echo Running test
go test .
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
echo Success
