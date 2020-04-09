
#!/bin/bash

LIB_TARGET_FOLDER=../../longtaillib
LIB_TARGET=${LIB_TARGET_FOLDER}/longtail_lib.a

echo "LIB_TARGET $LIB_TARGET"

if [ ! -e $LIB_TARGET ]
then
	pushd ${LIB_TARGET_FOLDER} >>/dev/null
	./build_longtail.sh
	popd
fi

echo Building longtail executable
go build -ldflags="-s -w" .
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
echo Success
