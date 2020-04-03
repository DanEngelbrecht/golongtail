
#!/bin/bash

LIB_TARGET_FOLDER=../lib/import/clib
LIB_TARGET=${LIB_TARGET_FOLDER}/longtail_lib.a

echo "LIB_TARGET $LIB_TARGET"

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
