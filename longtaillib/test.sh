
#!/bin/bash

LIB_TARGET_FOLDER=.
LIB_TARGET=${LIB_TARGET_FOLDER}/longtail_lib.a

echo "LIB_TARGET $LIB_TARGET"

if [ ! -e $LIB_TARGET ]
then
	./build_longtail.sh
fi

echo Running test
go test .
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
echo Success
