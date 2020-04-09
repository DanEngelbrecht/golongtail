
#!/bin/bash

LIB_TARGET_FOLDER="../../longtaillib"

pushd ${LIB_TARGET_FOLDER} >>/dev/null
./build_longtail.sh
popd >>/dev/null

echo Building longtail executable
go build -ldflags="-s -w" .
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
echo Success
