
#!/bin/bash

if [ ! -e ../golongtail/import/longtail_lib.a ]
then
	echo Building longtail library, this takes a couple of minutes, hold on...
	pushd ../golongtail/import >>/dev/null
	./build_lib.sh
	rc=$?
	popd
	if [[ $rc != 0 ]]; then exit $rc; fi
fi

echo Building longtail executable
go build .
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
echo Success
