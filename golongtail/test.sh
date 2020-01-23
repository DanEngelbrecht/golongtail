
#!/bin/bash

if [ ! -e import/longtail_lib.a ]
then
	echo Building longtail library, this takes a couple of minutes, hold on...
	pushd import >>/dev/null
	./build_lib.sh
	rc=$?
	popd
	if [[ $rc != 0 ]]; then exit $rc; fi
fi

echo Running test
go test .
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
echo Success
