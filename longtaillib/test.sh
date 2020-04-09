
#!/bin/bash

./build_longtail.sh

echo Running test
go test .
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
echo Success
