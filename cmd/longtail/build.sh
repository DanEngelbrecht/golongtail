
#!/bin/bash

echo Building longtail executable
go build -ldflags="-s -w" .
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
echo Success
