// -build windows
package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -maes -pthread -O3
// #include "longtail/lib/shareblockstore/longtail_shareblockstore.c"
import "C"
