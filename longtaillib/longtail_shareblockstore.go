// -build windows
package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -O3
// #include "longtail/lib/shareblockstore/longtail_shareblockstore.c"
import "C"
