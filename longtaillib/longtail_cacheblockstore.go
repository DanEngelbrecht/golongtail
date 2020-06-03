// -build windows
package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -pthread -O3
// #include "longtail/lib/cacheblockstore/longtail_cacheblockstore.c"
import "C"
