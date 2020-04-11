// -build windows
package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -maes -O3
// #include "longtail/lib/cacheblockstore/longtail_cacheblockstore.c"
import "C"
