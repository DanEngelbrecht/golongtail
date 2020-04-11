// -build windows
package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -maes -O3
// #include "longtail/src/longtail.c"
// #include "longtail/src/ext/stb_ds.c"
import "C"
