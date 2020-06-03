package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -pthread -maes -O3
// #include "longtail/lib/meowhash/longtail_meowhash.c"
import "C"
