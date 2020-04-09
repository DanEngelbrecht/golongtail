package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -maes -DLONGTAIL_ASSERTS
// #include "longtail/lib/blake2/ext/blake2s.c"
import "C"
