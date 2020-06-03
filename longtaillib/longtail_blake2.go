package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -maes -pthread -O3
// #include "longtail/lib/blake2/ext/blake2s.c"
import "C"
