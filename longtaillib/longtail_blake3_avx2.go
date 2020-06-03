package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -maes -mavx2 -O3
// #include "longtail/lib/blake3/ext/blake3_avx2.c"
import "C"
