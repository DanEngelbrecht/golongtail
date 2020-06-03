package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -mavx512vl -mavx512f -O3 -fno-asynchronous-unwind-tables
// #include "longtail/lib/blake3/ext/blake3_avx512.c"
import "C"
