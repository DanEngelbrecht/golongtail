package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -mavx2 -mavx512vl -mavx512f -fno-asynchronous-unwind-tables -O3
// #include "longtail/lib/blake3/ext/blake3_avx512.c"
import "C"
