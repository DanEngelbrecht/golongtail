package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -maes -DLONGTAIL_ASSERTS
// #include "longtail/lib/blake3/ext/blake3_avx2.c"
// #include "longtail/lib/blake3/ext/blake3_avx512.c"
// #include "longtail/lib/blake3/ext/blake3_sse41.c"
import "C"
