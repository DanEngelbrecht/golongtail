package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -maes -pthread -O3 -DBLAKE3_NO_AVX512 -DBLAKE3_NO_AVX2
// #include "longtail/lib/blake3/ext/blake3_portable.c"
// #include "longtail/lib/blake3/ext/blake3_dispatch.c"
// #include "longtail/lib/blake3/ext/blake3.c"
import "C"
