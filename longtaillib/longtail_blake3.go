package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -O3
// #include "longtail/lib/blake3/ext/blake3_portable.c"
// #include "longtail/lib/blake3/ext/blake3_dispatch.c"
// #include "longtail/lib/blake3/ext/blake3.c"
import "C"
