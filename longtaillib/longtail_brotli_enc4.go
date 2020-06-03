package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -maes -pthread -O3
// #include "longtail/lib/brotli/ext/enc/compress_fragment_two_pass.c"
// #include "longtail/lib/brotli/ext/enc/dictionary_hash.c"
import "C"
