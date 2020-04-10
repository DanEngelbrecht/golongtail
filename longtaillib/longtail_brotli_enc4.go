package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -maes -DLONGTAIL_ASSERTS
// #include "longtail/lib/brotli/ext/enc/compress_fragment_two_pass.c"
// #include "longtail/lib/brotli/ext/enc/dictionary_hash.c"
import "C"
