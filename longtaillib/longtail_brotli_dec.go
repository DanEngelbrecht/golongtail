package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -maes -DLONGTAIL_ASSERTS
// #include "longtail/lib/brotli/ext/dec/bit_reader.c"
// #include "longtail/lib/brotli/ext/dec/decode.c"
// #include "longtail/lib/brotli/ext/dec/huffman.c"
// #include "longtail/lib/brotli/ext/dec/state.c"
import "C"
