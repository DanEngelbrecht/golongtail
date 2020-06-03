package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -pthread -O3
// #include "longtail/lib/brotli/ext/dec/bit_reader.c"
// #include "longtail/lib/brotli/ext/dec/decode.c"
// #include "longtail/lib/brotli/ext/dec/huffman.c"
// #include "longtail/lib/brotli/ext/dec/state.c"
import "C"
