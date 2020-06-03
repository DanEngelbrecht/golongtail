package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -maes -pthread -O3
// #include "longtail/lib/brotli/ext/enc/backward_references_hq.c"
// #include "longtail/lib/brotli/ext/enc/bit_cost.c"
// #include "longtail/lib/brotli/ext/enc/block_splitter.c"
// #include "longtail/lib/brotli/ext/enc/brotli_bit_stream.c"
// #include "longtail/lib/brotli/ext/enc/cluster.c"
import "C"
