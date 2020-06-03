package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -maes -pthread -O3
// #include "longtail/lib/zstd/ext/decompress/huf_decompress.c"
// #include "longtail/lib/zstd/ext/decompress/zstd_ddict.c"
// #include "longtail/lib/zstd/ext/decompress/zstd_decompress.c"
// #include "longtail/lib/zstd/ext/decompress/zstd_decompress_block.c"
import "C"
