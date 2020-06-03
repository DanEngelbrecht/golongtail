package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -maes -pthread -O3
// #include "longtail/lib/zstd/ext/compress/fse_compress.c"
// #include "longtail/lib/zstd/ext/compress/hist.c"
// #include "longtail/lib/zstd/ext/compress/huf_compress.c"
// #include "longtail/lib/zstd/ext/compress/zstdmt_compress.c"
// #include "longtail/lib/zstd/ext/compress/zstd_compress.c"
// #include "longtail/lib/zstd/ext/compress/zstd_compress_literals.c"
// #include "longtail/lib/zstd/ext/compress/zstd_compress_sequences.c"
// #include "longtail/lib/zstd/ext/compress/zstd_compress_superblock.c"
// #include "longtail/lib/zstd/ext/compress/zstd_double_fast.c"
// #include "longtail/lib/zstd/ext/compress/zstd_fast.c"
// #include "longtail/lib/zstd/ext/compress/zstd_lazy.c"
// #include "longtail/lib/zstd/ext/compress/zstd_ldm.c"
// #include "longtail/lib/zstd/ext/compress/zstd_opt.c"
import "C"
