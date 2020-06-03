package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -maes -pthread -O3
// #include "longtail/lib/zstd/ext/common/debug.c"
// #include "longtail/lib/zstd/ext/common/entropy_common.c"
// #include "longtail/lib/zstd/ext/common/error_private.c"
// #include "longtail/lib/zstd/ext/common/fse_decompress.c"
// #include "longtail/lib/zstd/ext/common/pool.c"
// #include "longtail/lib/zstd/ext/common/threading.c"
// #include "longtail/lib/zstd/ext/common/xxhash.c"
// #include "longtail/lib/zstd/ext/common/zstd_common.c"
import "C"
