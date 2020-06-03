package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -maes -pthread -O3 -DLZ4_DISABLE_DEPRECATE_WARNINGS
// #include "longtail/lib/lz4/longtail_lz4.c"
// #include "longtail/lib/lz4/ext/lz4.c"
// #include "longtail/lib/lz4/ext/lz4frame.c"
// #include "longtail/lib/lz4/ext/lz4hc.c"
import "C"
