package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -O3
// #include "longtail/lib/brotli/ext/common/dictionary.c"
// #include "longtail/lib/brotli/ext/common/transform.c"
import "C"
