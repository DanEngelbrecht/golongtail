package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -pthread -O3
// #include "longtail/lib/brotli/ext/common/dictionary.c"
// #include "longtail/lib/brotli/ext/common/transform.c"
import "C"
