package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -maes -O3
// #include "longtail/lib/compressionregistry/longtail_compression_registry.c"
// #include "longtail/lib/compressionregistry/longtail_zstd_compression_registry.c"
// #include "longtail/lib/compressionregistry/longtail_full_compression_registry.c"
import "C"
