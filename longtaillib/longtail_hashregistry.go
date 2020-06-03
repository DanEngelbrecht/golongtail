package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -pthread -O3
// #include "longtail/lib/hashregistry/longtail_hash_registry.c"
// #include "longtail/lib/hashregistry/longtail_blake3_hash_registry.c"
// #include "longtail/lib/hashregistry/longtail_full_hash_registry.c"
import "C"
