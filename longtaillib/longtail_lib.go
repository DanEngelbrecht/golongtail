package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -maes -O3
// #include "longtail/lib/blake2/longtail_blake2.c"
// #include "longtail/lib/blake3/longtail_blake3.c"
// #include "longtail/lib/bikeshed/longtail_bikeshed.c"
// #include "longtail/lib/filestorage/longtail_filestorage.c"
// #include "longtail/lib/fsblockstore/longtail_fsblockstore.c"
// #include "longtail/lib/memstorage/longtail_memstorage.c"
// #include "longtail/lib/meowhash/longtail_meowhash.c"
// #include "longtail/lib/zstd/longtail_zstd.c"
import "C"
