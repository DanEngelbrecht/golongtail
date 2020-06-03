package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -pthread -O3
// #include "longtail/lib/atomiccancel/longtail_atomiccancel.c"
// #include "longtail/lib/blake2/longtail_blake2.c"
// #include "longtail/lib/blake3/longtail_blake3.c"
// #include "longtail/lib/bikeshed/longtail_bikeshed.c"
// #include "longtail/lib/filestorage/longtail_filestorage.c"
// #include "longtail/lib/fsblockstore/longtail_fsblockstore.c"
// #include "longtail/lib/memstorage/longtail_memstorage.c"
// #include "longtail/lib/zstd/longtail_zstd.c"
import "C"
