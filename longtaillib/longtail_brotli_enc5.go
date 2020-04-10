package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -maes -DLONGTAIL_ASSERTS
// #include "longtail/lib/brotli/ext/enc/encode.c"
// #include "longtail/lib/brotli/ext/enc/encoder_dict.c"
// #include "longtail/lib/brotli/ext/enc/entropy_encode.c"
// #include "longtail/lib/brotli/ext/enc/histogram.c"
// #include "longtail/lib/brotli/ext/enc/literal_cost.c"
// #include "longtail/lib/brotli/ext/enc/memory.c"
// #include "longtail/lib/brotli/ext/enc/metablock.c"
// #include "longtail/lib/brotli/ext/enc/static_dict.c"
// #include "longtail/lib/brotli/ext/enc/utf8_util.c"
import "C"
