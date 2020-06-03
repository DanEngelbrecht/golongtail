package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -O3 -D_WIN32 -DWINVER=0x0A00 -D_WIN32_WINNT=0x0A00 -DLONGTAIL_ASSERTS
// #include "longtail/lib/longtail_platform.c"
import "C"
