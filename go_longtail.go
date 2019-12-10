package golongtail

// #cgo CFLAGS: -g -std=gnu99
// #cgo LDFLAGS: -L. -l:longtail_lib.a
// #define _GNU_SOURCE
// #include "longtail/src/longtail.h"
// #include "longtail/lib/longtail_lib.h"
// #include <stdlib.h>
// void progressProxy(void* context, uint32_t total_count, uint32_t done_count);
import "C"
import (
	"fmt"
	"unsafe"
	"runtime"

	"github.com/mattn/go-pointer"
)

type progressFunc func(context interface{}, total int, current int)

type progressProxyData struct {
	progressFunc progressFunc
	context      interface{}
}

func makeProgressProxy(progressFunc progressFunc, context interface{}) progressProxyData {
	return progressProxyData{progressFunc, context}
}

type progressData struct {
	oldPercent int
	task       string
}

func progress(context interface{}, total int, current int) {
	p := context.(*progressData)
	if current < total {
		if p.oldPercent == 0 {
			fmt.Printf("%s: ", p.task)
		}
		percentDone := (100 * current) / total
		if (percentDone - p.oldPercent) >= 5 {
			fmt.Printf("%d%% ", percentDone)
			p.oldPercent = percentDone
		}
		return
	}
	if p.oldPercent != 0 {
		if p.oldPercent != 100 {
			fmt.Printf("100%%")
		}
		fmt.Print(" Done\n")
	}
}

//ChunkFolder hello
func ChunkFolder(folder_path string) int32 {
	progressProxy := makeProgressProxy(progress, &progressData{task: "Indexing"})
	c := pointer.Save(&progressProxy)

	path := C.CString(folder_path)
	defer C.free(unsafe.Pointer(path))

	fs := C.CreateFSStorageAPI()
	hs := C.CreateMeowHashAPI()
	jb := C.CreateBikeshedJobAPI(C.uint32_t(runtime.NumCPU()))
	fileInfos := C.GetFilesRecursively(fs, path)
	fmt.Printf("Files found: %d\n", int(*fileInfos.m_Paths.m_PathCount))

	compressionTypes := make([]C.uint32_t, int(*fileInfos.m_Paths.m_PathCount))
	for i := 1; i < int(*fileInfos.m_Paths.m_PathCount); i++ {
		compressionTypes[i] = 0
	}

	vi := C.CreateVersionIndex(
		fs,
		hs,
		jb,
		(C.JobAPI_ProgressFunc)(C.progressProxy),
		c,
		path,
		(*C.struct_Paths)(&fileInfos.m_Paths),
		fileInfos.m_FileSizes,
		(*C.uint32_t)(unsafe.Pointer(&compressionTypes[0])),
		C.uint32_t(32768))

	chunkCount := int32(*vi.m_ChunkCount);
	fmt.Printf("Chunks made: %d\n", chunkCount)

	C.Longtail_Free(unsafe.Pointer(vi))

	C.Longtail_Free(unsafe.Pointer(fileInfos))
	C.DestroyJobAPI(jb)
	C.DestroyHashAPI(hs)
	C.DestroyStorageAPI(fs)
	pointer.Unref(c)

	return chunkCount
}

/*
func main() {

	size := C.GetVersionIndexSize(
		10,
		20,
		30,
		40)
	println(size)


	return
}
*/
//export progressProxy
func progressProxy(progress unsafe.Pointer, total C.uint32_t, done C.uint32_t) {
	progressProxy := pointer.Restore(progress).(*progressProxyData)
	progressProxy.progressFunc(progressProxy.context, int(total), int(done))
}
