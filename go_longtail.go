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
	"unsafe"
	"runtime"

	"github.com/mattn/go-pointer"
)

//ProgressFunc ...
type ProgressFunc func(context interface{}, total int, current int)

//ProgressProxyData ...
type ProgressProxyData struct {
	ProgressFunc ProgressFunc
	Context      interface{}
}

//MakeProgressProxy create data for progress function
func MakeProgressProxy(progressFunc ProgressFunc, context interface{}) ProgressProxyData {
	return ProgressProxyData{progressFunc, context}
}

//LongtailFree ...
func LongtailFree(data unsafe.Pointer) {
	C.Longtail_Free(data)
}

//GetVersionIndex ...
func CreateVersionIndexFromFolder(folderPath string, progressProxyData ProgressProxyData) *C.struct_VersionIndex {
	progressContext := pointer.Save(&progressProxyData)
	defer pointer.Unref(progressContext)

	cFolderPath := C.CString(folderPath)
	defer C.free(unsafe.Pointer(cFolderPath))

	fs := C.CreateFSStorageAPI()
	defer C.DestroyStorageAPI(fs)

	hs := C.CreateMeowHashAPI()
	defer C.DestroyHashAPI(hs)

	jb := C.CreateBikeshedJobAPI(C.uint32_t(runtime.NumCPU()))
	defer C.DestroyJobAPI(jb)

	fileInfos := C.GetFilesRecursively(fs, cFolderPath)
	defer C.Longtail_Free(unsafe.Pointer(fileInfos))

	compressionTypes := make([]C.uint32_t, int(*fileInfos.m_Paths.m_PathCount))
	for i := 1; i < int(*fileInfos.m_Paths.m_PathCount); i++ {
		compressionTypes[i] = 0	// Need to change LIZARD_DEFAULT_COMPRESSION_TYPE form macro!
	}

	vindex := C.CreateVersionIndex(
		fs,
		hs,
		jb,
		(C.JobAPI_ProgressFunc)(C.progressProxy),
		progressContext,
		cFolderPath,
		(*C.struct_Paths)(&fileInfos.m_Paths),
		fileInfos.m_FileSizes,
		(*C.uint32_t)(unsafe.Pointer(&compressionTypes[0])),
		C.uint32_t(32768))

	return vindex
}

/*
//ChunkFolder hello
func ChunkFolder(folderPath string) int32 {
	progressProxy := makeProgressProxy(progress, &progressData{task: "Indexing"})
	c := pointer.Save(&progressProxy)

	path := C.CString(folderPath)
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
*/
//export progressProxy
func progressProxy(progress unsafe.Pointer, total C.uint32_t, done C.uint32_t) {
	progressProxy := pointer.Restore(progress).(*ProgressProxyData)
	progressProxy.ProgressFunc(progressProxy.Context, int(total), int(done))
}
