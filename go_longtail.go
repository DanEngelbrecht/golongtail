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
		compressionTypes[i] = C.LIZARD_DEFAULT_COMPRESSION_TYPE
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

//ReadVersionIndex ...
func ReadVersionIndex(indexPath string) *C.struct_VersionIndex {
	cIndexPath := C.CString(indexPath)
	defer C.free(unsafe.Pointer(cIndexPath))

	fs := C.CreateFSStorageAPI()
	defer C.DestroyStorageAPI(fs)

	vindex := C.ReadVersionIndex(fs, cIndexPath)

	return vindex
}

//WriteVersionIndex ...
func WriteVersionIndex(versionIndex *C.struct_VersionIndex, indexPath string) {
	cIndexPath := C.CString(indexPath)
	defer C.free(unsafe.Pointer(cIndexPath))

	fs := C.CreateFSStorageAPI()
	defer C.DestroyStorageAPI(fs)

	C.WriteVersionIndex(fs, versionIndex, cIndexPath)
}

//UpSyncVersion ...
func UpSyncVersion(versionPath string, versionIndexPath string, contentPath string, contentIndexPath string, missingContentPath string, missingContentIndexPath string, outputFormat string, maxChunksPerBlock int, targetBlockSize int, targetChunkSize int) (*C.struct_ContentIndex, error) {
	cVersionPath := C.CString(versionPath)
	defer C.free(unsafe.Pointer(cVersionPath))

	fs := C.CreateFSStorageAPI()
	defer C.DestroyStorageAPI(fs)
	hs := C.CreateMeowHashAPI()
	defer C.DestroyHashAPI(hs)
	jb := C.CreateBikeshedJobAPI(C.uint32_t(runtime.NumCPU()))
	defer C.DestroyJobAPI(jb)

	var vindex *C.struct_VersionIndex = nil
	defer C.Longtail_Free(unsafe.Pointer(vindex))

	cVersionIndexPath := C.CString(versionIndexPath)
	defer C.free(unsafe.Pointer(cVersionIndexPath))

	if len(versionIndexPath) > 0 {
		vindex = C.ReadVersionIndex(fs, cVersionIndexPath)
	}
	if nil == vindex {
		if len(versionPath) == 0 {
			return nil, fmt.Errorf("UpSyncVersion: version folder must be given if no valid version index is given")
		}
		fileInfos := C.GetFilesRecursively(fs, cVersionPath)
		defer C.Longtail_Free(unsafe.Pointer(fileInfos))

		compressionTypes := make([]C.uint32_t, int(*fileInfos.m_Paths.m_PathCount))
		for i := 1; i < int(*fileInfos.m_Paths.m_PathCount); i++ {
			compressionTypes[i] = C.LIZARD_DEFAULT_COMPRESSION_TYPE	// Currently we just use our only compression method
		}

		vindex = C.CreateVersionIndex(
			fs,
			hs,
			jb,
			(C.JobAPI_ProgressFunc)(C.progressProxy),
			nil,
			cVersionPath,
			(*C.struct_Paths)(&fileInfos.m_Paths),
			fileInfos.m_FileSizes,
			(*C.uint32_t)(unsafe.Pointer(&compressionTypes[0])),
			C.uint32_t(targetChunkSize))

		if vindex == nil {
			return nil, fmt.Errorf("UpSyncVersion: failed to create version index for folder `%s`", versionPath)
		}
	}

	cContentPath := C.CString(contentPath)
	defer C.free(unsafe.Pointer(cContentPath))

	var cindex *C.struct_ContentIndex = nil
	defer C.Longtail_Free(unsafe.Pointer(cindex))

	cContentIndexPath := C.CString(contentIndexPath)
	defer C.free(unsafe.Pointer(cContentIndexPath))

	if len(contentIndexPath) > 0 {
		cindex = C.ReadContentIndex(fs, cContentIndexPath)
	}
	if cindex == nil {
		if len(contentPath) == 0 && len(contentIndexPath) == 0 {
			cindex = C.CreateContentIndex(
				hs,
				C.uint64_t(0),
				nil,
				nil,
				nil,
				C.uint32_t(targetBlockSize),
				C.uint32_t(maxChunksPerBlock))
			if cindex == nil {
				return nil, fmt.Errorf("UpSyncVersion: failed to create empty content index")
			}
		}
		if len(contentPath) == 0 {
			return nil, fmt.Errorf("UpSyncVersion: content folder must be given if no valid content index is given")
		}
	}

	missingContentIndex := C.CreateMissingContent(
		hs,
		cindex,
		vindex,
		C.uint32_t(targetBlockSize),
		C.uint32_t(maxChunksPerBlock))

	if missingContentIndex == nil {
		return nil, fmt.Errorf("UpSyncVersion: Failed to generate content index for missing content")
	}

	cr := C.CreateDefaultCompressionRegistry()
	defer C.DestroyCompressionRegistry(cr)

	cMissingContentPath := C.CString(missingContentPath)
	defer C.free(unsafe.Pointer(cMissingContentPath))

	ok := C.WriteContent(
		fs,
		fs,
		cr,
		jb,
		nil,
		nil,
		missingContentIndex,
		vindex,
		cVersionPath,
		cMissingContentPath)

	if ok == 0 {
		C.Longtail_Free(unsafe.Pointer(missingContentIndex))
		return nil, fmt.Errorf("UpSyncVersion: Failed to create new content from `%s` to `%s`", versionPath, missingContentPath)
	}

	if len(versionIndexPath) > 0 {
		ok = C.WriteVersionIndex(
			fs,
			vindex,
			cVersionIndexPath)
		if ok == 0 {
			C.Longtail_Free(unsafe.Pointer(missingContentIndex))
			return nil, fmt.Errorf("UpSyncVersion: Failed to write the new version index to `%s`", versionIndexPath)
		}
	}

	if len(contentIndexPath) > 0 {
		ok = C.WriteContentIndex(
			fs,
			cindex,
			cContentIndexPath)
		if ok == 0 {
			C.Longtail_Free(unsafe.Pointer(missingContentIndex))
			return nil, fmt.Errorf("UpSyncVersion: Failed to write the new content index to `%s`", contentIndexPath)
		}
	}

	return missingContentIndex, nil
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
