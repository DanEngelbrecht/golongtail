package golongtail

// #cgo CFLAGS: -g -std=gnu99
// #cgo LDFLAGS: -L. -l:longtail_lib.a
// #define _GNU_SOURCE
// #include "longtail/src/longtail.h"
// #include "longtail/lib/longtail_lib.h"
// #include <stdlib.h>
// void progressProxy(void* context, uint32_t total_count, uint32_t done_count);
// static StorageAPI_HOpenFile Storage_OpenWriteFile(struct StorageAPI* api, const char* path, uint64_t initial_size)
// {
//   return api->OpenWriteFile(api, path, initial_size);
// }
// static int Storage_Write(struct StorageAPI* api, StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input)
// {
//   return api->Write(api, f, offset, length, input);
// }
// static void Storage_CloseWrite(struct StorageAPI* api, StorageAPI_HOpenFile f)
// {
//   return api->CloseWrite(api, f);
// }
// static const char* GetPath(const uint32_t* name_offsets, const char* name_data, uint32_t index)
// {
//   return &name_data[name_offsets[index]];
// }
import "C"
import (
	"fmt"
	"runtime"
	"unsafe"

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

func WriteToStorage(fs *C.struct_StorageAPI, path string, data []byte) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if C.EnsureParentPathExists(fs, cPath) == 0 {
		return fmt.Errorf("WriteToStorage: failed to create parent path for `%s`", path)
	}
	f := C.Storage_OpenWriteFile(fs, cPath, 0)
	if f == nil {
		return fmt.Errorf("WriteToStorage: failed to create file at `%s`", path)
	}
	defer C.Storage_CloseWrite(fs, f)
	if C.Storage_Write(fs, f, 0, (C.uint64_t)(len(data)), (unsafe.Pointer(&data[0]))) == 0 {
		return fmt.Errorf("WriteToStorage: failed to write %d bytes to file `%s`", len(data), path)
	}
	return nil
}

func CreateMeowHashAPI() *C.struct_HashAPI {
	return C.CreateMeowHashAPI()
}

func DestroyHashAPI(api *C.struct_HashAPI) {
	C.DestroyHashAPI(api)
}

func CreateFSStorageAPI() *C.struct_StorageAPI {
	return C.CreateFSStorageAPI()
}

func CreateInMemStorageAPI() *C.struct_StorageAPI {
	return C.CreateInMemStorageAPI()
}

func DestroyStorageAPI(api *C.struct_StorageAPI) {
	C.DestroyStorageAPI(api)
}

func CreateLizardCompressionAPI() *C.struct_CompressionAPI {
	return C.CreateLizardCompressionAPI()
}

func DestroyCompressionAPI(api *C.struct_CompressionAPI) {
	C.DestroyCompressionAPI(api)
}

func CreateBikeshedJobAPI(workerCount uint32) *C.struct_JobAPI {
	return C.CreateBikeshedJobAPI(C.uint32_t(workerCount))
}

func DestroyJobAPI(api *C.struct_JobAPI) {
	C.DestroyJobAPI(api)
}

func CreateDefaultCompressionRegistry() *C.struct_CompressionRegistry {
	return C.CreateDefaultCompressionRegistry()
}

func DestroyCompressionRegistry(registry *C.struct_CompressionRegistry) {
	C.DestroyCompressionRegistry(registry)
}

func GetNoCompressionType() uint32 {
	return uint32(C.NO_COMPRESSION_TYPE)
}

func GetLizardDefaultCompressionType() uint32 {
	return uint32(C.LIZARD_DEFAULT_COMPRESSION_TYPE)
}

func LongtailAlloc(size uint64) unsafe.Pointer {
	return C.Longtail_Alloc(C.size_t(size))
}

func LongtailFree(data unsafe.Pointer) {
	C.Longtail_Free(data)
}

func Longtail_Strdup(s *C.char) *C.char {
	return C.Longtail_Strdup(s)
}

func GetFilesRecursively(fs *C.struct_StorageAPI, rootPath string) *C.struct_FileInfos {
	cFolderPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cFolderPath))
	return C.GetFilesRecursively(fs, cFolderPath)
}

func GetPath(paths *C.struct_Paths, index uint32) string {
	cPath := C.GetPath(paths.m_Offsets, paths.m_Data, C.uint32_t(index))
	return C.GoString(cPath)
}

func GetVersionIndexPath(vindex *C.struct_VersionIndex, index uint32) string {
	cPath := C.GetPath(vindex.m_NameOffsets, vindex.m_NameData, C.uint32_t(index))
	return C.GoString(cPath)
}

func CreateVersionIndex(
	fs *C.struct_StorageAPI,
	hash *C.struct_HashAPI,
	job *C.struct_JobAPI,
	progressFunc ProgressFunc,
	context interface{},
	rootPath string,
	paths *C.struct_Paths,
	assetSizes [] uint64,
	assetCompressionTypes []uint32,
	maxChunkSize uint32) *C.struct_VersionIndex {

	progressProxyData := MakeProgressProxy(progressFunc, context)
	progressContext := pointer.Save(&progressProxyData)
	defer pointer.Unref(progressContext)

	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))
	
	cAssetSizes := (*C.uint64_t)(unsafe.Pointer(&assetSizes[0]))
	cAssetCompressionTypes := (*C.uint32_t)(unsafe.Pointer(&assetCompressionTypes[0]))

	vindex := C.CreateVersionIndex(
		fs,
		hash,
		job,
		(C.JobAPI_ProgressFunc)(C.progressProxy),
		progressContext,
		cRootPath,
		paths,
		cAssetSizes,
		cAssetCompressionTypes,
		C.uint32_t(maxChunkSize))

	return vindex
}

func WriteVersionIndex(fs *C.struct_StorageAPI, index *C.struct_VersionIndex, path string) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if C.WriteVersionIndex(fs, index, cPath) == 0 {
		return fmt.Errorf("WriteVersionIndex: failed to write index to `%s`", path)
	}
	return nil
}

func ReadVersionIndex(fs *C.struct_StorageAPI, path string) *C.struct_VersionIndex {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	return C.ReadVersionIndex(fs, cPath)
}

func CreateContentIndex(
	hash *C.struct_HashAPI,
	chunkCount uint64,
	chunkHashes []uint64,
	chunkSizes []uint32,
	compressionTypes []uint32,
	maxBlockSize uint32,
	maxChunksPerBlock uint32) *C.struct_ContentIndex {

	cChunkHashes := (*C.TLongtail_Hash)(unsafe.Pointer(&chunkHashes[0]))
	cChunkSizes := (*C.uint32_t)(unsafe.Pointer(&chunkSizes[0]))
	cCompressionTypes := (*C.uint32_t)(unsafe.Pointer(&compressionTypes[0]))

	cindex := C.CreateContentIndex(
		hash,
		C.uint64_t(chunkCount),
		cChunkHashes,
		cChunkSizes,
		cCompressionTypes,
		C.uint32_t(maxBlockSize),
		C.uint32_t(maxChunksPerBlock))

	return cindex
}

func WriteContentIndex(fs *C.struct_StorageAPI, index *C.struct_ContentIndex, path string) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if C.WriteContentIndex(fs, index, cPath) == 0 {
		return fmt.Errorf("WriteContentIndex: failed to write index to `%s`", path)
	}
	return nil
}

func ReadContentIndex(fs *C.struct_StorageAPI, path string) *C.struct_ContentIndex {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	return C.ReadContentIndex(fs, cPath)
}

//WriteContent
//ReadContent
//CreateMissingContent
//RetargetContent
//MergeContentIndex
//WriteVersion
//CreateVersionDiff
//ChangeVersion

//GetVersionIndex ...
func CreateVersionIndexFromFolder(fs *C.struct_StorageAPI, folderPath string, progressProxyData ProgressProxyData) *C.struct_VersionIndex {
	progressContext := pointer.Save(&progressProxyData)
	defer pointer.Unref(progressContext)

	cFolderPath := C.CString(folderPath)
	defer C.free(unsafe.Pointer(cFolderPath))

	//	fs := C.CreateFSStorageAPI()
	//	defer C.DestroyStorageAPI(fs)

	hs := C.CreateMeowHashAPI()
	defer C.DestroyHashAPI(hs)

	jb := C.CreateBikeshedJobAPI(C.uint32_t(runtime.NumCPU()))
	defer C.DestroyJobAPI(jb)

	fileInfos := C.GetFilesRecursively(fs, cFolderPath)
	defer C.Longtail_Free(unsafe.Pointer(fileInfos))

	compressionTypes := make([]C.uint32_t, int(*fileInfos.m_Paths.m_PathCount))
	for i := 0; i < int(*fileInfos.m_Paths.m_PathCount); i++ {
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

/*
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
			compressionTypes[i] = C.LIZARD_DEFAULT_COMPRESSION_TYPE // Currently we just use our only compression method
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
*/
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
