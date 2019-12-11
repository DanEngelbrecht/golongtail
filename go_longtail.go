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
	"reflect"
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

func WriteToStorage(storageApi *C.struct_StorageAPI, path string, data []byte) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if C.EnsureParentPathExists(storageApi, cPath) == 0 {
		return fmt.Errorf("WriteToStorage: failed to create parent path for `%s`", path)
	}
	f := C.Storage_OpenWriteFile(storageApi, cPath, 0)
	if f == nil {
		return fmt.Errorf("WriteToStorage: failed to create file at `%s`", path)
	}
	defer C.Storage_CloseWrite(storageApi, f)
	if len(data) > 0 {
		if C.Storage_Write(storageApi, f, 0, (C.uint64_t)(len(data)), (unsafe.Pointer(&data[0]))) == 0 {
			return fmt.Errorf("WriteToStorage: failed to write %d bytes to file `%s`", len(data), path)
		}
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

func GetFilesRecursively(storageApi *C.struct_StorageAPI, rootPath string) *C.struct_FileInfos {
	cFolderPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cFolderPath))
	return C.GetFilesRecursively(storageApi, cFolderPath)
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
	storageApi *C.struct_StorageAPI,
	hashAPI *C.struct_HashAPI,
	jobAPI *C.struct_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	versionPath string,
	compressionType uint32,
	targetChunkSize uint32) (error, *C.struct_VersionIndex) {

	progressProxyData := MakeProgressProxy(progressFunc, progressContext)
	cProgressProxyData := pointer.Save(&progressProxyData)
	defer pointer.Unref(cProgressProxyData)

	cVersionPath := C.CString(versionPath)
	defer C.free(unsafe.Pointer(cVersionPath))

	fileInfos := C.GetFilesRecursively(storageApi, cVersionPath)
	defer C.Longtail_Free(unsafe.Pointer(fileInfos))

	compressionTypes := make([]C.uint32_t, int(*fileInfos.m_Paths.m_PathCount))
	for i := 1; i < int(*fileInfos.m_Paths.m_PathCount); i++ {
		compressionTypes[i] = C.uint32_t(compressionType)
	}

	vindex := C.CreateVersionIndex(
		storageApi,
		hashAPI,
		jobAPI,
		(C.JobAPI_ProgressFunc)(C.progressProxy),
		cProgressProxyData,
		cVersionPath,
		(*C.struct_Paths)(&fileInfos.m_Paths),
		fileInfos.m_FileSizes,
		(*C.uint32_t)(unsafe.Pointer(&compressionTypes[0])),
		C.uint32_t(targetChunkSize))

	if vindex == nil {
		return fmt.Errorf("CreateVersionIndex: failed to create version index"), nil
	}

	return nil, vindex
}

func WriteVersionIndex(storageApi *C.struct_StorageAPI, index *C.struct_VersionIndex, path string) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if C.WriteVersionIndex(storageApi, index, cPath) == 0 {
		return fmt.Errorf("WriteVersionIndex: failed to write index to `%s`", path)
	}
	return nil
}

func ReadVersionIndex(storageApi *C.struct_StorageAPI, path string) *C.struct_VersionIndex {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	return C.ReadVersionIndex(storageApi, cPath)
}

func CreateContentIndex(
	hashAPI *C.struct_HashAPI,
	chunkCount uint64,
	chunkHashes []uint64,
	chunkSizes []uint32,
	compressionTypes []uint32,
	maxBlockSize uint32,
	maxChunksPerBlock uint32) (error, *C.struct_ContentIndex) {

	if chunkCount == 0 {
		return nil, C.CreateContentIndex(
			hashAPI,
			0,
			nil,
			nil,
			nil,
			C.uint32_t(maxBlockSize),
			C.uint32_t(maxChunksPerBlock))
	}
	cChunkHashes := (*C.TLongtail_Hash)(unsafe.Pointer(&chunkHashes[0]))
	cChunkSizes := (*C.uint32_t)(unsafe.Pointer(&chunkSizes[0]))
	cCompressionTypes := (*C.uint32_t)(unsafe.Pointer(&compressionTypes[0]))

	cindex := C.CreateContentIndex(
		hashAPI,
		C.uint64_t(chunkCount),
		cChunkHashes,
		cChunkSizes,
		cCompressionTypes,
		C.uint32_t(maxBlockSize),
		C.uint32_t(maxChunksPerBlock))

	if cindex == nil {
		return fmt.Errorf("CreateContentIndex: failed to create content index"), nil
	}

	return nil, cindex
}

func WriteContentIndex(storageApi *C.struct_StorageAPI, index *C.struct_ContentIndex, path string) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if C.WriteContentIndex(storageApi, index, cPath) == 0 {
		return fmt.Errorf("WriteContentIndex: failed to write index to `%s`", path)
	}
	return nil
}

func ReadContentIndex(storageApi *C.struct_StorageAPI, path string) *C.struct_ContentIndex {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	return C.ReadContentIndex(storageApi, cPath)
}

func WriteContent(
	sourceStorageAPI *C.struct_StorageAPI,
	targetStorageApi *C.struct_StorageAPI,
	compressionRegistry *C.struct_CompressionRegistry,
	jobAPI *C.struct_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	contentIndex *C.struct_ContentIndex,
	versionIndex *C.struct_VersionIndex,
	versionFolderPath string,
	contentFolderPath string) error {

	progressProxyData := MakeProgressProxy(progressFunc, progressContext)
	cProgressProxyData := pointer.Save(&progressProxyData)
	defer pointer.Unref(cProgressProxyData)

	cVersionFolderPath := C.CString(versionFolderPath)
	defer C.free(unsafe.Pointer(cVersionFolderPath))

	cContentFolderPath := C.CString(contentFolderPath)
	defer C.free(unsafe.Pointer(cContentFolderPath))

	ok := C.WriteContent(
		sourceStorageAPI,
		targetStorageApi,
		compressionRegistry,
		jobAPI,
		(C.JobAPI_ProgressFunc)(C.progressProxy),
		cProgressProxyData,
		contentIndex,
		versionIndex,
		cVersionFolderPath,
		cContentFolderPath)
	if ok == 0 {
		return fmt.Errorf("WriteContent: failed to write content to `%s`", contentFolderPath)
	}
	return nil
}

func ReadContent(
	sourceStorageAPI *C.struct_StorageAPI,
	hashAPI *C.struct_HashAPI,
	jobAPI *C.struct_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	contentFolderPath string) (error, *C.struct_ContentIndex) {

	progressProxyData := MakeProgressProxy(progressFunc, progressContext)
	cProgressProxyData := pointer.Save(&progressProxyData)
	defer pointer.Unref(cProgressProxyData)

	cContentFolderPath := C.CString(contentFolderPath)
	defer C.free(unsafe.Pointer(cContentFolderPath))

	contentIndex := C.ReadContent(
		sourceStorageAPI,
		hashAPI,
		jobAPI,
		(C.JobAPI_ProgressFunc)(C.progressProxy),
		cProgressProxyData,
		cContentFolderPath)
	if contentIndex == nil {
		return fmt.Errorf("ReadContent: failed to read content from `%s`", contentFolderPath), nil
	}
	return nil, contentIndex
}

func CreateMissingContent(
	hashAPI *C.struct_HashAPI,
	contentIndex *C.struct_ContentIndex,
	versionIndex *C.struct_VersionIndex,
	maxBlockSize uint32,
	maxChunksPerBlock uint32) (error, *C.struct_ContentIndex) {

	missingContentIndex := C.CreateMissingContent(
		hashAPI,
		contentIndex,
		versionIndex,
		C.uint32_t(maxBlockSize),
		C.uint32_t(maxChunksPerBlock))
	if missingContentIndex == nil {
		return fmt.Errorf("CreateMissingContent: failed to make missing content"), nil
	}
	return nil, missingContentIndex
}

func RetargetContent(
	referenceContentIndex *C.struct_ContentIndex,
	contentIndex *C.struct_ContentIndex) (error, *C.struct_ContentIndex) {
	retargetedContentIndex := C.RetargetContent(referenceContentIndex, contentIndex)
	if retargetedContentIndex == nil {
		return fmt.Errorf("RetargetContent: failed to make retargeted content"), nil
	}
	return nil, retargetedContentIndex
}

func MergeContentIndex(
	localContentIndex *C.struct_ContentIndex,
	remoteContentIndex *C.struct_ContentIndex) (error, *C.struct_ContentIndex) {
	mergedContentIndex := C.MergeContentIndex(localContentIndex, remoteContentIndex)
	if mergedContentIndex == nil {
		return fmt.Errorf("MergeContentIndex: failed to merge content indexes"), nil
	}
	return nil, mergedContentIndex
}

func WriteVersion(
	contentStorageAPI *C.struct_StorageAPI,
	versionStorageAPI *C.struct_StorageAPI,
	compressionRegistry *C.struct_CompressionRegistry,
	jobAPI *C.struct_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	contentIndex *C.struct_ContentIndex,
	versionIndex *C.struct_VersionIndex,
	contentFolderPath string,
	versionFolderPath string) error {

	progressProxyData := MakeProgressProxy(progressFunc, progressContext)
	cProgressProxyData := pointer.Save(&progressProxyData)
	defer pointer.Unref(cProgressProxyData)

	cContentFolderPath := C.CString(contentFolderPath)
	defer C.free(unsafe.Pointer(cContentFolderPath))

	cVersionFolderPath := C.CString(versionFolderPath)
	defer C.free(unsafe.Pointer(cVersionFolderPath))

	ok := C.WriteVersion(
		contentStorageAPI,
		versionStorageAPI,
		compressionRegistry,
		jobAPI,
		(C.JobAPI_ProgressFunc)(C.progressProxy),
		cProgressProxyData,
		contentIndex,
		versionIndex,
		cContentFolderPath,
		cVersionFolderPath)
	if ok == 0 {
		return fmt.Errorf("WriteVersion: failed to write version to `%s` from `%s`", versionFolderPath, contentFolderPath)
	}
	return nil
}

//CreateVersionDiff do we really need this? Maybe ChangeVersion should create one on the fly?
func CreateVersionDiff(
	sourceVersionIndex *C.struct_VersionIndex,
	targetVersionIndex *C.struct_VersionIndex) (error, *C.struct_VersionDiff) {
	versionDiff := C.CreateVersionDiff(sourceVersionIndex, targetVersionIndex)
	if versionDiff == nil {
		return fmt.Errorf("CreateVersionDiff: failed to diff versions"), nil
	}
	return nil, versionDiff
}

func ChangeVersion(
	contentStorageAPI *C.struct_StorageAPI,
	versionStorageAPI *C.struct_StorageAPI,
	hashAPI *C.struct_HashAPI,
	jobAPI *C.struct_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	compressionRegistry *C.struct_CompressionRegistry,
	contentIndex *C.struct_ContentIndex,
	sourceVersionIndex *C.struct_VersionIndex,
	targetVersionIndex *C.struct_VersionIndex,
	versionDiff *C.struct_VersionDiff,
	contentFolderPath string,
	versionFolderPath string) error {

	progressProxyData := MakeProgressProxy(progressFunc, progressContext)
	cProgressProxyData := pointer.Save(&progressProxyData)
	defer pointer.Unref(cProgressProxyData)

	cContentFolderPath := C.CString(contentFolderPath)
	defer C.free(unsafe.Pointer(cContentFolderPath))

	cVersionFolderPath := C.CString(versionFolderPath)
	defer C.free(unsafe.Pointer(cVersionFolderPath))

	ok := C.ChangeVersion(
		contentStorageAPI,
		versionStorageAPI,
		hashAPI,
		jobAPI,
		(C.JobAPI_ProgressFunc)(C.progressProxy),
		cProgressProxyData,
		compressionRegistry,
		contentIndex,
		sourceVersionIndex,
		targetVersionIndex,
		versionDiff,
		cContentFolderPath,
		cVersionFolderPath)
	if ok == 0 {
		return fmt.Errorf("ChangeVersion: failed to update version `%s` from `%s`", versionFolderPath, contentFolderPath)
	}
	return nil
}

//GetVersionIndex ...
func CreateVersionIndexFromFolder(storageApi *C.struct_StorageAPI, folderPath string, progressProxyData ProgressProxyData) *C.struct_VersionIndex {
	progressContext := pointer.Save(&progressProxyData)
	defer pointer.Unref(progressContext)

	cFolderPath := C.CString(folderPath)
	defer C.free(unsafe.Pointer(cFolderPath))

	hs := C.CreateMeowHashAPI()
	defer C.DestroyHashAPI(hs)

	jb := C.CreateBikeshedJobAPI(C.uint32_t(runtime.NumCPU()))
	defer C.DestroyJobAPI(jb)

	fileInfos := C.GetFilesRecursively(storageApi, cFolderPath)
	defer C.Longtail_Free(unsafe.Pointer(fileInfos))

	compressionTypes := make([]C.uint32_t, int(*fileInfos.m_Paths.m_PathCount))
	for i := 0; i < int(*fileInfos.m_Paths.m_PathCount); i++ {
		compressionTypes[i] = C.LIZARD_DEFAULT_COMPRESSION_TYPE
	}

	vindex := C.CreateVersionIndex(
		storageApi,
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

//UpSyncVersion ...
func GetMissingBlocks(
	contentStorageAPI *C.struct_StorageAPI,
	versionStorageAPI *C.struct_StorageAPI,
	hashAPI *C.struct_HashAPI,
	jobAPI *C.struct_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	versionPath string,
	versionIndexPath string,
	contentPath string,
	contentIndexPath string,
	missingContentPath string,
	missingContentIndexPath string,
	compressionType uint32,
	maxChunksPerBlock uint32,
	targetBlockSize uint32,
	targetChunkSize uint32) (error, []uint64) {

	var vindex *C.struct_VersionIndex = nil
	defer C.Longtail_Free(unsafe.Pointer(vindex))
	err := error(nil)

	if len(versionIndexPath) > 0 {
		vindex = ReadVersionIndex(versionStorageAPI, versionIndexPath)
	}
	if nil == vindex {
		err, vindex = CreateVersionIndex(
			versionStorageAPI,
			hashAPI,
			jobAPI,
			progressFunc,
			progressContext,
			versionPath,
			compressionType,
			targetChunkSize)

		if err != nil {
			return err, nil
		}
	}

	cContentPath := C.CString(contentPath)
	defer C.free(unsafe.Pointer(cContentPath))

	var cindex *C.struct_ContentIndex = nil
	defer C.Longtail_Free(unsafe.Pointer(cindex))

	cContentIndexPath := C.CString(contentIndexPath)
	defer C.free(unsafe.Pointer(cContentIndexPath))

	if len(contentIndexPath) > 0 {
		cindex = ReadContentIndex(contentStorageAPI, contentIndexPath)
	}
	if cindex == nil {
		err, cindex = CreateContentIndex(
			hashAPI,
			0,
			nil,
			nil,
			nil,
			targetBlockSize,
			maxChunksPerBlock)
		if err != nil {
			return err, nil
		}
	}

	err, missingContentIndex := CreateMissingContent(
		hashAPI,
		cindex,
		vindex,
		targetBlockSize,
		maxChunksPerBlock)
	defer C.Longtail_Free(unsafe.Pointer(missingContentIndex))

	if err != nil {
		return err, nil
	}

	compressionRegistry := C.CreateDefaultCompressionRegistry()
	defer C.DestroyCompressionRegistry(compressionRegistry)

	err = WriteContent(
		versionStorageAPI,
		contentStorageAPI,
		compressionRegistry,
		jobAPI,
		progressFunc,
		progressContext,
		missingContentIndex,
		vindex,
		versionPath,
		missingContentPath)

	if err != nil {
		return err, nil
	}

	if len(versionIndexPath) > 0 {
		err = WriteVersionIndex(
			contentStorageAPI,
			vindex,
			versionIndexPath)
		if err != nil {
			return err, nil
		}
	}

	if len(contentIndexPath) > 0 {
		err = WriteContentIndex(
			contentStorageAPI,
			cindex,
			contentIndexPath)
		if err != nil {
			return err, nil
		}
	}

	blockCount := uint64(*missingContentIndex.m_BlockCount)
	blockHashes := make([]uint64, blockCount)

    var oids []uint64
    sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&oids)))
    sliceHeader.Cap = int(blockCount)
    sliceHeader.Len = int(blockCount)
    sliceHeader.Data = uintptr(unsafe.Pointer(missingContentIndex.m_BlockHashes))

	for i := 0 ; i < int(blockCount); i++ {
		blockHashes[i] = oids[i]
	}

	return nil, blockHashes
}

/*
//ChunkFolder hello
func ChunkFolder(folderPath string) int32 {
	progressProxy := makeProgressProxy(progress, &progressData{task: "Indexing"})
	c := pointer.Save(&progressProxy)

	path := C.CString(folderPath)
	defer C.free(unsafe.Pointer(path))

	storageApi := C.CreateFSStorageAPI()
	hs := C.CreateMeowHashAPI()
	jb := C.CreateBikeshedJobAPI(C.uint32_t(runtime.NumCPU()))
	fileInfos := C.GetFilesRecursively(storageApi, path)
	fmt.Printf("Files found: %d\n", int(*fileInfos.m_Paths.m_PathCount))

	compressionTypes := make([]C.uint32_t, int(*fileInfos.m_Paths.m_PathCount))
	for i := 1; i < int(*fileInfos.m_Paths.m_PathCount); i++ {
		compressionTypes[i] = 0
	}

	vi := C.CreateVersionIndex(
		storageApi,
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
	C.DestroyStorageAPI(storageApi)
	pointer.Unref(c)

	return chunkCount
}
*/
//export progressProxy
func progressProxy(progress unsafe.Pointer, total C.uint32_t, done C.uint32_t) {
	progressProxy := pointer.Restore(progress).(*ProgressProxyData)
	progressProxy.ProgressFunc(progressProxy.Context, int(total), int(done))
}
