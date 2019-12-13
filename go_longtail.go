package golongtail

// #cgo CFLAGS: -g -std=gnu99
// #cgo LDFLAGS: -L. -l:longtail_lib.a
// #define _GNU_SOURCE
// #include "longtail/src/longtail.h"
// #include "longtail/lib/longtail_lib.h"
// #include <stdlib.h>
// void progressProxy(void* context, uint32_t total_count, uint32_t done_count);
// void logProxy(void* context, int level, char* str);
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
// static StorageAPI_HOpenFile Storage_OpenReadFile(struct StorageAPI* api, const char* path)
// {
//   return api->OpenReadFile(api, path);
// }
// static int Storage_Read(struct StorageAPI* api, StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output)
// {
//   return api->Read(api, f, offset, length, output);
// }
// static void Storage_CloseRead(struct StorageAPI* api, StorageAPI_HOpenFile f)
// {
//   return api->CloseRead(api, f);
// }
// static uint64_t Storage_GetSize(struct StorageAPI* api, StorageAPI_HOpenFile f)
// {
//   return api->GetSize(api, f);
// }
// static char* Storage_ConcatPath(struct StorageAPI* api, const char* root_path, const char* sub_path)
// {
//   return api->ConcatPath(api, root_path, sub_path);
// }
// static const char* GetPath(const uint32_t* name_offsets, const char* name_data, uint32_t index)
// {
//   return &name_data[name_offsets[index]];
// }
// static void* ReadFromStorage(struct StorageAPI* api, const char* rootPath, const char* blockPath)
// {
//    char* full_path = api->ConcatPath(api, rootPath, blockPath);
//    StorageAPI_HOpenFile block_file = api->OpenReadFile(api, full_path);
//    if (!block_file) {
//      Longtail_Free(full_path);
//      return 0;
//    }
//    uint64_t blockSize = api->GetSize(api, block_file);
//    void* buffer = Longtail_Alloc((size_t)blockSize);
//    api->Read(api, block_file, 0, blockSize, buffer);
//    api->CloseRead(api, block_file);
//    Longtail_Free(full_path);
//    return buffer;
// }
// static int WriteToStorage(struct StorageAPI* api, const char* rootPath, const char* blockPath, uint64_t blockSize, void* blockData)
// {
//    char* full_path = api->ConcatPath(api, rootPath, blockPath);
//    StorageAPI_HOpenFile block_file = api->OpenWriteFile(api, full_path, blockSize);
//    if (!block_file) {
//      Longtail_Free(full_path);
//      return 0;
//    }
//    api->Write(api, block_file, 0, blockSize, blockData);
//    api->CloseWrite(api, block_file);
//    Longtail_Free(full_path);
//    return 1;
// }
import "C"
import (
  "fmt"
  "runtime"
  "unsafe"

  "github.com/mattn/go-pointer"
)

// ReadFromStorage ...
func ReadFromStorage(api *C.struct_StorageAPI, rootPath string, path string) ([]byte, error) {
	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cFullPath := C.Storage_ConcatPath(api, cRootPath, cPath)
	defer C.free(unsafe.Pointer(cFullPath))
	
	f := C.Storage_OpenReadFile(api, cFullPath)
	if f == nil {
	  return nil, nil
	}
	blockSize := C.Storage_GetSize(api, f)
	blockData := make([]uint8, int(blockSize))
	C.Storage_Read(api, f, 0, blockSize, unsafe.Pointer(&blockData[0]))
	C.Storage_CloseRead(api, f)
	return blockData, nil
}

// WriteToStorage ...
func WriteToStorage(api *C.struct_StorageAPI, rootPath string, path string, blockData []byte) error {
	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cFullPath := C.Storage_ConcatPath(api, cRootPath, cPath)
	defer C.free(unsafe.Pointer(cFullPath))

  if C.EnsureParentPathExists(api, cFullPath) == 0 {
    return fmt.Errorf("WriteToStorage: failed to create parent path folder `%s` path `%s`", rootPath, path)
  }

	blockSize := C.uint64_t(len(blockData))

  f := C.Storage_OpenWriteFile(api, cFullPath, blockSize)
  if blockSize > 0 {
    C.Storage_Write(api, f, 0, blockSize, unsafe.Pointer(&blockData[0]))
  }
	C.Storage_CloseWrite(api, f)
	return nil
}

type progressFunc func(context interface{}, total int, current int)

type progressProxyData struct {
  progressFunc progressFunc
  Context      interface{}
}

func makeProgressProxy(progressFunc progressFunc, context interface{}) progressProxyData {
  return progressProxyData{progressFunc, context}
}

type logFunc func(context interface{}, level int, log string)
type logProxyData struct {
  logFunc   logFunc
  Context     interface{}
}

func makeLogProxy(logFunc logFunc, context interface{}) logProxyData {
  return logProxyData{logFunc, context}
}
/*
// WriteToStorage ...
func WriteToStorage(storageAPI *C.struct_StorageAPI, path string, data []byte) error {
  cPath := C.CString(path)
  defer C.free(unsafe.Pointer(cPath))
  if C.EnsureParentPathExists(storageAPI, cPath) == 0 {
    return fmt.Errorf("WriteToStorage: failed to create parent path for `%s`", path)
  }
  f := C.Storage_OpenWriteFile(storageAPI, cPath, 0)
  if f == nil {
    return fmt.Errorf("WriteToStorage: failed to create file at `%s`", path)
  }
  defer C.Storage_CloseWrite(storageAPI, f)
  if len(data) > 0 {
    if C.Storage_Write(storageAPI, f, 0, (C.uint64_t)(len(data)), (unsafe.Pointer(&data[0]))) == 0 {
      return fmt.Errorf("WriteToStorage: failed to write %d bytes to file `%s`", len(data), path)
    }
  }
  return nil
}
*/
// CreateMeowHashAPI ...
func CreateMeowHashAPI() *C.struct_HashAPI {
  return C.CreateMeowHashAPI()
}

// DestroyHashAPI ...
func DestroyHashAPI(api *C.struct_HashAPI) {
  C.DestroyHashAPI(api)
}

// CreateFSStorageAPI ...
func CreateFSStorageAPI() *C.struct_StorageAPI {
  return C.CreateFSStorageAPI()
}

// CreateInMemStorageAPI ...
func CreateInMemStorageAPI() *C.struct_StorageAPI {
  return C.CreateInMemStorageAPI()
}

// DestroyStorageAPI ...
func DestroyStorageAPI(api *C.struct_StorageAPI) {
  C.DestroyStorageAPI(api)
}

// CreateLizardCompressionAPI ...
func CreateLizardCompressionAPI() *C.struct_CompressionAPI {
  return C.CreateLizardCompressionAPI()
}

// DestroyCompressionAPI ...
func DestroyCompressionAPI(api *C.struct_CompressionAPI) {
  C.DestroyCompressionAPI(api)
}

// CreateBikeshedJobAPI ...
func CreateBikeshedJobAPI(workerCount uint32) *C.struct_JobAPI {
  return C.CreateBikeshedJobAPI(C.uint32_t(workerCount))
}

// DestroyJobAPI ...
func DestroyJobAPI(api *C.struct_JobAPI) {
  C.DestroyJobAPI(api)
}

// CreateDefaultCompressionRegistry ...
func CreateDefaultCompressionRegistry() *C.struct_CompressionRegistry {
  return C.CreateDefaultCompressionRegistry()
}

// DestroyCompressionRegistry ...
func DestroyCompressionRegistry(registry *C.struct_CompressionRegistry) {
  C.DestroyCompressionRegistry(registry)
}

// GetNoCompressionType ...
func GetNoCompressionType() uint32 {
  return uint32(C.NO_COMPRESSION_TYPE)
}

// GetLizardDefaultCompressionType ...
func GetLizardDefaultCompressionType() uint32 {
  return uint32(C.LIZARD_DEFAULT_COMPRESSION_TYPE)
}

// LongtailAlloc ...
func LongtailAlloc(size uint64) unsafe.Pointer {
  return C.Longtail_Alloc(C.size_t(size))
}

// LongtailFree ...
func LongtailFree(data unsafe.Pointer) {
  C.Longtail_Free(data)
}

// GetFilesRecursively ...
func GetFilesRecursively(storageAPI *C.struct_StorageAPI, rootPath string) *C.struct_FileInfos {
  cFolderPath := C.CString(rootPath)
  defer C.free(unsafe.Pointer(cFolderPath))
  return C.GetFilesRecursively(storageAPI, cFolderPath)
}

// GetPath ...
func GetPath(paths *C.struct_Paths, index uint32) string {
  cPath := C.GetPath(paths.m_Offsets, paths.m_Data, C.uint32_t(index))
  return C.GoString(cPath)
}

// GetVersionIndexPath ...
func GetVersionIndexPath(vindex *C.struct_VersionIndex, index uint32) string {
  cPath := C.GetPath(vindex.m_NameOffsets, vindex.m_NameData, C.uint32_t(index))
  return C.GoString(cPath)
}

// CreateVersionIndex ...
func CreateVersionIndex(
  storageAPI *C.struct_StorageAPI,
  hashAPI *C.struct_HashAPI,
  jobAPI *C.struct_JobAPI,
  progressFunc progressFunc,
  progressContext interface{},
  versionPath string,
  compressionType uint32,
  targetChunkSize uint32) (*C.struct_VersionIndex, error) {

  progressProxyData := makeProgressProxy(progressFunc, progressContext)
  cProgressProxyData := pointer.Save(&progressProxyData)
  defer pointer.Unref(cProgressProxyData)

  cVersionPath := C.CString(versionPath)
  defer C.free(unsafe.Pointer(cVersionPath))

  fileInfos := C.GetFilesRecursively(storageAPI, cVersionPath)
  defer C.Longtail_Free(unsafe.Pointer(fileInfos))

  compressionTypes := make([]C.uint32_t, int(*fileInfos.m_Paths.m_PathCount))
  for i := 1; i < int(*fileInfos.m_Paths.m_PathCount); i++ {
    compressionTypes[i] = C.uint32_t(compressionType)
  }

  cCompressionTypes := unsafe.Pointer(nil)
  if len(compressionTypes) > 0 {
    cCompressionTypes = unsafe.Pointer(&compressionTypes[0])
  }

  vindex := C.CreateVersionIndex(
    storageAPI,
    hashAPI,
    jobAPI,
    (C.JobAPI_ProgressFunc)(C.progressProxy),
    cProgressProxyData,
    cVersionPath,
    (*C.struct_Paths)(&fileInfos.m_Paths),
    fileInfos.m_FileSizes,
    (*C.uint32_t)(cCompressionTypes),
    C.uint32_t(targetChunkSize))

  if vindex == nil {
    return nil, fmt.Errorf("CreateVersionIndex: failed to create version index")
  }

  return vindex, nil
}

// WriteVersionIndex ...
func WriteVersionIndex(storageAPI *C.struct_StorageAPI, index *C.struct_VersionIndex, path string) error {
  cPath := C.CString(path)
  defer C.free(unsafe.Pointer(cPath))
  if C.WriteVersionIndex(storageAPI, index, cPath) == 0 {
    return fmt.Errorf("WriteVersionIndex: failed to write index to `%s`", path)
  }
  return nil
}

// ReadVersionIndex ...
func ReadVersionIndex(storageAPI *C.struct_StorageAPI, path string) *C.struct_VersionIndex {
  cPath := C.CString(path)
  defer C.free(unsafe.Pointer(cPath))
  return C.ReadVersionIndex(storageAPI, cPath)
}

// CreateContentIndex ...
func CreateContentIndex(
  hashAPI *C.struct_HashAPI,
  chunkCount uint64,
  chunkHashes []uint64,
  chunkSizes []uint32,
  compressionTypes []uint32,
  maxBlockSize uint32,
  maxChunksPerBlock uint32) (*C.struct_ContentIndex, error) {

  if chunkCount == 0 {
    return C.CreateContentIndex(
      hashAPI,
      0,
      nil,
      nil,
      nil,
      C.uint32_t(maxBlockSize),
      C.uint32_t(maxChunksPerBlock)), nil
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
    return nil, fmt.Errorf("CreateContentIndex: failed to create content index")
  }

  return cindex, nil
}

// WriteContentIndex ...
func WriteContentIndex(storageAPI *C.struct_StorageAPI, index *C.struct_ContentIndex, path string) error {
  cPath := C.CString(path)
  defer C.free(unsafe.Pointer(cPath))
  if C.WriteContentIndex(storageAPI, index, cPath) == 0 {
    return fmt.Errorf("WriteContentIndex: failed to write index to `%s`", path)
  }
  return nil
}

// ReadContentIndex ...
func ReadContentIndex(storageAPI *C.struct_StorageAPI, path string) *C.struct_ContentIndex {
  cPath := C.CString(path)
  defer C.free(unsafe.Pointer(cPath))
  return C.ReadContentIndex(storageAPI, cPath)
}

// WriteContent ...
func WriteContent(
  sourceStorageAPI *C.struct_StorageAPI,
  targetStorageAPI *C.struct_StorageAPI,
  compressionRegistry *C.struct_CompressionRegistry,
  jobAPI *C.struct_JobAPI,
  progressFunc progressFunc,
  progressContext interface{},
  contentIndex *C.struct_ContentIndex,
  versionIndex *C.struct_VersionIndex,
  versionFolderPath string,
  contentFolderPath string) error {

  progressProxyData := makeProgressProxy(progressFunc, progressContext)
  cProgressProxyData := pointer.Save(&progressProxyData)
  defer pointer.Unref(cProgressProxyData)

  cVersionFolderPath := C.CString(versionFolderPath)
  defer C.free(unsafe.Pointer(cVersionFolderPath))

  cContentFolderPath := C.CString(contentFolderPath)
  defer C.free(unsafe.Pointer(cContentFolderPath))

  ok := C.WriteContent(
    sourceStorageAPI,
    targetStorageAPI,
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

// ReadContent ...
func ReadContent(
  sourceStorageAPI *C.struct_StorageAPI,
  hashAPI *C.struct_HashAPI,
  jobAPI *C.struct_JobAPI,
  progressFunc progressFunc,
  progressContext interface{},
  contentFolderPath string) (*C.struct_ContentIndex, error) {

  progressProxyData := makeProgressProxy(progressFunc, progressContext)
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
    return nil, fmt.Errorf("ReadContent: failed to read content from `%s`", contentFolderPath)
  }
  return contentIndex, nil
}

// CreateMissingContent ...
func CreateMissingContent(
  hashAPI *C.struct_HashAPI,
  contentIndex *C.struct_ContentIndex,
  versionIndex *C.struct_VersionIndex,
  maxBlockSize uint32,
  maxChunksPerBlock uint32) (*C.struct_ContentIndex, error) {

  missingContentIndex := C.CreateMissingContent(
    hashAPI,
    contentIndex,
    versionIndex,
    C.uint32_t(maxBlockSize),
    C.uint32_t(maxChunksPerBlock))
  if missingContentIndex == nil {
    return nil, fmt.Errorf("CreateMissingContent: failed to make missing content")
  }
  return missingContentIndex, nil
}

// RetargetContent ...
func RetargetContent(
  referenceContentIndex *C.struct_ContentIndex,
  contentIndex *C.struct_ContentIndex) (*C.struct_ContentIndex, error) {
  retargetedContentIndex := C.RetargetContent(referenceContentIndex, contentIndex)
  if retargetedContentIndex == nil {
    return nil, fmt.Errorf("RetargetContent: failed to make retargeted content")
  }
  return retargetedContentIndex, nil
}

// MergeContentIndex ...
func MergeContentIndex(
  localContentIndex *C.struct_ContentIndex,
  remoteContentIndex *C.struct_ContentIndex) (*C.struct_ContentIndex, error) {
  mergedContentIndex := C.MergeContentIndex(localContentIndex, remoteContentIndex)
  if mergedContentIndex == nil {
    return nil, fmt.Errorf("MergeContentIndex: failed to merge content indexes")
  }
  return mergedContentIndex, nil
}

// WriteVersion ...
func WriteVersion(
  contentStorageAPI *C.struct_StorageAPI,
  versionStorageAPI *C.struct_StorageAPI,
  compressionRegistry *C.struct_CompressionRegistry,
  jobAPI *C.struct_JobAPI,
  progressFunc progressFunc,
  progressContext interface{},
  contentIndex *C.struct_ContentIndex,
  versionIndex *C.struct_VersionIndex,
  contentFolderPath string,
  versionFolderPath string) error {

  progressProxyData := makeProgressProxy(progressFunc, progressContext)
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
  targetVersionIndex *C.struct_VersionIndex) (*C.struct_VersionDiff, error) {
  versionDiff := C.CreateVersionDiff(sourceVersionIndex, targetVersionIndex)
  if versionDiff == nil {
    return nil, fmt.Errorf("CreateVersionDiff: failed to diff versions")
  }
  return versionDiff, nil
}

//ChangeVersion ...
func ChangeVersion(
  contentStorageAPI *C.struct_StorageAPI,
  versionStorageAPI *C.struct_StorageAPI,
  hashAPI *C.struct_HashAPI,
  jobAPI *C.struct_JobAPI,
  progressFunc progressFunc,
  progressContext interface{},
  compressionRegistry *C.struct_CompressionRegistry,
  contentIndex *C.struct_ContentIndex,
  sourceVersionIndex *C.struct_VersionIndex,
  targetVersionIndex *C.struct_VersionIndex,
  versionDiff *C.struct_VersionDiff,
  contentFolderPath string,
  versionFolderPath string) error {

  progressProxyData := makeProgressProxy(progressFunc, progressContext)
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

//CreateVersionIndexFromFolder ...
func CreateVersionIndexFromFolder(storageAPI *C.struct_StorageAPI, folderPath string, progressProxyData progressProxyData) *C.struct_VersionIndex {
  progressContext := pointer.Save(&progressProxyData)
  defer pointer.Unref(progressContext)

  cFolderPath := C.CString(folderPath)
  defer C.free(unsafe.Pointer(cFolderPath))

  hs := C.CreateMeowHashAPI()
  defer C.DestroyHashAPI(hs)

  jb := C.CreateBikeshedJobAPI(C.uint32_t(runtime.NumCPU()))
  defer C.DestroyJobAPI(jb)

  fileInfos := C.GetFilesRecursively(storageAPI, cFolderPath)
  defer C.Longtail_Free(unsafe.Pointer(fileInfos))

  compressionTypes := make([]C.uint32_t, int(*fileInfos.m_Paths.m_PathCount))
  for i := 0; i < int(*fileInfos.m_Paths.m_PathCount); i++ {
    compressionTypes[i] = C.LIZARD_DEFAULT_COMPRESSION_TYPE
  }

  vindex := C.CreateVersionIndex(
    storageAPI,
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

//GetMissingContent ...
func GetMissingContent(
  contentStorageAPI *C.struct_StorageAPI,
  versionStorageAPI *C.struct_StorageAPI,
  hashAPI *C.struct_HashAPI,
  jobAPI *C.struct_JobAPI,
  progressFunc progressFunc,
  progressContext interface{},
  versionPath string,
  versionIndexPath string,
  contentPath string,
  contentIndexPath string,
  missingContentPath string,
  compressionType uint32,
  maxChunksPerBlock uint32,
  targetBlockSize uint32,
  targetChunkSize uint32) (*C.struct_ContentIndex, error) {

  var vindex *C.struct_VersionIndex = nil
  defer C.Longtail_Free(unsafe.Pointer(vindex))
  err := error(nil)

  if len(versionIndexPath) > 0 {
    vindex = ReadVersionIndex(versionStorageAPI, versionIndexPath)
  }
  if nil == vindex {
    vindex, err = CreateVersionIndex(
      versionStorageAPI,
      hashAPI,
      jobAPI,
      progressFunc,
      progressContext,
      versionPath,
      compressionType,
      targetChunkSize)

    if err != nil {
      return nil, err
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
    cindex, err = ReadContent(
      contentStorageAPI,
      hashAPI,
      jobAPI,
      progressFunc,
      progressContext,
      contentPath)
/*
    cindex, err = CreateContentIndex(
      hashAPI,
      0,
      nil,
      nil,
      nil,
      targetBlockSize,
      maxChunksPerBlock)*/
    if err != nil {
      return nil, err
    }
  }

  missingContentIndex, err := CreateMissingContent(
    hashAPI,
    cindex,
    vindex,
    targetBlockSize,
    maxChunksPerBlock)

  if err != nil {
    return nil, err
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
    C.Longtail_Free(unsafe.Pointer(missingContentIndex))
    return nil, err
  }

  if len(versionIndexPath) > 0 {
    err = WriteVersionIndex(
      contentStorageAPI,
      vindex,
      versionIndexPath)
    if err != nil {
      C.Longtail_Free(unsafe.Pointer(missingContentIndex))
      return nil, err
    }
  }

  if len(contentIndexPath) > 0 {
    err = WriteContentIndex(
      contentStorageAPI,
      cindex,
      contentIndexPath)
    if err != nil {
      C.Longtail_Free(unsafe.Pointer(missingContentIndex))
      return nil, err
    }
  }
/*
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
*/
  return missingContentIndex, nil
}

//GetPathsForContentBlocks ...
func GetPathsForContentBlocks(contentIndex *C.struct_ContentIndex) *C.struct_Paths {
  return C.GetPathsForContentBlocks(contentIndex)
}

//export progressProxy
func progressProxy(progress unsafe.Pointer, total C.uint32_t, done C.uint32_t) {
  progressProxyData := pointer.Restore(progress).(*progressProxyData)
  progressProxyData.progressFunc(progressProxyData.Context, int(total), int(done))
}

//export logProxy
func logProxy(context unsafe.Pointer, level C.int, log *C.char) {
  logProxyData := pointer.Restore(context).(*logProxyData)
  logProxyData.logFunc(logProxyData.Context, int(level), C.GoString(log))
}

//SetLogger ...
func SetLogger(logFunc logFunc, logContext interface{}) unsafe.Pointer {
  logProxyData := makeLogProxy(logFunc, logContext)
  cLogProxyData := pointer.Save(&logProxyData)

  C.Longtail_SetLog(C.Longtail_Log(C.logProxy), cLogProxyData)
  return cLogProxyData
}

//ClearLogger ...
func ClearLogger(logger unsafe.Pointer) {
  C.Longtail_SetLog(nil, nil)
  pointer.Unref(logger)
}

//SetLogLevel ...
func SetLogLevel(level int) {
  C.Longtail_SetLogLevel(C.int(level))
}
