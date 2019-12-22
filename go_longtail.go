package golongtail

// #cgo CFLAGS: -g -std=gnu99
// #cgo LDFLAGS: -L. -l:longtail_lib.a
// #define _GNU_SOURCE
// #include "longtail/src/longtail.h"
// #include "longtail/lib/longtail_lib.h"
// #include <stdlib.h>
// void progressProxy(void* context, uint32_t total_count, uint32_t done_count);
// void logProxy(void* context, int level, char* str);
// static Longtail_StorageAPI_HOpenFile Storage_OpenWriteFile(struct Longtail_StorageAPI* api, const char* path, uint64_t initial_size)
// {
//   Longtail_StorageAPI_HOpenFile f;
//   int err = api->OpenWriteFile(api, path, initial_size, &f);
//   if (err) {
//     return 0;
//   }
//   return f;
// }
// static int Storage_Write(struct Longtail_StorageAPI* api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, const void* input)
// {
//   return api->Write(api, f, offset, length, input);
// }
// static Longtail_StorageAPI_HOpenFile Storage_OpenReadFile(struct Longtail_StorageAPI* api, const char* path)
// {
//   Longtail_StorageAPI_HOpenFile f;
//   int err = api->OpenReadFile(api, path, &f);
//   if (err) {
//     return 0;
//   }
//   return f;
// }
// static int Storage_Read(struct Longtail_StorageAPI* api, Longtail_StorageAPI_HOpenFile f, uint64_t offset, uint64_t length, void* output)
// {
//   return api->Read(api, f, offset, length, output);
// }
// static void Storage_CloseFile(struct Longtail_StorageAPI* api, Longtail_StorageAPI_HOpenFile f)
// {
//   return api->CloseFile(api, f);
// }
// static uint64_t Storage_GetSize(struct Longtail_StorageAPI* api, Longtail_StorageAPI_HOpenFile f)
// {
//   uint64_t s;
//   int err = api->GetSize(api, f, &s);
//   if (err) {
//     return 0;
//   }
//   return s;
// }
// static char* Storage_ConcatPath(struct Longtail_StorageAPI* api, const char* root_path, const char* sub_path)
// {
//   return api->ConcatPath(api, root_path, sub_path);
// }
// static const char* GetPath(const uint32_t* name_offsets, const char* name_data, uint32_t index)
// {
//   return &name_data[name_offsets[index]];
// }
// static void* ReadFromStorage(struct Longtail_StorageAPI* api, const char* rootPath, const char* blockPath)
// {
//    char* full_path = api->ConcatPath(api, rootPath, blockPath);
//    Longtail_StorageAPI_HOpenFile block_file;
//    int err = api->OpenReadFile(api, full_path, &block_file);
//    if (err) {
//      Longtail_Free(full_path);
//      return 0;
//    }
//    uint64_t blockSize;
//    err = api->GetSize(api, block_file, &blockSize);
//    if (err) {
//      api->CloseFile(api, block_file);
//      Longtail_Free(full_path);
//      return 0;
//    }
//    void* buffer = Longtail_Alloc((size_t)blockSize);
//    api->Read(api, block_file, 0, blockSize, buffer);
//    api->CloseFile(api, block_file);
//    Longtail_Free(full_path);
//    return buffer;
// }
// static int WriteToStorage(struct Longtail_StorageAPI* api, const char* rootPath, const char* blockPath, uint64_t blockSize, void* blockData)
// {
//    char* full_path = api->ConcatPath(api, rootPath, blockPath);
//    Longtail_StorageAPI_HOpenFile block_file;
//    int err = api->OpenWriteFile(api, full_path, blockSize, &block_file);
//    if (err) {
//      Longtail_Free(full_path);
//      return 0;
//    }
//    api->Write(api, block_file, 0, blockSize, blockData);
//    api->CloseFile(api, block_file);
//    Longtail_Free(full_path);
//    return 1;
// }
import "C"
import (
  "path/filepath"
  "fmt"
  "os"
  "unsafe"

  "github.com/mattn/go-pointer"
)

// ReadFromStorage ...
func ReadFromStorage(api *C.struct_Longtail_StorageAPI, rootPath string, path string) ([]byte, error) {
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
	C.Storage_CloseFile(api, f)
	return blockData, nil
}

// WriteToStorage ...
func WriteToStorage(api *C.struct_Longtail_StorageAPI, rootPath string, path string, blockData []byte) error {
	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cFullPath := C.Storage_ConcatPath(api, cRootPath, cPath)
	defer C.free(unsafe.Pointer(cFullPath))

  err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
  if err != nil {
    return err
  }

	blockSize := C.uint64_t(len(blockData))

  f := C.Storage_OpenWriteFile(api, cFullPath, blockSize)
  if blockSize > 0 {
    C.Storage_Write(api, f, 0, blockSize, unsafe.Pointer(&blockData[0]))
  }
	C.Storage_CloseFile(api, f)
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
func WriteToStorage(storageAPI *C.struct_Longtail_StorageAPI, path string, data []byte) error {
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
func CreateMeowHashAPI() *C.struct_Longtail_HashAPI {
  return C.Longtail_CreateMeowHashAPI()
}

// DestroyHashAPI ...
func DestroyHashAPI(api *C.struct_Longtail_HashAPI) {
  C.Longtail_DestroyHashAPI(api)
}

// CreateFSStorageAPI ...
func CreateFSStorageAPI() *C.struct_Longtail_StorageAPI {
  return C.Longtail_CreateFSStorageAPI()
}

// CreateInMemStorageAPI ...
func CreateInMemStorageAPI() *C.struct_Longtail_StorageAPI {
  return C.Longtail_CreateInMemStorageAPI()
}

// DestroyStorageAPI ...
func DestroyStorageAPI(api *C.struct_Longtail_StorageAPI) {
  C.Longtail_DestroyStorageAPI(api)
}

// CreateLizardCompressionAPI ...
func CreateLizardCompressionAPI() *C.struct_Longtail_CompressionAPI {
  return C.Longtail_CreateLizardCompressionAPI()
}

// DestroyCompressionAPI ...
func DestroyCompressionAPI(api *C.struct_Longtail_CompressionAPI) {
  C.Longtail_DestroyCompressionAPI(api)
}

// CreateBikeshedJobAPI ...
func CreateBikeshedJobAPI(workerCount uint32) *C.struct_Longtail_JobAPI {
  return C.Longtail_CreateBikeshedJobAPI(C.uint32_t(workerCount))
}

// DestroyJobAPI ...
func DestroyJobAPI(api *C.struct_Longtail_JobAPI) {
  C.Longtail_DestroyJobAPI(api)
}

// Longtail_CreateDefaultCompressionRegistry ...
func CreateDefaultCompressionRegistry() *C.struct_Longtail_CompressionRegistry {
  return C.Longtail_CreateDefaultCompressionRegistry()
}

// DestroyCompressionRegistry ...
func DestroyCompressionRegistry(registry *C.struct_Longtail_CompressionRegistry) {
  C.Longtail_DestroyCompressionRegistry(registry)
}

// GetNoCompressionType ...
func GetNoCompressionType() uint32 {
  return uint32(C.LONGTAIL_NO_COMPRESSION_TYPE)
}

// GetLizardDefaultCompressionType ...
func GetLizardDefaultCompressionType() uint32 {
  return uint32(C.LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE)
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
func GetFilesRecursively(storageAPI *C.struct_Longtail_StorageAPI, rootPath string) (*C.struct_Longtail_FileInfos, error) {
  cFolderPath := C.CString(rootPath)
  defer C.free(unsafe.Pointer(cFolderPath))
  var fileInfos *C.struct_Longtail_FileInfos
  errno := C.Longtail_GetFilesRecursively(storageAPI, cFolderPath, &fileInfos)
  if errno != 0 {
    return nil, fmt.Errorf("GetFilesRecursively: failed with error %d", errno)
  }
  return fileInfos, nil
}

// GetPath ...
func GetPath(paths *C.struct_Longtail_Paths, index uint32) string {
  cPath := C.GetPath(paths.m_Offsets, paths.m_Data, C.uint32_t(index))
  return C.GoString(cPath)
}

// GetVersionIndexPath ...
func GetVersionIndexPath(vindex *C.struct_Longtail_VersionIndex, index uint32) string {
  cPath := C.GetPath(vindex.m_NameOffsets, vindex.m_NameData, C.uint32_t(index))
  return C.GoString(cPath)
}

// CreateVersionIndex ...
func CreateVersionIndex(
  storageAPI *C.struct_Longtail_StorageAPI,
  hashAPI *C.struct_Longtail_HashAPI,
  jobAPI *C.struct_Longtail_JobAPI,
  progressFunc progressFunc,
  progressContext interface{},
  versionPath string,
  compressionType uint32,
  targetChunkSize uint32) (*C.struct_Longtail_VersionIndex, error) {

  progressProxyData := makeProgressProxy(progressFunc, progressContext)
  cProgressProxyData := pointer.Save(&progressProxyData)
  defer pointer.Unref(cProgressProxyData)

  cVersionPath := C.CString(versionPath)
  defer C.free(unsafe.Pointer(cVersionPath))

  var fileInfos *C.struct_Longtail_FileInfos
  errno := C.Longtail_GetFilesRecursively(storageAPI, cVersionPath, &fileInfos)
  if errno != 0 {
    return nil, fmt.Errorf("GetFilesRecursively: scan folder `%s` failed with error %d", versionPath, errno)
  }
  defer C.Longtail_Free(unsafe.Pointer(fileInfos))

  compressionTypes := make([]C.uint32_t, int(*fileInfos.m_Paths.m_PathCount))
  for i := 1; i < int(*fileInfos.m_Paths.m_PathCount); i++ {
    compressionTypes[i] = C.uint32_t(compressionType)
  }

  cCompressionTypes := unsafe.Pointer(nil)
  if len(compressionTypes) > 0 {
    cCompressionTypes = unsafe.Pointer(&compressionTypes[0])
  }

  var vindex *C.struct_Longtail_VersionIndex
  errno = C.Longtail_CreateVersionIndex(
    storageAPI,
    hashAPI,
    jobAPI,
    (C.Longtail_JobAPI_ProgressFunc)(C.progressProxy),
    cProgressProxyData,
    cVersionPath,
    (*C.struct_Longtail_Paths)(&fileInfos.m_Paths),
    fileInfos.m_FileSizes,
    (*C.uint32_t)(cCompressionTypes),
    C.uint32_t(targetChunkSize),
    &vindex)

  if errno != 0 {
    return nil, fmt.Errorf("CreateVersionIndex: failed with error %d", errno)
  }

  return vindex, nil
}

// WriteVersionIndex ...
func WriteVersionIndex(storageAPI *C.struct_Longtail_StorageAPI, index *C.struct_Longtail_VersionIndex, path string) error {
  cPath := C.CString(path)
  defer C.free(unsafe.Pointer(cPath))
  errno := C.Longtail_WriteVersionIndex(storageAPI, index, cPath)
  if errno != 0 {
    return fmt.Errorf("WriteVersionIndex: write index to `%s` failed with error %d", path, errno)
  }
  return nil
}

// ReadVersionIndex ...
func ReadVersionIndex(storageAPI *C.struct_Longtail_StorageAPI, path string) (*C.struct_Longtail_VersionIndex, error) {
  cPath := C.CString(path)
  defer C.free(unsafe.Pointer(cPath))
  var vindex *C.struct_Longtail_VersionIndex
  errno := C.Longtail_ReadVersionIndex(storageAPI, cPath, &vindex)
  if errno != 0 {
    return nil, fmt.Errorf("ReadVersionIndex: read index from `%s` failed with error %d", path, errno)
  }
  return vindex, nil
}

// CreateContentIndex ...
func CreateContentIndex(
  hashAPI *C.struct_Longtail_HashAPI,
  chunkCount uint64,
  chunkHashes []uint64,
  chunkSizes []uint32,
  compressionTypes []uint32,
  maxBlockSize uint32,
  maxChunksPerBlock uint32) (*C.struct_Longtail_ContentIndex, error) {

  var cindex *C.struct_Longtail_ContentIndex
  if chunkCount == 0 {
    errno := C.Longtail_CreateContentIndex(
      hashAPI,
      0,
      nil,
      nil,
      nil,
      C.uint32_t(maxBlockSize),
      C.uint32_t(maxChunksPerBlock),
      &cindex)
    if errno != 0 {
      return nil, fmt.Errorf("CreateContentIndex: create empty content index failed with error %d", errno)
    }
  }
  cChunkHashes := (*C.TLongtail_Hash)(unsafe.Pointer(&chunkHashes[0]))
  cChunkSizes := (*C.uint32_t)(unsafe.Pointer(&chunkSizes[0]))
  cCompressionTypes := (*C.uint32_t)(unsafe.Pointer(&compressionTypes[0]))

  errno := C.Longtail_CreateContentIndex(
    hashAPI,
    C.uint64_t(chunkCount),
    cChunkHashes,
    cChunkSizes,
    cCompressionTypes,
    C.uint32_t(maxBlockSize),
    C.uint32_t(maxChunksPerBlock),
    &cindex)

  if errno != 0 {
    return nil, fmt.Errorf("CreateContentIndex: create content index failed with error %d", errno)
  }

  return cindex, nil
}

// WriteContentIndex ...
func WriteContentIndex(storageAPI *C.struct_Longtail_StorageAPI, index *C.struct_Longtail_ContentIndex, path string) error {
  cPath := C.CString(path)
  defer C.free(unsafe.Pointer(cPath))
  errno := C.Longtail_WriteContentIndex(storageAPI, index, cPath) 
  if errno != 0 {
    return fmt.Errorf("WriteContentIndex: write index to `%s` failed with error %d", path, errno)
  }
  return nil
}

// ReadContentIndex ...
func ReadContentIndex(storageAPI *C.struct_Longtail_StorageAPI, path string) (*C.struct_Longtail_ContentIndex, error) {
  cPath := C.CString(path)
  defer C.free(unsafe.Pointer(cPath))
  var cindex *C.struct_Longtail_ContentIndex
  errno := C.Longtail_ReadContentIndex(storageAPI, cPath, &cindex)
  if errno != 0 {
    return nil, fmt.Errorf("ReadContentIndex: read index from `%s` failed with error %d", path, errno)
  }
  return cindex, nil
}

// WriteContent ...
func WriteContent(
  sourceStorageAPI *C.struct_Longtail_StorageAPI,
  targetStorageAPI *C.struct_Longtail_StorageAPI,
  compressionRegistry *C.struct_Longtail_CompressionRegistry,
  jobAPI *C.struct_Longtail_JobAPI,
  progressFunc progressFunc,
  progressContext interface{},
  contentIndex *C.struct_Longtail_ContentIndex,
  versionIndex *C.struct_Longtail_VersionIndex,
  versionFolderPath string,
  contentFolderPath string) error {

  progressProxyData := makeProgressProxy(progressFunc, progressContext)
  cProgressProxyData := pointer.Save(&progressProxyData)
  defer pointer.Unref(cProgressProxyData)

  cVersionFolderPath := C.CString(versionFolderPath)
  defer C.free(unsafe.Pointer(cVersionFolderPath))

  cContentFolderPath := C.CString(contentFolderPath)
  defer C.free(unsafe.Pointer(cContentFolderPath))

  errno := C.Longtail_WriteContent(
    sourceStorageAPI,
    targetStorageAPI,
    compressionRegistry,
    jobAPI,
    (C.Longtail_JobAPI_ProgressFunc)(C.progressProxy),
    cProgressProxyData,
    contentIndex,
    versionIndex,
    cVersionFolderPath,
    cContentFolderPath)
  if errno != 0 {
    return fmt.Errorf("WriteContent: write content to `%s` failed with error %d", contentFolderPath, errno)
  }
  return nil
}

// ReadContent ...
func ReadContent(
  sourceStorageAPI *C.struct_Longtail_StorageAPI,
  hashAPI *C.struct_Longtail_HashAPI,
  jobAPI *C.struct_Longtail_JobAPI,
  progressFunc progressFunc,
  progressContext interface{},
  contentFolderPath string) (*C.struct_Longtail_ContentIndex, error) {

  progressProxyData := makeProgressProxy(progressFunc, progressContext)
  cProgressProxyData := pointer.Save(&progressProxyData)
  defer pointer.Unref(cProgressProxyData)

  cContentFolderPath := C.CString(contentFolderPath)
  defer C.free(unsafe.Pointer(cContentFolderPath))

  var contentIndex *C.struct_Longtail_ContentIndex
  errno := C.Longtail_ReadContent(
    sourceStorageAPI,
    hashAPI,
    jobAPI,
    (C.Longtail_JobAPI_ProgressFunc)(C.progressProxy),
    cProgressProxyData,
    cContentFolderPath,
    &contentIndex)
  if errno != 0 {
    return nil, fmt.Errorf("ReadContent: read content from `%s` failed with error %d", contentFolderPath, errno)
  }
  return contentIndex, nil
}

// CreateMissingContent ...
func CreateMissingContent(
  hashAPI *C.struct_Longtail_HashAPI,
  contentIndex *C.struct_Longtail_ContentIndex,
  versionIndex *C.struct_Longtail_VersionIndex,
  maxBlockSize uint32,
  maxChunksPerBlock uint32) (*C.struct_Longtail_ContentIndex, error) {

  var missingContentIndex *C.struct_Longtail_ContentIndex
  errno := C.Longtail_CreateMissingContent(
    hashAPI,
    contentIndex,
    versionIndex,
    C.uint32_t(maxBlockSize),
    C.uint32_t(maxChunksPerBlock),
    &missingContentIndex)
  if errno != 0 {
    return nil, fmt.Errorf("CreateMissingContent: make missing content failed with error %d", errno)
  }
  return missingContentIndex, nil
}

// RetargetContent ...
func RetargetContent(
  referenceContentIndex *C.struct_Longtail_ContentIndex,
  contentIndex *C.struct_Longtail_ContentIndex) (*C.struct_Longtail_ContentIndex, error) {
  var retargetedContentIndex *C.struct_Longtail_ContentIndex
  errno := C.Longtail_RetargetContent(referenceContentIndex, contentIndex, &retargetedContentIndex)
  if errno != 0 {
    return nil, fmt.Errorf("RetargetContent: retarget content failed with error %d", errno)
  }
  return retargetedContentIndex, nil
}

// MergeContentIndex ...
func MergeContentIndex(
  localContentIndex *C.struct_Longtail_ContentIndex,
  remoteContentIndex *C.struct_Longtail_ContentIndex) (*C.struct_Longtail_ContentIndex, error) {
  var mergedContentIndex *C.struct_Longtail_ContentIndex
  errno := C.Longtail_MergeContentIndex(localContentIndex, remoteContentIndex, &mergedContentIndex)
  if errno != 0 {
    return nil, fmt.Errorf("MergeContentIndex: merge content indexes failed with error %d", errno)
  }
  return mergedContentIndex, nil
}

// WriteVersion ...
func WriteVersion(
  contentStorageAPI *C.struct_Longtail_StorageAPI,
  versionStorageAPI *C.struct_Longtail_StorageAPI,
  compressionRegistry *C.struct_Longtail_CompressionRegistry,
  jobAPI *C.struct_Longtail_JobAPI,
  progressFunc progressFunc,
  progressContext interface{},
  contentIndex *C.struct_Longtail_ContentIndex,
  versionIndex *C.struct_Longtail_VersionIndex,
  contentFolderPath string,
  versionFolderPath string) error {

  progressProxyData := makeProgressProxy(progressFunc, progressContext)
  cProgressProxyData := pointer.Save(&progressProxyData)
  defer pointer.Unref(cProgressProxyData)

  cContentFolderPath := C.CString(contentFolderPath)
  defer C.free(unsafe.Pointer(cContentFolderPath))

  cVersionFolderPath := C.CString(versionFolderPath)
  defer C.free(unsafe.Pointer(cVersionFolderPath))

  errno := C.Longtail_WriteVersion(
    contentStorageAPI,
    versionStorageAPI,
    compressionRegistry,
    jobAPI,
    (C.Longtail_JobAPI_ProgressFunc)(C.progressProxy),
    cProgressProxyData,
    contentIndex,
    versionIndex,
    cContentFolderPath,
    cVersionFolderPath)
  if errno != 0 {
    return fmt.Errorf("WriteVersion: write version to `%s` from `%s` failed with error %d", versionFolderPath, contentFolderPath, errno)
  }
  return nil
}

//CreateVersionDiff do we really need this? Maybe ChangeVersion should create one on the fly?
func CreateVersionDiff(
  sourceVersionIndex *C.struct_Longtail_VersionIndex,
  targetVersionIndex *C.struct_Longtail_VersionIndex) (*C.struct_Longtail_VersionDiff, error) {
  var versionDiff *C.struct_Longtail_VersionDiff
  errno := C.Longtail_CreateVersionDiff(sourceVersionIndex, targetVersionIndex, &versionDiff)
  if errno != 0 {
    return nil, fmt.Errorf("CreateVersionDiff: diff versions failed with error %d", errno)
  }
  return versionDiff, nil
}

//ChangeVersion ...
func ChangeVersion(
  contentStorageAPI *C.struct_Longtail_StorageAPI,
  versionStorageAPI *C.struct_Longtail_StorageAPI,
  hashAPI *C.struct_Longtail_HashAPI,
  jobAPI *C.struct_Longtail_JobAPI,
  progressFunc progressFunc,
  progressContext interface{},
  compressionRegistry *C.struct_Longtail_CompressionRegistry,
  contentIndex *C.struct_Longtail_ContentIndex,
  sourceVersionIndex *C.struct_Longtail_VersionIndex,
  targetVersionIndex *C.struct_Longtail_VersionIndex,
  versionDiff *C.struct_Longtail_VersionDiff,
  contentFolderPath string,
  versionFolderPath string) error {

  progressProxyData := makeProgressProxy(progressFunc, progressContext)
  cProgressProxyData := pointer.Save(&progressProxyData)
  defer pointer.Unref(cProgressProxyData)

  cContentFolderPath := C.CString(contentFolderPath)
  defer C.free(unsafe.Pointer(cContentFolderPath))

  cVersionFolderPath := C.CString(versionFolderPath)
  defer C.free(unsafe.Pointer(cVersionFolderPath))

  errno := C.Longtail_ChangeVersion(
    contentStorageAPI,
    versionStorageAPI,
    hashAPI,
    jobAPI,
    (C.Longtail_JobAPI_ProgressFunc)(C.progressProxy),
    cProgressProxyData,
    compressionRegistry,
    contentIndex,
    sourceVersionIndex,
    targetVersionIndex,
    versionDiff,
    cContentFolderPath,
    cVersionFolderPath)
  if errno != 0 {
    return fmt.Errorf("ChangeVersion: update version `%s` from `%s` failed with error %d", versionFolderPath, contentFolderPath, errno)
  }
  return nil
}

//GetMissingContent ... this is handy, but not what we should expose other than for tests!
func GetMissingContent(
  contentStorageAPI *C.struct_Longtail_StorageAPI,
  versionStorageAPI *C.struct_Longtail_StorageAPI,
  hashAPI *C.struct_Longtail_HashAPI,
  jobAPI *C.struct_Longtail_JobAPI,
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
  targetChunkSize uint32) (*C.struct_Longtail_ContentIndex, error) {

  var vindex *C.struct_Longtail_VersionIndex = nil
  defer C.Longtail_Free(unsafe.Pointer(vindex))
  err := error(nil)

  if len(versionIndexPath) > 0 {
    vindex, _ = ReadVersionIndex(versionStorageAPI, versionIndexPath)
  }
  if err != nil {
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

  var cindex *C.struct_Longtail_ContentIndex = nil
  defer C.Longtail_Free(unsafe.Pointer(cindex))

  cContentIndexPath := C.CString(contentIndexPath)
  defer C.free(unsafe.Pointer(cContentIndexPath))

  if len(contentIndexPath) > 0 {
    cindex, _ = ReadContentIndex(contentStorageAPI, contentIndexPath)
  }
  if err != nil {
    cindex, err = ReadContent(
      contentStorageAPI,
      hashAPI,
      jobAPI,
      progressFunc,
      progressContext,
      contentPath)
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

  compressionRegistry := C.Longtail_CreateDefaultCompressionRegistry()
  defer C.Longtail_DestroyCompressionRegistry(compressionRegistry)

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
  return missingContentIndex, nil
}

//GetPathsForContentBlocks ...
func GetPathsForContentBlocks(contentIndex *C.struct_Longtail_ContentIndex) (*C.struct_Longtail_Paths, error) {
  var paths *C.struct_Longtail_Paths
  errno := C.Longtail_GetPathsForContentBlocks(contentIndex, &paths)
  if errno != 0 {
    return nil, fmt.Errorf("GetPathsForContentBlocks: get paths failed with error %d", errno)
  }
  return paths, nil
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
