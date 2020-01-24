package golongtail

// #cgo CFLAGS: -g -std=gnu99
// #cgo LDFLAGS: -L. -lm
// #include "golongtail.h"
import "C"
import (
	"fmt"
	"reflect"
	"sync/atomic"
	"unsafe"
)

type Longtail_Paths struct {
	cPaths *C.struct_Longtail_Paths
}

type Longtail_FileInfos struct {
	cFileInfos *C.struct_Longtail_FileInfos
}

type Longtail_ContentIndex struct {
	cContentIndex *C.struct_Longtail_ContentIndex
}

type Longtail_VersionIndex struct {
	cVersionIndex *C.struct_Longtail_VersionIndex
}

type Longtail_VersionDiff struct {
	cVersionDiff *C.struct_Longtail_VersionDiff
}

type Longtail_JobAPI struct {
	cJobAPI *C.struct_Longtail_JobAPI
}

type Longtail_CompressionRegistryAPI struct {
	cCompressionRegistryAPI *C.struct_Longtail_CompressionRegistryAPI
}

type Longtail_CompressionAPI struct {
	cCompressionAPI *C.struct_Longtail_CompressionAPI
}

type Longtail_StorageAPI struct {
	cStorageAPI *C.struct_Longtail_StorageAPI
}

type Longtail_HashAPI struct {
	cHashAPI *C.struct_Longtail_HashAPI
}

var pointerIndex uint32
var pointerStore [512]interface{}
var pointerIndexer = (*[1 << 30]C.uint32_t)(C.malloc(4 * 512))

func SavePointer(v interface{}) unsafe.Pointer {
	if v == nil {
		return nil
	}

	newPointerIndex := (atomic.AddUint32(&pointerIndex, 1)) % 512
	for pointerStore[newPointerIndex] != nil {
		newPointerIndex = (atomic.AddUint32(&pointerIndex, 1)) % 512
	}
	pointerIndexer[newPointerIndex] = C.uint32_t(newPointerIndex)
	pointerStore[newPointerIndex] = v
	return unsafe.Pointer(&pointerIndexer[newPointerIndex])
}

func RestorePointer(ptr unsafe.Pointer) (v interface{}) {
	if ptr == nil {
		return nil
	}

	p := (*C.uint32_t)(ptr)
	index := uint32(*p)

	return pointerStore[index]
}

func UnrefPointer(ptr unsafe.Pointer) {
	if ptr == nil {
		return
	}

	p := (*C.uint32_t)(ptr)
	index := uint32(*p)
	pointerStore[index] = nil
}

// ReadFromStorage ...
func ReadFromStorage(storageAPI Longtail_StorageAPI, rootPath string, path string) ([]byte, error) {
	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cFullPath := C.Storage_ConcatPath(storageAPI.cStorageAPI, cRootPath, cPath)
	defer C.Longtail_Free(unsafe.Pointer(cFullPath))

	blockSize := C.Storage_GetSize(storageAPI.cStorageAPI, cFullPath)
	blockData := make([]byte, int(blockSize))
	errno := C.int(0)
	if blockSize > 0 {
		errno = C.Storage_Read(storageAPI.cStorageAPI, cFullPath, 0, blockSize, unsafe.Pointer(&blockData[0]))
	}
	if errno != 0 {
		return nil, fmt.Errorf("ReadFromStorage: Storage_Read(%s/%s) failed with error %d", rootPath, path, errno)
	}
	return blockData, nil
}

// WriteToStorage ...
func WriteToStorage(storageAPI Longtail_StorageAPI, rootPath string, path string, blockData []byte) error {
	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cFullPath := C.Storage_ConcatPath(storageAPI.cStorageAPI, cRootPath, cPath)
	defer C.Longtail_Free(unsafe.Pointer(cFullPath))

	errno := C.EnsureParentPathExists(storageAPI.cStorageAPI, cFullPath)
	if errno != 0 {
		return fmt.Errorf("WriteToStorage: C.EnsureParentPathExists(`%s/%s`) failed with error %d", rootPath, path, errno)
	}

	blockSize := C.uint64_t(len(blockData))
	var data unsafe.Pointer
	if blockSize > 0 {
		data = unsafe.Pointer(&blockData[0])
	}
	errno = C.Storage_Write(storageAPI.cStorageAPI, cFullPath, blockSize, data)
	if errno != 0 {
		return fmt.Errorf("WriteToStorage: C.Storage_Write(`%s/%s`) failed with error %d", rootPath, path, errno)
	}
	return nil
}

type ProgressFunc func(context interface{}, total int, current int)

type progressProxyData struct {
	progressFunc ProgressFunc
	Context      interface{}
}

func makeProgressProxy(progressFunc ProgressFunc, context interface{}) progressProxyData {
	return progressProxyData{progressFunc, context}
}

type logFunc func(context interface{}, level int, log string)
type logProxyData struct {
	logFunc logFunc
	Context interface{}
}

func makeLogProxy(logFunc logFunc, context interface{}) logProxyData {
	return logProxyData{logFunc, context}
}

type assertFunc func(context interface{}, expression string, file string, line int)
type assertProxyData struct {
	assertFunc assertFunc
	Context    interface{}
}

func makeAssertProxy(assertFunc assertFunc, context interface{}) assertProxyData {
	return assertProxyData{assertFunc, context}
}

func (paths *Longtail_Paths) Dispose() {
	C.Longtail_Free(unsafe.Pointer(paths.cPaths))
}

func (paths *Longtail_Paths) GetPathCount() uint32 {
	return uint32(*paths.cPaths.m_PathCount)
}

func (fileInfos *Longtail_FileInfos) Dispose() {
	C.Longtail_Free(unsafe.Pointer(fileInfos.cFileInfos))
}

func carray2slice(array *C.uint64_t, len int) []uint64 {
	var list []uint64
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&list)))
	sliceHeader.Cap = len
	sliceHeader.Len = len
	sliceHeader.Data = uintptr(unsafe.Pointer(array))
	return list
}

func (fileInfos *Longtail_FileInfos) GetFileCount() uint32 {
	return uint32(*fileInfos.cFileInfos.m_Paths.m_PathCount)
}

func (fileInfos *Longtail_FileInfos) GetFileSizes() []uint64 {
	size := int(*fileInfos.cFileInfos.m_Paths.m_PathCount)
	return carray2slice(fileInfos.cFileInfos.m_FileSizes, size)
}

func (fileInfos *Longtail_FileInfos) GetPaths() Longtail_Paths {
	return Longtail_Paths{cPaths: &fileInfos.cFileInfos.m_Paths}
}

func (contentIndex *Longtail_ContentIndex) Dispose() {
	C.Longtail_Free(unsafe.Pointer(contentIndex.cContentIndex))
}

func (contentIndex *Longtail_ContentIndex) GetVersion() uint32 {
	return uint32(*contentIndex.cContentIndex.m_Version)
}

func (contentIndex *Longtail_ContentIndex) GetHashAPI() uint32 {
	return uint32(*contentIndex.cContentIndex.m_HashAPI)
}

func (contentIndex *Longtail_ContentIndex) GetBlockCount() uint64 {
	return uint64(*contentIndex.cContentIndex.m_BlockCount)
}

func (versionIndex *Longtail_VersionIndex) Dispose() {
	C.Longtail_Free(unsafe.Pointer(versionIndex.cVersionIndex))
}

func (versionIndex *Longtail_VersionIndex) GetVersion() uint32 {
	return uint32(*versionIndex.cVersionIndex.m_Version)
}

func (versionIndex *Longtail_VersionIndex) GetHashAPI() uint32 {
	return uint32(*versionIndex.cVersionIndex.m_HashAPI)
}

func (versionIndex *Longtail_VersionIndex) GetAssetCount() uint32 {
	return uint32(*versionIndex.cVersionIndex.m_AssetCount)
}

func (versionIndex *Longtail_VersionIndex) GetChunkCount() uint32 {
	return uint32(*versionIndex.cVersionIndex.m_ChunkCount)
}

func (versionDiff *Longtail_VersionDiff) Dispose() {
	C.Longtail_Free(unsafe.Pointer(versionDiff.cVersionDiff))
}

// CreateBlake2HashAPI ...
func CreateBlake2HashAPI() Longtail_HashAPI {
	return Longtail_HashAPI{cHashAPI: C.Longtail_CreateBlake2HashAPI()}
}

// CreateBlake3HashAPI ...
func CreateBlake3HashAPI() Longtail_HashAPI {
	return Longtail_HashAPI{cHashAPI: C.Longtail_CreateBlake3HashAPI()}
}

// CreateMeowHashAPI ...
func CreateMeowHashAPI() Longtail_HashAPI {
	return Longtail_HashAPI{cHashAPI: C.Longtail_CreateMeowHashAPI()}
}

// Longtail_HashAPI.Dispose() ...
func (hashAPI *Longtail_HashAPI) Dispose() {
	C.Longtail_DisposeAPI(&hashAPI.cHashAPI.m_API)
}

// GetBlake2HashIdentifier() ...
func GetBlake2HashIdentifier() uint32 {
	return uint32(C.GetBlake2HashIdentifier())
}

// GetBlake3HashIdentifier() ...
func GetBlake3HashIdentifier() uint32 {
	return uint32(C.GetBlake3HashIdentifier())
}

// GetMeowHashIdentifier() ...
func GetMeowHashIdentifier() uint32 {
	return uint32(C.GetMeowHashIdentifier())
}

// CreateFSStorageAPI ...
func CreateFSStorageAPI() Longtail_StorageAPI {
	return Longtail_StorageAPI{cStorageAPI: C.Longtail_CreateFSStorageAPI()}
}

// CreateInMemStorageAPI ...
func CreateInMemStorageAPI() Longtail_StorageAPI {
	return Longtail_StorageAPI{cStorageAPI: C.Longtail_CreateInMemStorageAPI()}
}

// Longtail_StorageAPI.Dispose() ...
func (storageAPI *Longtail_StorageAPI) Dispose() {
	C.Longtail_DisposeAPI(&storageAPI.cStorageAPI.m_API)
}

// CreateLizardCompressionAPI ...
func CreateLizardCompressionAPI() Longtail_CompressionAPI {
	return Longtail_CompressionAPI{cCompressionAPI: C.Longtail_CreateLizardCompressionAPI()}
}

// CreateBrotliCompressionAPI ...
func CreateBrotliCompressionAPI() Longtail_CompressionAPI {
	return Longtail_CompressionAPI{cCompressionAPI: C.Longtail_CreateBrotliCompressionAPI()}
}

// CreateZStdCompressionAPI ...
func CreateZStdCompressionAPI() Longtail_CompressionAPI {
	return Longtail_CompressionAPI{cCompressionAPI: C.Longtail_CreateZStdCompressionAPI()}
}

// Longtail_CompressionAPI.Dispose() ...
func (compressionAPI *Longtail_CompressionAPI) Dispose() {
	C.Longtail_DisposeAPI(&compressionAPI.cCompressionAPI.m_API)
}

// CreateBikeshedJobAPI ...
func CreateBikeshedJobAPI(workerCount uint32) Longtail_JobAPI {
	return Longtail_JobAPI{cJobAPI: C.Longtail_CreateBikeshedJobAPI(C.uint32_t(workerCount))}
}

// Longtail_JobAPI.Dispose() ...
func (jobAPI *Longtail_JobAPI) Dispose() {
	C.Longtail_DisposeAPI(&jobAPI.cJobAPI.m_API)
}

// Longtail_CreateDefaultCompressionRegistry ...
func CreateDefaultCompressionRegistry() Longtail_CompressionRegistryAPI {
	return Longtail_CompressionRegistryAPI{cCompressionRegistryAPI: C.CompressionRegistry_CreateDefault()}
}

// Longtail_CompressionRegistryAPI ...
func (compressionRegistry *Longtail_CompressionRegistryAPI) Dispose() {
	C.Longtail_DisposeAPI(&compressionRegistry.cCompressionRegistryAPI.m_API)
}

// GetNoCompressionType ...
func GetNoCompressionType() uint32 {
	return uint32(C.LONGTAIL_NO_COMPRESSION_TYPE)
}

// GetBrotliGenericMinCompressionType ...
func GetBrotliGenericMinCompressionType() uint32 {
	return uint32(C.LONGTAIL_BROTLI_GENERIC_MIN_QUALITY_TYPE)
}

// GetBrotliGenericDefaultCompressionType ...
func GetBrotliGenericDefaultCompressionType() uint32 {
	return uint32(C.LONGTAIL_BROTLI_GENERIC_DEFAULT_QUALITY_TYPE)
}

// GetBrotliGenericMaxCompressionType ...
func GetBrotliGenericMaxCompressionType() uint32 {
	return uint32(C.LONGTAIL_BROTLI_GENERIC_MAX_QUALITY_TYPE)
}

// GetBrotliTextMinCompressionType ...
func GetBrotliTextMinCompressionType() uint32 {
	return uint32(C.LONGTAIL_BROTLI_TEXT_MIN_QUALITY_TYPE)
}

// GetBrotliTextDefaultCompressionType ...
func GetBrotliTextDefaultCompressionType() uint32 {
	return uint32(C.LONGTAIL_BROTLI_TEXT_DEFAULT_QUALITY_TYPE)
}

// GetBrotliTextMaxCompressionType ...
func GetBrotliTextMaxCompressionType() uint32 {
	return uint32(C.LONGTAIL_BROTLI_TEXT_MAX_QUALITY_TYPE)
}

// GetLizardMinCompressionType ...
func GetLizardMinCompressionType() uint32 {
	return uint32(C.LONGTAIL_LIZARD_MIN_COMPRESSION_TYPE)
}

// GetLizardDefaultCompressionType ...
func GetLizardDefaultCompressionType() uint32 {
	return uint32(C.LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE)
}

// GetLizardMaxCompressionType ...
func GetLizardMaxCompressionType() uint32 {
	return uint32(C.LONGTAIL_LIZARD_MAX_COMPRESSION_TYPE)
}

// GetZStdMinCompressionType ...
func GetZStdMinCompressionType() uint32 {
	return uint32(C.LONGTAIL_ZSTD_MIN_COMPRESSION_TYPE)
}

// GetZStdDefaultCompressionType ...
func GetZStdDefaultCompressionType() uint32 {
	return uint32(C.LONGTAIL_ZSTD_DEFAULT_COMPRESSION_TYPE)
}

// GetZStdMaxCompressionType ...
func GetZStdMaxCompressionType() uint32 {
	return uint32(C.LONGTAIL_ZSTD_MAX_COMPRESSION_TYPE)
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
func GetFilesRecursively(storageAPI Longtail_StorageAPI, rootPath string) (Longtail_FileInfos, error) {
	cFolderPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cFolderPath))
	var fileInfos *C.struct_Longtail_FileInfos
	errno := C.Longtail_GetFilesRecursively(storageAPI.cStorageAPI, cFolderPath, &fileInfos)
	if errno != 0 {
		return Longtail_FileInfos{cFileInfos: nil}, fmt.Errorf("GetFilesRecursively: C.Longtail_GetFilesRecursively(`%s`) failed with error %d", rootPath, errno)
	}
	return Longtail_FileInfos{cFileInfos: fileInfos}, nil
}

// GetPath ...
func GetPath(paths Longtail_Paths, index uint32) string {
	cPath := C.GetPath(paths.cPaths.m_Offsets, paths.cPaths.m_Data, C.uint32_t(index))
	return C.GoString(cPath)
}

// GetVersionIndexPath ...
func GetVersionIndexPath(vindex Longtail_VersionIndex, index uint32) string {
	cPath := C.GetPath(vindex.cVersionIndex.m_NameOffsets, vindex.cVersionIndex.m_NameData, C.uint32_t(index))
	return C.GoString(cPath)
}

// CreateVersionIndexUtil ...
func CreateVersionIndex(
	storageAPI Longtail_StorageAPI,
	hashAPI Longtail_HashAPI,
	jobAPI Longtail_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	rootPath string,
	paths Longtail_Paths,
	assetSizes []uint64,
	assetCompressionTypes []uint32,
	maxChunkSize uint32) (Longtail_VersionIndex, error) {
	progressProxyData := makeProgressProxy(progressFunc, progressContext)
	cProgressProxyData := SavePointer(&progressProxyData)
	defer UnrefPointer(cProgressProxyData)

	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))

	cAssetSizes := unsafe.Pointer(nil)
	if len(assetSizes) > 0 {
		cAssetSizes = unsafe.Pointer(&assetSizes[0])
	}

	cCompressionTypes := unsafe.Pointer(nil)
	if len(assetCompressionTypes) > 0 {
		cCompressionTypes = unsafe.Pointer(&assetCompressionTypes[0])
	}

	var vindex *C.struct_Longtail_VersionIndex
	errno := C.Longtail_CreateVersionIndex(
		storageAPI.cStorageAPI,
		hashAPI.cHashAPI,
		jobAPI.cJobAPI,
		(C.Longtail_JobAPI_ProgressFunc)(C.progressProxy),
		cProgressProxyData,
		cRootPath,
		paths.cPaths,
		(*C.uint64_t)(cAssetSizes),
		(*C.uint32_t)(cCompressionTypes),
		C.uint32_t(maxChunkSize),
		&vindex)

	if errno != 0 {
		return Longtail_VersionIndex{cVersionIndex: nil}, fmt.Errorf("CreateVersionIndex: C.Longtail_CreateVersionIndex(`%s`): failed with error %d", rootPath, errno)
	}

	return Longtail_VersionIndex{cVersionIndex: vindex}, nil
}

// WriteVersionIndexToBuffer ...
func WriteVersionIndexToBuffer(index Longtail_VersionIndex) ([]byte, error) {
	var buffer unsafe.Pointer
	size := C.size_t(0)
	errno := C.Longtail_WriteVersionIndexToBuffer(index.cVersionIndex, &buffer, &size)
	if errno != 0 {
		return nil, fmt.Errorf("WriteVersionIndexToBuffer: C.Longtail_WriteVersionIndexToBuffer() failed with error %d", errno)
	}
	defer C.Longtail_Free(buffer)
	bytes := C.GoBytes(buffer, C.int(size))
	return bytes, nil
}

// WriteVersionIndex ...
func WriteVersionIndex(storageAPI Longtail_StorageAPI, index Longtail_VersionIndex, path string) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	errno := C.Longtail_WriteVersionIndex(storageAPI.cStorageAPI, index.cVersionIndex, cPath)
	if errno != 0 {
		return fmt.Errorf("WriteVersionIndex: C.Longtail_WriteVersionIndex(`%s`) failed with error %d", path, errno)
	}
	return nil
}

// ReadVersionIndexFromBuffer ...
func ReadVersionIndexFromBuffer(buffer []byte) (Longtail_VersionIndex, error) {
	cBuffer := unsafe.Pointer(&buffer[0])
	cSize := C.size_t(len(buffer))
	var vindex *C.struct_Longtail_VersionIndex
	errno := C.Longtail_ReadVersionIndexFromBuffer(cBuffer, cSize, &vindex)
	if errno != 0 {
		return Longtail_VersionIndex{cVersionIndex: nil}, fmt.Errorf("ReadVersionIndexFromBuffer: C.Longtail_ReadVersionIndexFromBuffer() failed with error %d", errno)
	}
	return Longtail_VersionIndex{cVersionIndex: vindex}, nil
}

// ReadVersionIndex ...
func ReadVersionIndex(storageAPI Longtail_StorageAPI, path string) (Longtail_VersionIndex, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var vindex *C.struct_Longtail_VersionIndex
	errno := C.Longtail_ReadVersionIndex(storageAPI.cStorageAPI, cPath, &vindex)
	if errno != 0 {
		return Longtail_VersionIndex{cVersionIndex: nil}, fmt.Errorf("ReadVersionIndex: C.Longtail_ReadVersionIndex(`%s`) failed with error %d", path, errno)
	}
	return Longtail_VersionIndex{cVersionIndex: vindex}, nil
}

// CreateContentIndex ...
func CreateContentIndex(
	hashAPI Longtail_HashAPI,
	chunkCount uint64,
	chunkHashes []uint64,
	chunkSizes []uint32,
	compressionTypes []uint32,
	maxBlockSize uint32,
	maxChunksPerBlock uint32) (Longtail_ContentIndex, error) {

	var cindex *C.struct_Longtail_ContentIndex
	if chunkCount == 0 {
		errno := C.Longtail_CreateContentIndex(
			hashAPI.cHashAPI,
			0,
			nil,
			nil,
			nil,
			C.uint32_t(maxBlockSize),
			C.uint32_t(maxChunksPerBlock),
			&cindex)
		if errno != 0 {
			return Longtail_ContentIndex{cContentIndex: nil}, fmt.Errorf("CreateContentIndex: C.Longtail_CreateContentIndex(%d) failed with error %d", chunkCount, errno)
		}
	}
	var cChunkHashes *C.TLongtail_Hash
	var cChunkSizes *C.uint32_t
	var cCompressionTypes *C.uint32_t
	if chunkCount > 0 {
		cChunkHashes = (*C.TLongtail_Hash)(unsafe.Pointer(&chunkHashes[0]))
		cChunkSizes = (*C.uint32_t)(unsafe.Pointer(&chunkSizes[0]))
		cCompressionTypes = (*C.uint32_t)(unsafe.Pointer(&compressionTypes[0]))
	}

	errno := C.Longtail_CreateContentIndex(
		hashAPI.cHashAPI,
		C.uint64_t(chunkCount),
		cChunkHashes,
		cChunkSizes,
		cCompressionTypes,
		C.uint32_t(maxBlockSize),
		C.uint32_t(maxChunksPerBlock),
		&cindex)

	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, fmt.Errorf("CreateContentIndex: C.Longtail_CreateContentIndex(%d) create content index failed with error %d", chunkCount, errno)
	}

	return Longtail_ContentIndex{cContentIndex: cindex}, nil
}

// WriteContentIndexToBuffer ...
func WriteContentIndexToBuffer(index Longtail_ContentIndex) ([]byte, error) {
	var buffer unsafe.Pointer
	size := C.size_t(0)
	errno := C.Longtail_WriteContentIndexToBuffer(index.cContentIndex, &buffer, &size)
	if errno != 0 {
		return nil, fmt.Errorf("WriteContentIndexToBuffer: C.Longtail_WriteContentIndexToBuffer() failed with error %d", errno)
	}
	defer C.Longtail_Free(buffer)
	bytes := C.GoBytes(buffer, C.int(size))
	return bytes, nil
}

// WriteContentIndex ...
func WriteContentIndex(storageAPI Longtail_StorageAPI, index Longtail_ContentIndex, path string) error {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	errno := C.Longtail_WriteContentIndex(storageAPI.cStorageAPI, index.cContentIndex, cPath)
	if errno != 0 {
		return fmt.Errorf("WriteContentIndex: C.Longtail_WriteContentIndex`%s`) failed with error %d", path, errno)
	}
	return nil
}

// ReadContentIndexFromBuffer ...
func ReadContentIndexFromBuffer(buffer []byte) (Longtail_ContentIndex, error) {
	cBuffer := unsafe.Pointer(&buffer[0])
	cSize := C.size_t(len(buffer))
	var cindex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_ReadContentIndexFromBuffer(cBuffer, cSize, &cindex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, fmt.Errorf("ReadContentIndexFromBuffer: C.Longtail_ReadContentIndexFromBuffer() failed with error %d", errno)
	}
	return Longtail_ContentIndex{cContentIndex: cindex}, nil
}

// ReadContentIndex ...
func ReadContentIndex(storageAPI Longtail_StorageAPI, path string) (Longtail_ContentIndex, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var cindex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_ReadContentIndex(storageAPI.cStorageAPI, cPath, &cindex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, fmt.Errorf("ReadContentIndex: C.Longtail_ReadContentIndex(`%s`) failed with error %d", path, errno)
	}
	return Longtail_ContentIndex{cContentIndex: cindex}, nil
}

// WriteContent ...
func WriteContent(
	sourceStorageAPI Longtail_StorageAPI,
	targetStorageAPI Longtail_StorageAPI,
	compressionRegistryAPI Longtail_CompressionRegistryAPI,
	jobAPI Longtail_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	contentIndex Longtail_ContentIndex,
	versionIndex Longtail_VersionIndex,
	versionFolderPath string,
	contentFolderPath string) error {

	progressProxyData := makeProgressProxy(progressFunc, progressContext)
	cProgressProxyData := SavePointer(&progressProxyData)
	defer UnrefPointer(cProgressProxyData)

	cVersionFolderPath := C.CString(versionFolderPath)
	defer C.free(unsafe.Pointer(cVersionFolderPath))

	cContentFolderPath := C.CString(contentFolderPath)
	defer C.free(unsafe.Pointer(cContentFolderPath))

	errno := C.Longtail_WriteContent(
		sourceStorageAPI.cStorageAPI,
		targetStorageAPI.cStorageAPI,
		compressionRegistryAPI.cCompressionRegistryAPI,
		jobAPI.cJobAPI,
		(C.Longtail_JobAPI_ProgressFunc)(C.progressProxy),
		cProgressProxyData,
		contentIndex.cContentIndex,
		versionIndex.cVersionIndex,
		cVersionFolderPath,
		cContentFolderPath)
	if errno != 0 {
		return fmt.Errorf("WriteContent: C.Longtail_WriteContent(`%s`, `%s` failed with error %d", versionFolderPath, contentFolderPath, errno)
	}
	return nil
}

// ReadContent ...
func ReadContent(
	sourceStorageAPI Longtail_StorageAPI,
	hashAPI Longtail_HashAPI,
	jobAPI Longtail_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	contentFolderPath string) (Longtail_ContentIndex, error) {

	progressProxyData := makeProgressProxy(progressFunc, progressContext)
	cProgressProxyData := SavePointer(&progressProxyData)
	defer UnrefPointer(cProgressProxyData)

	cContentFolderPath := C.CString(contentFolderPath)
	defer C.free(unsafe.Pointer(cContentFolderPath))

	var contentIndex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_ReadContent(
		sourceStorageAPI.cStorageAPI,
		hashAPI.cHashAPI,
		jobAPI.cJobAPI,
		(C.Longtail_JobAPI_ProgressFunc)(C.progressProxy),
		cProgressProxyData,
		cContentFolderPath,
		&contentIndex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, fmt.Errorf("ReadContent: C.Longtail_ReadContent(`%s`) failed with error %d", contentFolderPath, errno)
	}
	return Longtail_ContentIndex{cContentIndex: contentIndex}, nil
}

// CreateMissingContent ...
func CreateMissingContent(
	hashAPI Longtail_HashAPI,
	contentIndex Longtail_ContentIndex,
	versionIndex Longtail_VersionIndex,
	maxBlockSize uint32,
	maxChunksPerBlock uint32) (Longtail_ContentIndex, error) {

	var missingContentIndex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_CreateMissingContent(
		hashAPI.cHashAPI,
		contentIndex.cContentIndex,
		versionIndex.cVersionIndex,
		C.uint32_t(maxBlockSize),
		C.uint32_t(maxChunksPerBlock),
		&missingContentIndex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, fmt.Errorf("CreateMissingContent: C.Longtail_CreateMissingContent() failed with error %d", errno)
	}
	return Longtail_ContentIndex{cContentIndex: missingContentIndex}, nil
}

//GetPathsForContentBlocks ...
func GetPathsForContentBlocks(contentIndex Longtail_ContentIndex) (Longtail_Paths, error) {
	var paths *C.struct_Longtail_Paths
	errno := C.Longtail_GetPathsForContentBlocks(contentIndex.cContentIndex, &paths)
	if errno != 0 {
		return Longtail_Paths{cPaths: nil}, fmt.Errorf("GetPathsForContentBlocks: C.Longtail_GetPathsForContentBlocks() failed with error %d", errno)
	}
	return Longtail_Paths{cPaths: paths}, nil
}

// RetargetContent ...
func RetargetContent(
	referenceContentIndex Longtail_ContentIndex,
	contentIndex Longtail_ContentIndex) (Longtail_ContentIndex, error) {
	var retargetedContentIndex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_RetargetContent(referenceContentIndex.cContentIndex, contentIndex.cContentIndex, &retargetedContentIndex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, fmt.Errorf("RetargetContent: C.Longtail_RetargetContent() failed with error %d", errno)
	}
	return Longtail_ContentIndex{cContentIndex: retargetedContentIndex}, nil
}

// MergeContentIndex ...
func MergeContentIndex(
	localContentIndex Longtail_ContentIndex,
	remoteContentIndex Longtail_ContentIndex) (Longtail_ContentIndex, error) {
	var mergedContentIndex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_MergeContentIndex(localContentIndex.cContentIndex, remoteContentIndex.cContentIndex, &mergedContentIndex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, fmt.Errorf("MergeContentIndex: C.Longtail_MergeContentIndex() failed with error %d", errno)
	}
	return Longtail_ContentIndex{cContentIndex: mergedContentIndex}, nil
}

// WriteVersion ...
func WriteVersion(
	contentStorageAPI Longtail_StorageAPI,
	versionStorageAPI Longtail_StorageAPI,
	compressionRegistryAPI Longtail_CompressionRegistryAPI,
	jobAPI Longtail_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	contentIndex Longtail_ContentIndex,
	versionIndex Longtail_VersionIndex,
	contentFolderPath string,
	versionFolderPath string) error {

	progressProxyData := makeProgressProxy(progressFunc, progressContext)
	cProgressProxyData := SavePointer(&progressProxyData)
	defer UnrefPointer(cProgressProxyData)

	cContentFolderPath := C.CString(contentFolderPath)
	defer C.free(unsafe.Pointer(cContentFolderPath))

	cVersionFolderPath := C.CString(versionFolderPath)
	defer C.free(unsafe.Pointer(cVersionFolderPath))

	errno := C.Longtail_WriteVersion(
		contentStorageAPI.cStorageAPI,
		versionStorageAPI.cStorageAPI,
		compressionRegistryAPI.cCompressionRegistryAPI,
		jobAPI.cJobAPI,
		(C.Longtail_JobAPI_ProgressFunc)(C.progressProxy),
		cProgressProxyData,
		contentIndex.cContentIndex,
		versionIndex.cVersionIndex,
		cContentFolderPath,
		cVersionFolderPath)
	if errno != 0 {
		return fmt.Errorf("WriteVersion: C.Longtail_WriteVersion(`%s`, `%s) failed with error %d", versionFolderPath, contentFolderPath, errno)
	}
	return nil
}

//CreateVersionDiff do we really need this? Maybe ChangeVersion should create one on the fly?
func CreateVersionDiff(
	sourceVersionIndex Longtail_VersionIndex,
	targetVersionIndex Longtail_VersionIndex) (Longtail_VersionDiff, error) {
	var versionDiff *C.struct_Longtail_VersionDiff
	errno := C.Longtail_CreateVersionDiff(sourceVersionIndex.cVersionIndex, targetVersionIndex.cVersionIndex, &versionDiff)
	if errno != 0 {
		return Longtail_VersionDiff{cVersionDiff: nil}, fmt.Errorf("CreateVersionDiff: C.Longtail_CreateVersionDiff() failed with error %d", errno)
	}
	return Longtail_VersionDiff{cVersionDiff: versionDiff}, nil
}

//ChangeVersion ...
func ChangeVersion(
	contentStorageAPI Longtail_StorageAPI,
	versionStorageAPI Longtail_StorageAPI,
	hashAPI Longtail_HashAPI,
	jobAPI Longtail_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	compressionRegistryAPI Longtail_CompressionRegistryAPI,
	contentIndex Longtail_ContentIndex,
	sourceVersionIndex Longtail_VersionIndex,
	targetVersionIndex Longtail_VersionIndex,
	versionDiff Longtail_VersionDiff,
	contentFolderPath string,
	versionFolderPath string) error {

	progressProxyData := makeProgressProxy(progressFunc, progressContext)
	cProgressProxyData := SavePointer(&progressProxyData)
	defer UnrefPointer(cProgressProxyData)

	cContentFolderPath := C.CString(contentFolderPath)
	defer C.free(unsafe.Pointer(cContentFolderPath))

	cVersionFolderPath := C.CString(versionFolderPath)
	defer C.free(unsafe.Pointer(cVersionFolderPath))

	errno := C.Longtail_ChangeVersion(
		contentStorageAPI.cStorageAPI,
		versionStorageAPI.cStorageAPI,
		hashAPI.cHashAPI,
		jobAPI.cJobAPI,
		(C.Longtail_JobAPI_ProgressFunc)(C.progressProxy),
		cProgressProxyData,
		compressionRegistryAPI.cCompressionRegistryAPI,
		contentIndex.cContentIndex,
		sourceVersionIndex.cVersionIndex,
		targetVersionIndex.cVersionIndex,
		versionDiff.cVersionDiff,
		cContentFolderPath,
		cVersionFolderPath)
	if errno != 0 {
		return fmt.Errorf("ChangeVersion: C.Longtail_ChangeVersio(`%s`, `%s`) failed with error %d", versionFolderPath, contentFolderPath, errno)
	}
	return nil
}

//export progressProxy
func progressProxy(progress unsafe.Pointer, total C.uint32_t, done C.uint32_t) {
	progressProxyData := RestorePointer(progress).(*progressProxyData)
	progressProxyData.progressFunc(progressProxyData.Context, int(total), int(done))
}

//export logProxy
func logProxy(context unsafe.Pointer, level C.int, log *C.char) {
	logProxyData := RestorePointer(context).(*logProxyData)
	logProxyData.logFunc(logProxyData.Context, int(level), C.GoString(log))
}

//SetLogger ...
func SetLogger(logFunc logFunc, logContext interface{}) unsafe.Pointer {
	logProxyData := makeLogProxy(logFunc, logContext)
	cLogProxyData := SavePointer(&logProxyData)

	C.Longtail_SetLog(C.Longtail_Log(C.logProxy), cLogProxyData)
	return cLogProxyData
}

//ClearLogger ...
func ClearLogger(logger unsafe.Pointer) {
	C.Longtail_SetLog(nil, nil)
	UnrefPointer(logger)
}

//SetLogLevel ...
func SetLogLevel(level int) {
	C.Longtail_SetLogLevel(C.int(level))
}

var activeAssertFunc assertFunc
var activeAssertContext interface{}

//SetAssert ...
func SetAssert(assertFunc assertFunc, assertContext interface{}) {
	activeAssertFunc = assertFunc
	activeAssertContext = assertContext
	C.Longtail_SetAssert(C.Longtail_Assert(C.assertProxy))
}

//ClearAssert ...
func ClearAssert() {
	C.Longtail_SetAssert(C.Longtail_Assert(nil))
	activeAssertFunc = nil
	activeAssertContext = nil
}

//export assertProxy
func assertProxy(expression *C.char, file *C.char, line C.int) {
	if activeAssertFunc != nil {
		activeAssertFunc(activeAssertContext, C.GoString(expression), C.GoString(file), int(line))
	}
}
