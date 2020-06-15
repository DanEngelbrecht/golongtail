package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -maes -pthread -O3
// #include "golongtail.h"
import "C"
import (
	"fmt"
	"reflect"
	"sync/atomic"
	"unsafe"
)

const EPERM = 1       /* Not super-user */
const ENOENT = 2      /* No such file or directory */
const ESRCH = 3       /* No such process */
const EINTR = 4       /* Interrupted system call */
const EIO = 5         /* I/O error */
const ENXIO = 6       /* No such device or address */
const E2BIG = 7       /* Arg list too long */
const ENOEXEC = 8     /* Exec format error */
const EBADF = 9       /* Bad file number */
const ECHILD = 10     /* No children */
const EAGAIN = 11     /* No more processes */
const ENOMEM = 12     /* Not enough core */
const EACCES = 13     /* Permission denied */
const EFAULT = 14     /* Bad address */
const ENOTBLK = 15    /* Block device required */
const EBUSY = 16      /* Mount device busy */
const EEXIST = 17     /* File exists */
const EXDEV = 18      /* Cross-device link */
const ENODEV = 19     /* No such device */
const ENOTDIR = 20    /* Not a directory */
const EISDIR = 21     /* Is a directory */
const EINVAL = 22     /* Invalid argument */
const ENFILE = 23     /* Too many open files in system */
const EMFILE = 24     /* Too many open files */
const ENOTTY = 25     /* Not a typewriter */
const ETXTBSY = 26    /* Text file busy */
const EFBIG = 27      /* File too large */
const ENOSPC = 28     /* No space left on device */
const ESPIPE = 29     /* Illegal seek */
const EROFS = 30      /* Read only file system */
const EMLINK = 31     /* Too many links */
const EPIPE = 32      /* Broken pipe */
const EDOM = 33       /* Math arg out of domain of func */
const ERANGE = 34     /* Math result not representable */
const ECANCELED = 105 /* Operation canceled */

func ErrNoToDescription(errno int) string {
	switch errno {
	case EPERM:
		return "Not super-user"
	case ENOENT:
		return "No such file or directory"
	case ESRCH:
		return "No such process"
	case EINTR:
		return "Interrupted system call"
	case EIO:
		return "I/O error"
	case ENXIO:
		return "No such device or address"
	case E2BIG:
		return "Arg list too long"
	case ENOEXEC:
		return "Exec format error"
	case EBADF:
		return "Bad file number"
	case ECHILD:
		return "No children"
	case EAGAIN:
		return "No more processes"
	case ENOMEM:
		return "Not enough core"
	case EACCES:
		return "Permission denied"
	case EFAULT:
		return "Bad address"
	case ENOTBLK:
		return "Block device required"
	case EBUSY:
		return "Mount device busy"
	case EEXIST:
		return "File exists"
	case EXDEV:
		return "Cross-device link"
	case ENODEV:
		return "No such device"
	case ENOTDIR:
		return "Not a directory"
	case EISDIR:
		return "Is a directory"
	case EINVAL:
		return "Invalid argument"
	case ENFILE:
		return "Too many open files in system"
	case EMFILE:
		return "Too many open files"
	case ENOTTY:
		return "Not a typewriter"
	case ETXTBSY:
		return "Text file busy"
	case EFBIG:
		return "File too large"
	case ENOSPC:
		return "No space left on device"
	case ESPIPE:
		return "Illegal seek"
	case EROFS:
		return "Read only file system"
	case EMLINK:
		return "Too many links"
	case EPIPE:
		return "Broken pipe"
	case EDOM:
		return "Math arg out of domain of func"
	case ERANGE:
		return "Math result not representable"
	case ECANCELED:
		return "Operation canceled"
	}
	return fmt.Sprintf("%d", errno)
}

type ProgressAPI interface {
	OnProgress(totalCount uint32, doneCount uint32)
}
type PathFilterAPI interface {
	Include(rootPath string, assetPath string, assetName string, isDir bool, size uint64, permissions uint16) bool
}

type AsyncPutStoredBlockAPI interface {
	OnComplete(errno int)
}

type AsyncGetStoredBlockAPI interface {
	OnComplete(stored_block Longtail_StoredBlock, errno int)
}

type AsyncGetIndexAPI interface {
	OnComplete(content_index Longtail_ContentIndex, errno int)
}

type AsyncRetargetContentAPI interface {
	OnComplete(content_index Longtail_ContentIndex, errno int)
}

type Assert interface {
	OnAssert(expression string, file string, line int)
}

type Logger interface {
	OnLog(level int, log string)
}

type Longtail_AsyncGetStoredBlockAPI struct {
	cAsyncCompleteAPI *C.struct_Longtail_AsyncGetStoredBlockAPI
}

type Longtail_AsyncPutStoredBlockAPI struct {
	cAsyncCompleteAPI *C.struct_Longtail_AsyncPutStoredBlockAPI
}

type Longtail_AsyncGetIndexAPI struct {
	cAsyncCompleteAPI *C.struct_Longtail_AsyncGetIndexAPI
}

type Longtail_AsyncRetargetContentAPI struct {
	cAsyncCompleteAPI *C.struct_Longtail_AsyncRetargetContentAPI
}

type BlockStoreStats struct {
	IndexGetCount      uint64
	BlocksGetCount     uint64
	BlocksPutCount     uint64
	ChunksGetCount     uint64
	ChunksPutCount     uint64
	BytesGetCount      uint64
	BytesPutCount      uint64
	IndexGetRetryCount uint64
	BlockGetRetryCount uint64
	BlockPutRetryCount uint64
	IndexGetFailCount  uint64
	BlockGetFailCount  uint64
	BlockPutFailCount  uint64
}

type BlockStoreAPI interface {
	PutStoredBlock(storedBlock Longtail_StoredBlock, asyncCompleteAPI Longtail_AsyncPutStoredBlockAPI) int
	PreflightGet(blockCount uint64, hashes []uint64, refCounts []uint32) int
	GetStoredBlock(blockHash uint64, asyncCompleteAPI Longtail_AsyncGetStoredBlockAPI) int
	GetIndex(asyncCompleteAPI Longtail_AsyncGetIndexAPI) int
	RetargetContent(contentIndex Longtail_ContentIndex, asyncCompleteAPI Longtail_AsyncRetargetContentAPI) int
	GetStats() (BlockStoreStats, int)
	Close()
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

type Longtail_ProgressAPI struct {
	cProgressAPI *C.struct_Longtail_ProgressAPI
}

type Longtail_PathFilterAPI struct {
	cPathFilterAPI *C.struct_Longtail_PathFilterAPI
}

type Longtail_JobAPI struct {
	cJobAPI *C.struct_Longtail_JobAPI
}

type Longtail_CompressionRegistryAPI struct {
	cCompressionRegistryAPI *C.struct_Longtail_CompressionRegistryAPI
}

type Longtail_HashRegistryAPI struct {
	cHashRegistryAPI *C.struct_Longtail_HashRegistryAPI
}

type Longtail_CompressionAPI struct {
	cCompressionAPI *C.struct_Longtail_CompressionAPI
}

type Longtail_BlockStoreAPI struct {
	cBlockStoreAPI *C.struct_Longtail_BlockStoreAPI
}

type Longtail_StoredBlock struct {
	cStoredBlock *C.struct_Longtail_StoredBlock
}

type Longtail_BlockIndex struct {
	cBlockIndex *C.struct_Longtail_BlockIndex
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
func (storageAPI *Longtail_StorageAPI) ReadFromStorage(rootPath string, path string) ([]byte, int) {
	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cFullPath := C.Longtail_Storage_ConcatPath(storageAPI.cStorageAPI, cRootPath, cPath)
	defer C.Longtail_Free(unsafe.Pointer(cFullPath))

	var cOpenFile C.Longtail_StorageAPI_HOpenFile
	errno := C.Longtail_Storage_OpenReadFile(storageAPI.cStorageAPI, cFullPath, &cOpenFile)
	if errno != 0 {
		return nil, int(errno)
	}
	defer C.Longtail_Storage_CloseFile(storageAPI.cStorageAPI, cOpenFile)

	var cSize C.uint64_t
	errno = C.Longtail_Storage_GetSize(storageAPI.cStorageAPI, cOpenFile, &cSize)
	if errno != 0 {
		return nil, int(errno)
	}

	blockData := make([]byte, int(cSize))
	if cSize > 0 {
		errno = C.Longtail_Storage_Read(storageAPI.cStorageAPI, cOpenFile, 0, cSize, unsafe.Pointer(&blockData[0]))
		if errno != 0 {
			return nil, int(errno)
		}
	}
	return blockData, 0
}

// WriteToStorage ...
func (storageAPI *Longtail_StorageAPI) WriteToStorage(rootPath string, path string, blockData []byte) int {
	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cFullPath := C.Longtail_Storage_ConcatPath(storageAPI.cStorageAPI, cRootPath, cPath)
	defer C.Longtail_Free(unsafe.Pointer(cFullPath))

	errno := C.EnsureParentPathExists(storageAPI.cStorageAPI, cFullPath)
	if errno != 0 {
		return int(errno)
	}

	blockSize := C.uint64_t(len(blockData))

	var cOpenFile C.Longtail_StorageAPI_HOpenFile
	errno = C.Longtail_Storage_OpenWriteFile(storageAPI.cStorageAPI, cFullPath, blockSize, &cOpenFile)
	if errno != 0 {
		return int(errno)
	}
	defer C.Longtail_Storage_CloseFile(storageAPI.cStorageAPI, cOpenFile)

	if blockSize > 0 {
		data := unsafe.Pointer(&blockData[0])
		errno = C.Longtail_Storage_Write(storageAPI.cStorageAPI, cOpenFile, 0, blockSize, data)
	}

	if errno != 0 {
		return int(errno)
	}
	return 0
}

func (fileInfos *Longtail_FileInfos) Dispose() {
	if fileInfos.cFileInfos != nil {
		C.Longtail_Free(unsafe.Pointer(fileInfos.cFileInfos))
		fileInfos.cFileInfos = nil
	}
}

func carray2slice64(array *C.uint64_t, len int) []uint64 {
	var list []uint64
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&list)))
	sliceHeader.Cap = len
	sliceHeader.Len = len
	sliceHeader.Data = uintptr(unsafe.Pointer(array))
	return list
}

func carray2slice32(array *C.uint32_t, len int) []uint32 {
	var list []uint32
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&list)))
	sliceHeader.Cap = len
	sliceHeader.Len = len
	sliceHeader.Data = uintptr(unsafe.Pointer(array))
	return list
}

func carray2slice16(array *C.uint16_t, len int) []uint16 {
	var list []uint16
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&list)))
	sliceHeader.Cap = len
	sliceHeader.Len = len
	sliceHeader.Data = uintptr(unsafe.Pointer(array))
	return list
}

func carray2sliceByte(array *C.char, len int) []byte {
	var list []byte
	sliceHeader := (*reflect.SliceHeader)((unsafe.Pointer(&list)))
	sliceHeader.Cap = len
	sliceHeader.Len = len
	sliceHeader.Data = uintptr(unsafe.Pointer(array))
	return list
}

func (fileInfos *Longtail_FileInfos) GetFileCount() uint32 {
	return uint32(fileInfos.cFileInfos.m_Count)
}

func (fileInfos *Longtail_FileInfos) GetFileSizes() []uint64 {
	size := int(fileInfos.cFileInfos.m_Count)
	return carray2slice64(fileInfos.cFileInfos.m_Sizes, size)
}

func (fileInfos *Longtail_FileInfos) GetFilePermissions() []uint16 {
	size := int(fileInfos.cFileInfos.m_Count)
	return carray2slice16(fileInfos.cFileInfos.m_Permissions, size)
}

func (contentIndex *Longtail_ContentIndex) IsValid() bool {
	return contentIndex.cContentIndex != nil
}

func (contentIndex *Longtail_ContentIndex) Dispose() {
	if contentIndex.cContentIndex != nil {
		C.Longtail_Free(unsafe.Pointer(contentIndex.cContentIndex))
		contentIndex.cContentIndex = nil
	}
}

func (contentIndex *Longtail_ContentIndex) GetVersion() uint32 {
	return uint32(*contentIndex.cContentIndex.m_Version)
}

func (contentIndex *Longtail_ContentIndex) GetHashIdentifier() uint32 {
	return uint32(*contentIndex.cContentIndex.m_HashIdentifier)
}

func (contentIndex *Longtail_ContentIndex) GetMaxBlockSize() uint32 {
	return uint32(*contentIndex.cContentIndex.m_MaxBlockSize)
}

func (contentIndex *Longtail_ContentIndex) GetMaxChunksPerBlock() uint32 {
	return uint32(*contentIndex.cContentIndex.m_MaxChunksPerBlock)
}

func (contentIndex *Longtail_ContentIndex) GetChunkSizes() []uint32 {
	size := int(*contentIndex.cContentIndex.m_ChunkCount)
	return carray2slice32(contentIndex.cContentIndex.m_ChunkLengths, size)
}

func (hashAPI *Longtail_HashAPI) GetIdentifier() uint32 {
	return uint32(C.Longtail_Hash_GetIdentifier(hashAPI.cHashAPI))
}

func (contentIndex *Longtail_ContentIndex) GetBlockCount() uint64 {
	return uint64(*contentIndex.cContentIndex.m_BlockCount)
}

func (contentIndex *Longtail_ContentIndex) GetChunkCount() uint64 {
	return uint64(*contentIndex.cContentIndex.m_ChunkCount)
}

func (contentIndex *Longtail_ContentIndex) GetBlockHashes() []uint64 {
	size := int(C.Longtail_ContentIndex_GetBlockCount(contentIndex.cContentIndex))
	return carray2slice64(C.Longtail_ContentIndex_BlockHashes(contentIndex.cContentIndex), size)
}

func (versionIndex *Longtail_VersionIndex) Dispose() {
	if versionIndex.cVersionIndex != nil {
		C.Longtail_Free(unsafe.Pointer(versionIndex.cVersionIndex))
		versionIndex.cVersionIndex = nil
	}
}

func (versionIndex *Longtail_VersionIndex) GetVersion() uint32 {
	return uint32(*versionIndex.cVersionIndex.m_Version)
}

func (versionIndex *Longtail_VersionIndex) GetHashIdentifier() uint32 {
	return uint32(*versionIndex.cVersionIndex.m_HashIdentifier)
}

func (versionIndex *Longtail_VersionIndex) GetTargetChunkSize() uint32 {
	return uint32(*versionIndex.cVersionIndex.m_TargetChunkSize)
}

func (versionIndex *Longtail_VersionIndex) GetAssetCount() uint32 {
	return uint32(*versionIndex.cVersionIndex.m_AssetCount)
}

func (versionIndex *Longtail_VersionIndex) GetAssetPath(assetIndex uint32) string {
	cPath := C.GetVersionIndexPath(versionIndex.cVersionIndex, C.uint32_t(assetIndex))
	return C.GoString(cPath)
}

func (versionIndex *Longtail_VersionIndex) GetAssetSize(assetIndex uint32) uint64 {
	cSize := C.GetVersionAssetSize(versionIndex.cVersionIndex, C.uint32_t(assetIndex))
	return uint64(cSize)
}

func (versionIndex *Longtail_VersionIndex) GetAssetPermissions(assetIndex uint32) uint16 {
	cPermissions := C.GetVersionAssetPermissions(versionIndex.cVersionIndex, C.uint32_t(assetIndex))
	return uint16(cPermissions)
}

func (versionIndex *Longtail_VersionIndex) GetChunkCount() uint32 {
	return uint32(*versionIndex.cVersionIndex.m_ChunkCount)
}

func (versionIndex *Longtail_VersionIndex) GetChunkHashes() []uint64 {
	size := int(*versionIndex.cVersionIndex.m_ChunkCount)
	return carray2slice64(versionIndex.cVersionIndex.m_ChunkHashes, size)
}

func (versionIndex *Longtail_VersionIndex) GetChunkSizes() []uint32 {
	size := int(*versionIndex.cVersionIndex.m_ChunkCount)
	return carray2slice32(versionIndex.cVersionIndex.m_ChunkSizes, size)
}

func (versionIndex *Longtail_VersionIndex) GetAssetSizes() []uint64 {
	size := int(*versionIndex.cVersionIndex.m_AssetCount)
	return carray2slice64(versionIndex.cVersionIndex.m_AssetSizes, size)
}

func (versionIndex *Longtail_VersionIndex) GetChunkTags() []uint32 {
	size := int(*versionIndex.cVersionIndex.m_ChunkCount)
	return carray2slice32(versionIndex.cVersionIndex.m_ChunkTags, size)
}

func (versionDiff *Longtail_VersionDiff) Dispose() {
	if versionDiff.cVersionDiff != nil {
		C.Longtail_Free(unsafe.Pointer(versionDiff.cVersionDiff))
		versionDiff.cVersionDiff = nil
	}
}

// CreateFullHashRegistry ...
func CreateFullHashRegistry() Longtail_HashRegistryAPI {
	return Longtail_HashRegistryAPI{cHashRegistryAPI: C.Longtail_CreateFullHashRegistry()}
}

// CreateBlake3HashRegistry ...
func CreateBlake3HashRegistry() Longtail_HashRegistryAPI {
	return Longtail_HashRegistryAPI{cHashRegistryAPI: C.Longtail_CreateBlake3HashRegistry()}
}

// Longtail_HashRegistryAPI ...
func (hashRegistry *Longtail_HashRegistryAPI) Dispose() {
	if hashRegistry.cHashRegistryAPI != nil {
		C.Longtail_DisposeAPI(&hashRegistry.cHashRegistryAPI.m_API)
		hashRegistry.cHashRegistryAPI = nil
	}
}

// Longtail_HashRegistryAPI ...
func (hashRegistry *Longtail_HashRegistryAPI) GetHashAPI(hashIdentifier uint32) (Longtail_HashAPI, int) {
	var hash_api *C.struct_Longtail_HashAPI
	errno := C.Longtail_GetHashRegistry_GetHashAPI(hashRegistry.cHashRegistryAPI, C.uint32_t(hashIdentifier), &hash_api)
	if errno != 0 {
		return Longtail_HashAPI{cHashAPI: nil}, int(errno)
	}
	return Longtail_HashAPI{cHashAPI: hash_api}, 0
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
	if hashAPI.cHashAPI != nil {
		C.Longtail_DisposeAPI(&hashAPI.cHashAPI.m_API)
		hashAPI.cHashAPI = nil
	}
}

// GetBlake2HashIdentifier() ...
func GetBlake2HashIdentifier() uint32 {
	return uint32(C.Longtail_GetBlake2HashType())
}

// GetBlake3HashIdentifier() ...
func GetBlake3HashIdentifier() uint32 {
	return uint32(C.Longtail_GetBlake3HashType())
}

// GetMeowHashIdentifier() ...
func GetMeowHashIdentifier() uint32 {
	return uint32(C.Longtail_GetMeowHashType())
}

//// Longtail_AsyncPutStoredBlockAPI::OnComplete() ...
func (asyncCompleteAPI *Longtail_AsyncPutStoredBlockAPI) OnComplete(errno int) {
	C.Longtail_AsyncPutStoredBlock_OnComplete(asyncCompleteAPI.cAsyncCompleteAPI, C.int(errno))
}

//// Longtail_AsyncGetStoredBlockAPI::OnComplete() ...
func (asyncCompleteAPI *Longtail_AsyncGetStoredBlockAPI) OnComplete(stored_block Longtail_StoredBlock, errno int) {
	C.Longtail_AsyncGetStoredBlock_OnComplete(asyncCompleteAPI.cAsyncCompleteAPI, stored_block.cStoredBlock, C.int(errno))
}

//// Longtail_AsyncGetIndexAPI::OnComplete() ...
func (asyncCompleteAPI *Longtail_AsyncGetIndexAPI) OnComplete(content_index Longtail_ContentIndex, errno int) {
	C.Longtail_AsyncGetIndex_OnComplete(asyncCompleteAPI.cAsyncCompleteAPI, content_index.cContentIndex, C.int(errno))
}

//// Longtail_AsyncRetargetContentAPI::OnComplete() ...
func (asyncCompleteAPI *Longtail_AsyncRetargetContentAPI) OnComplete(content_index Longtail_ContentIndex, errno int) {
	C.Longtail_AsyncRetargetContent_OnComplete(asyncCompleteAPI.cAsyncCompleteAPI, content_index.cContentIndex, C.int(errno))
}

// CreateFSBlockStore() ...
func CreateFSBlockStore(storageAPI Longtail_StorageAPI, contentPath string, defaultMaxBlockSize uint32, defaultMaxChunksPerBlock uint32) Longtail_BlockStoreAPI {
	cContentPath := C.CString(contentPath)
	defer C.free(unsafe.Pointer(cContentPath))
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateFSBlockStoreAPI(storageAPI.cStorageAPI, cContentPath, C.uint32_t(defaultMaxBlockSize), C.uint32_t(defaultMaxChunksPerBlock))}
}

// CreateCacheBlockStore() ...
func CreateCacheBlockStore(cacheBlockStore Longtail_BlockStoreAPI, persistentBlockStore Longtail_BlockStoreAPI) Longtail_BlockStoreAPI {
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateCacheBlockStoreAPI(cacheBlockStore.cBlockStoreAPI, persistentBlockStore.cBlockStoreAPI)}
}

// CreateCompressBlockStore() ...
func CreateCompressBlockStore(backingBlockStore Longtail_BlockStoreAPI, compressionRegistry Longtail_CompressionRegistryAPI) Longtail_BlockStoreAPI {
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateCompressBlockStoreAPI(backingBlockStore.cBlockStoreAPI, compressionRegistry.cCompressionRegistryAPI)}
}

// CreateRetainingBlockStore() ...
func CreateRetainingBlockStore(backingBlockStore Longtail_BlockStoreAPI) Longtail_BlockStoreAPI {
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateRetainingBlockStoreAPI(backingBlockStore.cBlockStoreAPI)}
}

// CreateShareBlockStore() ...
func CreateShareBlockStore(backingBlockStore Longtail_BlockStoreAPI) Longtail_BlockStoreAPI {
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateShareBlockStoreAPI(backingBlockStore.cBlockStoreAPI)}
}

// Longtail_BlockStoreAPI.Dispose() ...
func (blockStoreAPI *Longtail_BlockStoreAPI) Dispose() {
	if blockStoreAPI.cBlockStoreAPI != nil {
		C.Longtail_DisposeAPI(&blockStoreAPI.cBlockStoreAPI.m_API)
		blockStoreAPI.cBlockStoreAPI = nil
	}
}

//// PutStoredBlock() ...
func (blockStoreAPI *Longtail_BlockStoreAPI) PutStoredBlock(
	storedBlock Longtail_StoredBlock,
	asyncCompleteAPI Longtail_AsyncPutStoredBlockAPI) int {
	errno := C.Longtail_BlockStore_PutStoredBlock(
		blockStoreAPI.cBlockStoreAPI,
		storedBlock.cStoredBlock,
		asyncCompleteAPI.cAsyncCompleteAPI)
	return int(errno)
}

// GetStoredBlock() ...
func (blockStoreAPI *Longtail_BlockStoreAPI) GetStoredBlock(
	blockHash uint64,
	asyncCompleteAPI Longtail_AsyncGetStoredBlockAPI) int {

	errno := C.Longtail_BlockStore_GetStoredBlock(
		blockStoreAPI.cBlockStoreAPI,
		C.uint64_t(blockHash),
		asyncCompleteAPI.cAsyncCompleteAPI)
	return int(errno)
}

// GetIndex() ...
func (blockStoreAPI *Longtail_BlockStoreAPI) GetIndex(
	asyncCompleteAPI Longtail_AsyncGetIndexAPI) int {

	errno := C.Longtail_BlockStore_GetIndex(
		blockStoreAPI.cBlockStoreAPI,
		asyncCompleteAPI.cAsyncCompleteAPI)
	return int(errno)
}

// RetargetContent() ...
func (blockStoreAPI *Longtail_BlockStoreAPI) RetargetContent(
	contentIndex Longtail_ContentIndex,
	asyncCompleteAPI Longtail_AsyncRetargetContentAPI) int {

	errno := C.Longtail_BlockStore_RetargetContent(
		blockStoreAPI.cBlockStoreAPI,
		contentIndex.cContentIndex,
		asyncCompleteAPI.cAsyncCompleteAPI)
	return int(errno)
}

// GetStats() ...
func (blockStoreAPI *Longtail_BlockStoreAPI) GetStats() (BlockStoreStats, int) {
	var cStats C.struct_Longtail_BlockStore_Stats
	errno := C.Longtail_BlockStore_GetStats(
		blockStoreAPI.cBlockStoreAPI,
		&cStats)

	stats := BlockStoreStats{
		IndexGetCount:      (uint64)(cStats.m_IndexGetCount),
		BlocksGetCount:     (uint64)(cStats.m_BlocksGetCount),
		BlocksPutCount:     (uint64)(cStats.m_BlocksPutCount),
		ChunksGetCount:     (uint64)(cStats.m_ChunksGetCount),
		ChunksPutCount:     (uint64)(cStats.m_ChunksPutCount),
		BytesGetCount:      (uint64)(cStats.m_BytesGetCount),
		BytesPutCount:      (uint64)(cStats.m_BytesPutCount),
		IndexGetRetryCount: (uint64)(cStats.m_IndexGetRetryCount),
		BlockGetRetryCount: (uint64)(cStats.m_BlockGetRetryCount),
		BlockPutRetryCount: (uint64)(cStats.m_BlockPutRetryCount),
		IndexGetFailCount:  (uint64)(cStats.m_IndexGetFailCount),
		BlockGetFailCount:  (uint64)(cStats.m_BlockGetFailCount),
		BlockPutFailCount:  (uint64)(cStats.m_BlockPutFailCount),
	}
	return stats, int(errno)
}

func (blockIndex *Longtail_BlockIndex) GetBlockHash() uint64 {
	return uint64(*blockIndex.cBlockIndex.m_BlockHash)
}

func (blockIndex *Longtail_BlockIndex) GetHashIdentifier() uint32 {
	return uint32(*blockIndex.cBlockIndex.m_HashIdentifier)
}

func (blockIndex *Longtail_BlockIndex) GetChunkCount() uint32 {
	return uint32(*blockIndex.cBlockIndex.m_ChunkCount)
}

func (blockIndex *Longtail_BlockIndex) GetTag() uint32 {
	return uint32(*blockIndex.cBlockIndex.m_Tag)
}

func (blockIndex *Longtail_BlockIndex) GetChunkHashes() []uint64 {
	size := int(*blockIndex.cBlockIndex.m_ChunkCount)
	return carray2slice64(blockIndex.cBlockIndex.m_ChunkHashes, size)
}

func (blockIndex *Longtail_BlockIndex) GetChunkSizes() []uint32 {
	size := int(*blockIndex.cBlockIndex.m_ChunkCount)
	return carray2slice32(blockIndex.cBlockIndex.m_ChunkSizes, size)
}

func (storedBlock *Longtail_StoredBlock) GetBlockIndex() Longtail_BlockIndex {
	return Longtail_BlockIndex{cBlockIndex: storedBlock.cStoredBlock.m_BlockIndex}
}

func (storedBlock *Longtail_StoredBlock) GetChunksBlockData() []byte {
	size := int(storedBlock.cStoredBlock.m_BlockChunksDataSize)
	return carray2sliceByte((*C.char)(storedBlock.cStoredBlock.m_BlockData), size)
}

func (storedBlock *Longtail_StoredBlock) Dispose() {
	if storedBlock.cStoredBlock != nil {
		C.Longtail_StoredBlock_Dispose(storedBlock.cStoredBlock)
		storedBlock.cStoredBlock = nil
	}
}

func (blockIndex *Longtail_BlockIndex) Dispose() {
	if blockIndex.cBlockIndex != nil {
		C.Longtail_Free(unsafe.Pointer(blockIndex.cBlockIndex))
		blockIndex.cBlockIndex = nil
	}
}

func WriteStoredBlockToBuffer(storedBlock Longtail_StoredBlock) ([]byte, int) {
	var buffer unsafe.Pointer
	var size C.size_t
	errno := C.Longtail_WriteStoredBlockToBuffer(storedBlock.cStoredBlock, &buffer, &size)
	if errno != 0 {
		return nil, int(errno)
	}
	defer C.Longtail_Free(buffer)
	bytes := C.GoBytes(buffer, C.int(size))
	return bytes, 0
}

func ReadStoredBlockFromBuffer(buffer []byte) (Longtail_StoredBlock, int) {
	cBuffer := unsafe.Pointer(&buffer[0])
	size := C.size_t(len(buffer))
	var stored_block *C.struct_Longtail_StoredBlock
	errno := C.Longtail_ReadStoredBlockFromBuffer(cBuffer, size, &stored_block)
	if errno != 0 {
		return Longtail_StoredBlock{cStoredBlock: nil}, int(errno)
	}
	return Longtail_StoredBlock{cStoredBlock: stored_block}, 0
}

func ValidateContent(contentIndex Longtail_ContentIndex, versionIndex Longtail_VersionIndex) int {
	errno := C.Longtail_ValidateContent(contentIndex.cContentIndex, versionIndex.cVersionIndex)
	return int(errno)
}

// CreateStoredBlock() ...
func CreateStoredBlock(
	blockHash uint64,
	hashIdentifier uint32,
	compressionType uint32,
	chunkHashes []uint64,
	chunkSizes []uint32,
	blockData []uint8,
	blockDataIncludesIndex bool) (Longtail_StoredBlock, int) {
	chunkCount := len(chunkHashes)
	if chunkCount != len(chunkSizes) {
		return Longtail_StoredBlock{cStoredBlock: nil}, EINVAL
	}
	cChunkHashes := (*C.TLongtail_Hash)(unsafe.Pointer(nil))
	if chunkCount > 0 {
		cChunkHashes = (*C.TLongtail_Hash)(unsafe.Pointer(&chunkHashes[0]))
	}
	cChunkSizes := (*C.uint32_t)(unsafe.Pointer(nil))
	if chunkCount > 0 {
		cChunkSizes = (*C.uint32_t)(unsafe.Pointer(&chunkSizes[0]))
	}
	blockByteCount := len(blockData)
	blockByteOffset := 0
	if blockDataIncludesIndex {
		blockByteOffset = int(C.Longtail_GetBlockIndexSize((C.uint32_t)(chunkCount)))
		blockByteCount = blockByteCount - blockByteOffset
	}
	var cStoredBlock *C.struct_Longtail_StoredBlock
	errno := C.Longtail_CreateStoredBlock(
		C.TLongtail_Hash(blockHash),
		C.uint32_t(hashIdentifier),
		C.uint32_t(chunkCount),
		C.uint32_t(compressionType),
		cChunkHashes,
		cChunkSizes,
		C.uint32_t(blockByteCount),
		&cStoredBlock)
	if errno != 0 {
		return Longtail_StoredBlock{}, int(errno)
	}
	cBlockBytes := unsafe.Pointer(nil)
	if blockByteCount > 0 {
		cBlockBytes = unsafe.Pointer(&blockData[blockByteOffset])
	}
	C.memmove(cStoredBlock.m_BlockData, cBlockBytes, C.size_t(blockByteCount))
	return Longtail_StoredBlock{cStoredBlock: cStoredBlock}, 0
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
	if storageAPI.cStorageAPI != nil {
		C.Longtail_DisposeAPI(&storageAPI.cStorageAPI.m_API)
		storageAPI.cStorageAPI = nil
	}
}

// CreateBrotliCompressionAPI ...
func CreateBrotliCompressionAPI() Longtail_CompressionAPI {
	return Longtail_CompressionAPI{cCompressionAPI: C.Longtail_CreateBrotliCompressionAPI()}
}

// CreateLZ4CompressionAPI ...
func CreateLZ4CompressionAPI() Longtail_CompressionAPI {
	return Longtail_CompressionAPI{cCompressionAPI: C.Longtail_CreateLZ4CompressionAPI()}
}

// CreateZStdCompressionAPI ...
func CreateZStdCompressionAPI() Longtail_CompressionAPI {
	return Longtail_CompressionAPI{cCompressionAPI: C.Longtail_CreateZStdCompressionAPI()}
}

// Longtail_CompressionAPI.Dispose() ...
func (compressionAPI *Longtail_CompressionAPI) Dispose() {
	if compressionAPI.cCompressionAPI != nil {
		C.Longtail_DisposeAPI(&compressionAPI.cCompressionAPI.m_API)
		compressionAPI.cCompressionAPI = nil
	}
}

// CreateBikeshedJobAPI ...
func CreateBikeshedJobAPI(workerCount uint32, workerPriority int) Longtail_JobAPI {
	return Longtail_JobAPI{cJobAPI: C.Longtail_CreateBikeshedJobAPI(C.uint32_t(workerCount), C.int(workerPriority))}
}

// Longtail_ProgressAPI.Dispose() ...
func (progressAPI *Longtail_ProgressAPI) Dispose() {
	if progressAPI.cProgressAPI != nil {
		C.Longtail_DisposeAPI(&progressAPI.cProgressAPI.m_API)
		progressAPI.cProgressAPI = nil
	}
}

// Longtail_JobAPI.Dispose() ...
func (jobAPI *Longtail_JobAPI) Dispose() {
	if jobAPI.cJobAPI != nil {
		C.Longtail_DisposeAPI(&jobAPI.cJobAPI.m_API)
		jobAPI.cJobAPI = nil
	}
}

// CreateFullCompressionRegistry ...
func CreateFullCompressionRegistry() Longtail_CompressionRegistryAPI {
	return Longtail_CompressionRegistryAPI{cCompressionRegistryAPI: C.Longtail_CreateFullCompressionRegistry()}
}

// CreateZStdCompressionRegistry ...
func CreateZStdCompressionRegistry() Longtail_CompressionRegistryAPI {
	return Longtail_CompressionRegistryAPI{cCompressionRegistryAPI: C.Longtail_CreateZStdCompressionRegistry()}
}

// Longtail_CompressionRegistryAPI ...
func (compressionRegistry *Longtail_CompressionRegistryAPI) Dispose() {
	if compressionRegistry.cCompressionRegistryAPI != nil {
		C.Longtail_DisposeAPI(&compressionRegistry.cCompressionRegistryAPI.m_API)
		compressionRegistry.cCompressionRegistryAPI = nil
	}
}

// GetNoCompressionType ...
func GetNoCompressionType() uint32 {
	return uint32(0)
}

// GetBrotliGenericMinCompressionType ...
func GetBrotliGenericMinCompressionType() uint32 {
	return uint32(C.Longtail_GetBrotliGenericMinQuality())
}

// GetBrotliGenericDefaultCompressionType ...
func GetBrotliGenericDefaultCompressionType() uint32 {
	return uint32(C.Longtail_GetBrotliGenericDefaultQuality())
}

// GetBrotliGenericMaxCompressionType ...
func GetBrotliGenericMaxCompressionType() uint32 {
	return uint32(C.Longtail_GetBrotliGenericMaxQuality())
}

// GetBrotliTextMinCompressionType ...
func GetBrotliTextMinCompressionType() uint32 {
	return uint32(C.Longtail_GetBrotliTextMinQuality())
}

// GetBrotliTextDefaultCompressionType ...
func GetBrotliTextDefaultCompressionType() uint32 {
	return uint32(C.Longtail_GetBrotliTextDefaultQuality())
}

// GetBrotliTextMaxCompressionType ...
func GetBrotliTextMaxCompressionType() uint32 {
	return uint32(C.Longtail_GetBrotliTextMaxQuality())
}

// GetLZ4DefaultCompressionType ...
func GetLZ4DefaultCompressionType() uint32 {
	return uint32(C.Longtail_GetLZ4DefaultQuality())
}

// GetZStdMinCompressionType ...
func GetZStdMinCompressionType() uint32 {
	return uint32(C.Longtail_GetZStdMinQuality())
}

// GetZStdDefaultCompressionType ...
func GetZStdDefaultCompressionType() uint32 {
	return uint32(C.Longtail_GetZStdDefaultQuality())
}

// GetZStdMaxCompressionType ...
func GetZStdMaxCompressionType() uint32 {
	return uint32(C.Longtail_GetZStdMaxQuality())
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
func GetFilesRecursively(storageAPI Longtail_StorageAPI, pathFilter Longtail_PathFilterAPI, rootPath string) (Longtail_FileInfos, int) {
	cFolderPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cFolderPath))
	var fileInfos *C.struct_Longtail_FileInfos
	errno := C.Longtail_GetFilesRecursively(storageAPI.cStorageAPI, pathFilter.cPathFilterAPI, nil, nil, cFolderPath, &fileInfos)
	if errno != 0 {
		return Longtail_FileInfos{cFileInfos: nil}, int(errno)
	}
	return Longtail_FileInfos{cFileInfos: fileInfos}, 0
}

// GetPathCount ...
func (fileInfos *Longtail_FileInfos) GetPathCount() uint32 {
	return uint32(fileInfos.cFileInfos.m_Count)
}

// GetPath ...
func (fileInfos Longtail_FileInfos) GetPath(index uint32) string {
	cPath := C.Longtail_FileInfos_GetPath(fileInfos.cFileInfos, C.uint32_t(index))
	return C.GoString(cPath)
}

// WriteBlockIndexToBuffer ...
func WriteBlockIndexToBuffer(index Longtail_BlockIndex) ([]byte, int) {
	var buffer unsafe.Pointer
	size := C.size_t(0)
	errno := C.Longtail_WriteBlockIndexToBuffer(index.cBlockIndex, &buffer, &size)
	if errno != 0 {
		return nil, int(errno)
	}
	defer C.Longtail_Free(buffer)
	bytes := C.GoBytes(buffer, C.int(size))
	return bytes, 0
}

// ReadBlockIndexFromBuffer ...
func ReadBlockIndexFromBuffer(buffer []byte) (Longtail_BlockIndex, int) {
	cBuffer := unsafe.Pointer(&buffer[0])
	cSize := C.size_t(len(buffer))
	var bindex *C.struct_Longtail_BlockIndex
	errno := C.Longtail_ReadBlockIndexFromBuffer(cBuffer, cSize, &bindex)
	if errno != 0 {
		return Longtail_BlockIndex{cBlockIndex: nil}, int(errno)
	}
	return Longtail_BlockIndex{cBlockIndex: bindex}, 0
}

// CreateVersionIndex ...
func CreateVersionIndex(
	storageAPI Longtail_StorageAPI,
	hashAPI Longtail_HashAPI,
	jobAPI Longtail_JobAPI,
	progressAPI *Longtail_ProgressAPI,
	rootPath string,
	fileInfos Longtail_FileInfos,
	assetCompressionTypes []uint32,
	maxChunkSize uint32) (Longtail_VersionIndex, int) {

	var cProgressAPI *C.struct_Longtail_ProgressAPI
	if progressAPI != nil {
		cProgressAPI = progressAPI.cProgressAPI
	}

	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))

	cCompressionTypes := unsafe.Pointer(nil)
	if len(assetCompressionTypes) > 0 {
		cCompressionTypes = unsafe.Pointer(&assetCompressionTypes[0])
	}

	var vindex *C.struct_Longtail_VersionIndex
	errno := C.Longtail_CreateVersionIndex(
		storageAPI.cStorageAPI,
		hashAPI.cHashAPI,
		jobAPI.cJobAPI,
		cProgressAPI,
		nil,
		nil,
		cRootPath,
		fileInfos.cFileInfos,
		(*C.uint32_t)(cCompressionTypes),
		C.uint32_t(maxChunkSize),
		&vindex)

	if errno != 0 {
		return Longtail_VersionIndex{cVersionIndex: nil}, int(errno)
	}

	return Longtail_VersionIndex{cVersionIndex: vindex}, 0
}

// WriteVersionIndexToBuffer ...
func WriteVersionIndexToBuffer(index Longtail_VersionIndex) ([]byte, int) {
	var buffer unsafe.Pointer
	size := C.size_t(0)
	errno := C.Longtail_WriteVersionIndexToBuffer(index.cVersionIndex, &buffer, &size)
	if errno != 0 {
		return nil, int(errno)
	}
	defer C.Longtail_Free(buffer)
	bytes := C.GoBytes(buffer, C.int(size))
	return bytes, 0
}

// WriteVersionIndex ...
func WriteVersionIndex(storageAPI Longtail_StorageAPI, index Longtail_VersionIndex, path string) int {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	errno := C.Longtail_WriteVersionIndex(storageAPI.cStorageAPI, index.cVersionIndex, cPath)
	if errno != 0 {
		return int(errno)
	}
	return 0
}

// ReadVersionIndexFromBuffer ...
func ReadVersionIndexFromBuffer(buffer []byte) (Longtail_VersionIndex, int) {
	cBuffer := unsafe.Pointer(&buffer[0])
	cSize := C.size_t(len(buffer))
	var vindex *C.struct_Longtail_VersionIndex
	errno := C.Longtail_ReadVersionIndexFromBuffer(cBuffer, cSize, &vindex)
	if errno != 0 {
		return Longtail_VersionIndex{cVersionIndex: nil}, int(errno)
	}
	return Longtail_VersionIndex{cVersionIndex: vindex}, 0
}

// ReadVersionIndex ...
func ReadVersionIndex(storageAPI Longtail_StorageAPI, path string) (Longtail_VersionIndex, int) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var vindex *C.struct_Longtail_VersionIndex
	errno := C.Longtail_ReadVersionIndex(storageAPI.cStorageAPI, cPath, &vindex)
	if errno != 0 {
		return Longtail_VersionIndex{cVersionIndex: nil}, int(errno)
	}
	return Longtail_VersionIndex{cVersionIndex: vindex}, 0
}

// CreateContentIndex
func CreateContentIndex(
	hashAPI Longtail_HashAPI,
	versionIndex Longtail_VersionIndex,
	maxBlockSize uint32,
	maxChunksPerBlock uint32) (Longtail_ContentIndex, int) {

	var cindex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_CreateContentIndex(
		hashAPI.cHashAPI,
		versionIndex.cVersionIndex,
		C.uint32_t(maxBlockSize),
		C.uint32_t(maxChunksPerBlock),
		&cindex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, int(errno)
	}

	return Longtail_ContentIndex{cContentIndex: cindex}, 0
}

// CreateContentIndexRaw ...
func CreateContentIndexRaw(
	hashAPI Longtail_HashAPI,
	chunkHashes []uint64,
	chunkSizes []uint32,
	compressionTypes []uint32,
	maxBlockSize uint32,
	maxChunksPerBlock uint32) (Longtail_ContentIndex, int) {

	chunkCount := uint64(len(chunkHashes))
	var cindex *C.struct_Longtail_ContentIndex
	if chunkCount == 0 {
		errno := C.Longtail_CreateContentIndexRaw(
			hashAPI.cHashAPI,
			0,
			nil,
			nil,
			nil,
			C.uint32_t(maxBlockSize),
			C.uint32_t(maxChunksPerBlock),
			&cindex)
		if errno != 0 {
			return Longtail_ContentIndex{cContentIndex: nil}, int(errno)
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

	errno := C.Longtail_CreateContentIndexRaw(
		hashAPI.cHashAPI,
		C.uint64_t(chunkCount),
		cChunkHashes,
		cChunkSizes,
		cCompressionTypes,
		C.uint32_t(maxBlockSize),
		C.uint32_t(maxChunksPerBlock),
		&cindex)

	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, int(errno)
	}

	return Longtail_ContentIndex{cContentIndex: cindex}, 0
}

func CreateContentIndexFromBlocks(
	maxBlockSize uint32,
	maxChunksPerBlock uint32,
	blockIndexes []Longtail_BlockIndex) (Longtail_ContentIndex, int) {
	rawBlockIndexes := make([]*C.struct_Longtail_BlockIndex, len(blockIndexes))
	blockCount := len(blockIndexes)
	for index, blockIndex := range blockIndexes {
		rawBlockIndexes[index] = blockIndex.cBlockIndex
	}
	var cBlockIndexes unsafe.Pointer
	if blockCount > 0 {
		cBlockIndexes = unsafe.Pointer(&rawBlockIndexes[0])
	}

	var cindex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_CreateContentIndexFromBlocks(
		C.uint32_t(maxBlockSize),
		C.uint32_t(maxChunksPerBlock),
		C.uint64_t(blockCount),
		(**C.struct_Longtail_BlockIndex)(cBlockIndexes),
		&cindex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, int(errno)
	}
	return Longtail_ContentIndex{cContentIndex: cindex}, 0
}

// WriteContentIndexToBuffer ...
func WriteContentIndexToBuffer(index Longtail_ContentIndex) ([]byte, int) {
	var buffer unsafe.Pointer
	size := C.size_t(0)
	errno := C.Longtail_WriteContentIndexToBuffer(index.cContentIndex, &buffer, &size)
	if errno != 0 {
		return nil, int(errno)
	}
	defer C.Longtail_Free(buffer)
	bytes := C.GoBytes(buffer, C.int(size))
	return bytes, 0
}

// WriteContentIndex ...
func WriteContentIndex(storageAPI Longtail_StorageAPI, index Longtail_ContentIndex, path string) int {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	errno := C.Longtail_WriteContentIndex(storageAPI.cStorageAPI, index.cContentIndex, cPath)
	if errno != 0 {
		return int(errno)
	}
	return 0
}

// ReadContentIndexFromBuffer ...
func ReadContentIndexFromBuffer(buffer []byte) (Longtail_ContentIndex, int) {
	cBuffer := unsafe.Pointer(&buffer[0])
	cSize := C.size_t(len(buffer))
	var cindex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_ReadContentIndexFromBuffer(cBuffer, cSize, &cindex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, int(errno)
	}
	return Longtail_ContentIndex{cContentIndex: cindex}, 0
}

// ReadContentIndex ...
func ReadContentIndex(storageAPI Longtail_StorageAPI, path string) (Longtail_ContentIndex, int) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var cindex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_ReadContentIndex(storageAPI.cStorageAPI, cPath, &cindex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, int(errno)
	}
	return Longtail_ContentIndex{cContentIndex: cindex}, 0
}

// CreateProgressAPI ...
func CreateProgressAPI(progress ProgressAPI) Longtail_ProgressAPI {
	cContext := SavePointer(progress)
	progressAPIProxy := C.CreateProgressProxyAPI(cContext)
	return Longtail_ProgressAPI{cProgressAPI: progressAPIProxy}
}

//export ProgressAPIProxy_OnProgress
func ProgressAPIProxy_OnProgress(progress_api *C.struct_Longtail_ProgressAPI, total_count C.uint32_t, done_count C.uint32_t) {
	context := C.ProgressAPIProxy_GetContext(unsafe.Pointer(progress_api))
	progress := RestorePointer(context).(ProgressAPI)
	progress.OnProgress(uint32(total_count), uint32(done_count))
}

//export ProgressAPIProxy_Dispose
func ProgressAPIProxy_Dispose(api *C.struct_Longtail_API) {
	context := C.ProgressAPIProxy_GetContext(unsafe.Pointer(api))
	UnrefPointer(context)
	C.Longtail_Free(unsafe.Pointer(api))
}

// CreatePathFilterAPI ...
func CreatePathFilterAPI(pathFilter PathFilterAPI) Longtail_PathFilterAPI {
	cContext := SavePointer(pathFilter)
	pathFilterAPIProxy := C.CreatePathFilterProxyAPI(cContext)
	return Longtail_PathFilterAPI{cPathFilterAPI: pathFilterAPIProxy}
}

//export PathFilterAPIProxy_Include
func PathFilterAPIProxy_Include(path_filter_api *C.struct_Longtail_PathFilterAPI, root_path *C.char, asset_path *C.char, asset_name *C.char, is_dir C.int, size C.uint64_t, permissions C.uint16_t) C.int {
	context := C.PathFilterAPIProxy_GetContext(unsafe.Pointer(path_filter_api))
	pathFilter := RestorePointer(context).(PathFilterAPI)
	isDir := is_dir != 0
	if pathFilter.Include(C.GoString(root_path), C.GoString(asset_path), C.GoString(asset_name), isDir, uint64(size), uint16(permissions)) {
		return 1
	}
	return 0
}

//export PathFilterAPIProxy_Dispose
func PathFilterAPIProxy_Dispose(api *C.struct_Longtail_API) {
	context := C.PathFilterAPIProxy_GetContext(unsafe.Pointer(api))
	UnrefPointer(context)
	C.Longtail_Free(unsafe.Pointer(api))
}

// CreateAsyncPutStoredBlockAPI ...
func CreateAsyncPutStoredBlockAPI(asyncComplete AsyncPutStoredBlockAPI) Longtail_AsyncPutStoredBlockAPI {
	cContext := SavePointer(asyncComplete)
	asyncCompleteAPIProxy := C.CreateAsyncPutStoredBlockAPI(cContext)
	return Longtail_AsyncPutStoredBlockAPI{cAsyncCompleteAPI: asyncCompleteAPIProxy}
}

//export AsyncPutStoredBlockAPIProxy_OnComplete
func AsyncPutStoredBlockAPIProxy_OnComplete(async_complete_api *C.struct_Longtail_AsyncPutStoredBlockAPI, errno C.int) {
	context := C.AsyncPutStoredBlockAPIProxy_GetContext(unsafe.Pointer(async_complete_api))
	asyncComplete := RestorePointer(context).(AsyncPutStoredBlockAPI)
	asyncComplete.OnComplete(int(errno))
}

//export AsyncPutStoredBlockAPIProxy_Dispose
func AsyncPutStoredBlockAPIProxy_Dispose(api *C.struct_Longtail_API) {
	context := C.AsyncPutStoredBlockAPIProxy_GetContext(unsafe.Pointer(api))
	UnrefPointer(context)
	C.Longtail_Free(unsafe.Pointer(api))
}

// CreateAsyncGetStoredBlockAPI ...
func CreateAsyncGetStoredBlockAPI(asyncComplete AsyncGetStoredBlockAPI) Longtail_AsyncGetStoredBlockAPI {
	cContext := SavePointer(asyncComplete)
	asyncCompleteAPIProxy := C.CreateAsyncGetStoredBlockAPI(cContext)
	return Longtail_AsyncGetStoredBlockAPI{cAsyncCompleteAPI: asyncCompleteAPIProxy}
}

//export AsyncGetStoredBlockAPIProxy_OnComplete
func AsyncGetStoredBlockAPIProxy_OnComplete(async_complete_api *C.struct_Longtail_AsyncGetStoredBlockAPI, stored_block *C.struct_Longtail_StoredBlock, errno C.int) {
	context := C.AsyncGetStoredBlockAPIProxy_GetContext(unsafe.Pointer(async_complete_api))
	asyncComplete := RestorePointer(context).(AsyncGetStoredBlockAPI)
	asyncComplete.OnComplete(Longtail_StoredBlock{cStoredBlock: stored_block}, int(errno))
}

//export AsyncGetStoredBlockAPIProxy_Dispose
func AsyncGetStoredBlockAPIProxy_Dispose(api *C.struct_Longtail_API) {
	context := C.AsyncGetStoredBlockAPIProxy_GetContext(unsafe.Pointer(api))
	UnrefPointer(context)
	C.Longtail_Free(unsafe.Pointer(api))
}

// CreateAsyncGetIndexAPI ...
func CreateAsyncGetIndexAPI(asyncComplete AsyncGetIndexAPI) Longtail_AsyncGetIndexAPI {
	cContext := SavePointer(asyncComplete)
	asyncCompleteAPIProxy := C.CreateAsyncGetIndexAPI(cContext)
	return Longtail_AsyncGetIndexAPI{cAsyncCompleteAPI: asyncCompleteAPIProxy}
}

//export AsyncGetIndexAPIProxy_OnComplete
func AsyncGetIndexAPIProxy_OnComplete(async_complete_api *C.struct_Longtail_AsyncGetIndexAPI, content_index *C.struct_Longtail_ContentIndex, errno C.int) {
	context := C.AsyncGetIndexAPIProxy_GetContext(unsafe.Pointer(async_complete_api))
	asyncComplete := RestorePointer(context).(AsyncGetIndexAPI)
	asyncComplete.OnComplete(Longtail_ContentIndex{cContentIndex: content_index}, int(errno))
}

//export AsyncGetIndexAPIProxy_Dispose
func AsyncGetIndexAPIProxy_Dispose(api *C.struct_Longtail_API) {
	context := C.AsyncGetIndexAPIProxy_GetContext(unsafe.Pointer(api))
	UnrefPointer(context)
	C.Longtail_Free(unsafe.Pointer(api))
}

// CreateAsyncRetargetContentAPI ...
func CreateAsyncRetargetContentAPI(asyncComplete AsyncRetargetContentAPI) Longtail_AsyncRetargetContentAPI {
	cContext := SavePointer(asyncComplete)
	asyncCompleteAPIProxy := C.CreateAsyncRetargetContentAPI(cContext)
	return Longtail_AsyncRetargetContentAPI{cAsyncCompleteAPI: asyncCompleteAPIProxy}
}

//export AsyncRetargetContentAPIProxy_OnComplete
func AsyncRetargetContentAPIProxy_OnComplete(async_complete_api *C.struct_Longtail_AsyncRetargetContentAPI, content_index *C.struct_Longtail_ContentIndex, errno C.int) {
	context := C.AsyncRetargetContentAPIProxy_GetContext(unsafe.Pointer(async_complete_api))
	asyncComplete := RestorePointer(context).(AsyncRetargetContentAPI)
	asyncComplete.OnComplete(Longtail_ContentIndex{cContentIndex: content_index}, int(errno))
}

//export AsyncRetargetContentAPIProxy_Dispose
func AsyncRetargetContentAPIProxy_Dispose(api *C.struct_Longtail_API) {
	context := C.AsyncRetargetContentAPIProxy_GetContext(unsafe.Pointer(api))
	UnrefPointer(context)
	C.Longtail_Free(unsafe.Pointer(api))
}

// WriteContent ...
func WriteContent(
	sourceStorageAPI Longtail_StorageAPI,
	targetBlockStoreAPI Longtail_BlockStoreAPI,
	jobAPI Longtail_JobAPI,
	progressAPI *Longtail_ProgressAPI,
	block_store_content_index Longtail_ContentIndex,
	versionContentIndex Longtail_ContentIndex,
	versionIndex Longtail_VersionIndex,
	versionFolderPath string) int {

	var cProgressAPI *C.struct_Longtail_ProgressAPI
	if progressAPI != nil {
		cProgressAPI = progressAPI.cProgressAPI
	}

	cVersionFolderPath := C.CString(versionFolderPath)
	defer C.free(unsafe.Pointer(cVersionFolderPath))

	errno := C.Longtail_WriteContent(
		sourceStorageAPI.cStorageAPI,
		targetBlockStoreAPI.cBlockStoreAPI,
		jobAPI.cJobAPI,
		cProgressAPI,
		nil,
		nil,
		block_store_content_index.cContentIndex,
		versionContentIndex.cContentIndex,
		versionIndex.cVersionIndex,
		cVersionFolderPath)
	if errno != 0 {
		return int(errno)
	}
	return 0
}

// CreateMissingContent ...
func CreateMissingContent(
	hashAPI Longtail_HashAPI,
	contentIndex Longtail_ContentIndex,
	versionIndex Longtail_VersionIndex,
	maxBlockSize uint32,
	maxChunksPerBlock uint32) (Longtail_ContentIndex, int) {

	var missingContentIndex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_CreateMissingContent(
		hashAPI.cHashAPI,
		contentIndex.cContentIndex,
		versionIndex.cVersionIndex,
		C.uint32_t(maxBlockSize),
		C.uint32_t(maxChunksPerBlock),
		&missingContentIndex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, int(errno)
	}
	return Longtail_ContentIndex{cContentIndex: missingContentIndex}, 0
}

// RetargetContent ...
func RetargetContent(
	referenceContentIndex Longtail_ContentIndex,
	contentIndex Longtail_ContentIndex) (Longtail_ContentIndex, int) {
	var retargetedContentIndex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_RetargetContent(referenceContentIndex.cContentIndex, contentIndex.cContentIndex, &retargetedContentIndex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, int(errno)
	}
	return Longtail_ContentIndex{cContentIndex: retargetedContentIndex}, 0
}

// MergeContentIndex ...
func MergeContentIndex(
	localContentIndex Longtail_ContentIndex,
	remoteContentIndex Longtail_ContentIndex) (Longtail_ContentIndex, int) {
	var mergedContentIndex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_MergeContentIndex(localContentIndex.cContentIndex, remoteContentIndex.cContentIndex, &mergedContentIndex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, int(errno)
	}
	return Longtail_ContentIndex{cContentIndex: mergedContentIndex}, 0
}

// AddContentIndex ...
func AddContentIndex(
	localContentIndex Longtail_ContentIndex,
	remoteContentIndex Longtail_ContentIndex) (Longtail_ContentIndex, int) {
	var mergedContentIndex *C.struct_Longtail_ContentIndex
	errno := C.Longtail_AddContentIndex(localContentIndex.cContentIndex, remoteContentIndex.cContentIndex, &mergedContentIndex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, int(errno)
	}
	return Longtail_ContentIndex{cContentIndex: mergedContentIndex}, 0
}

// WriteVersion ...
func WriteVersion(
	contentBlockStoreAPI Longtail_BlockStoreAPI,
	versionStorageAPI Longtail_StorageAPI,
	jobAPI Longtail_JobAPI,
	progressAPI *Longtail_ProgressAPI,
	contentIndex Longtail_ContentIndex,
	versionIndex Longtail_VersionIndex,
	versionFolderPath string,
	retainPermissions bool) int {

	var cProgressAPI *C.struct_Longtail_ProgressAPI
	if progressAPI != nil {
		cProgressAPI = progressAPI.cProgressAPI
	}

	cVersionFolderPath := C.CString(versionFolderPath)
	defer C.free(unsafe.Pointer(cVersionFolderPath))

	cRetainPermissions := C.int(0)
	if retainPermissions {
		cRetainPermissions = C.int(1)
	}

	errno := C.Longtail_WriteVersion(
		contentBlockStoreAPI.cBlockStoreAPI,
		versionStorageAPI.cStorageAPI,
		jobAPI.cJobAPI,
		cProgressAPI,
		nil,
		nil,
		contentIndex.cContentIndex,
		versionIndex.cVersionIndex,
		cVersionFolderPath,
		cRetainPermissions)
	if errno != 0 {
		return int(errno)
	}
	return 0
}

//CreateVersionDiff do we really need this? Maybe ChangeVersion should create one on the fly?
func CreateVersionDiff(
	sourceVersionIndex Longtail_VersionIndex,
	targetVersionIndex Longtail_VersionIndex) (Longtail_VersionDiff, int) {
	var versionDiff *C.struct_Longtail_VersionDiff
	errno := C.Longtail_CreateVersionDiff(sourceVersionIndex.cVersionIndex, targetVersionIndex.cVersionIndex, &versionDiff)
	if errno != 0 {
		return Longtail_VersionDiff{cVersionDiff: nil}, int(errno)
	}
	return Longtail_VersionDiff{cVersionDiff: versionDiff}, 0
}

//ChangeVersion ...
func ChangeVersion(
	contentBlockStoreAPI Longtail_BlockStoreAPI,
	versionStorageAPI Longtail_StorageAPI,
	hashAPI Longtail_HashAPI,
	jobAPI Longtail_JobAPI,
	progressAPI *Longtail_ProgressAPI,
	contentIndex Longtail_ContentIndex,
	sourceVersionIndex Longtail_VersionIndex,
	targetVersionIndex Longtail_VersionIndex,
	versionDiff Longtail_VersionDiff,
	versionFolderPath string,
	retainPermissions bool) int {

	var cProgressAPI *C.struct_Longtail_ProgressAPI
	if progressAPI != nil {
		cProgressAPI = progressAPI.cProgressAPI
	}

	cVersionFolderPath := C.CString(versionFolderPath)
	defer C.free(unsafe.Pointer(cVersionFolderPath))

	cRetainPermissions := C.int(0)
	if retainPermissions {
		cRetainPermissions = C.int(1)
	}

	errno := C.Longtail_ChangeVersion(
		contentBlockStoreAPI.cBlockStoreAPI,
		versionStorageAPI.cStorageAPI,
		hashAPI.cHashAPI,
		jobAPI.cJobAPI,
		cProgressAPI,
		nil,
		nil,
		contentIndex.cContentIndex,
		sourceVersionIndex.cVersionIndex,
		targetVersionIndex.cVersionIndex,
		versionDiff.cVersionDiff,
		cVersionFolderPath,
		cRetainPermissions)
	if errno != 0 {
		return int(errno)
	}
	return 0
}

//export LogProxy_Log
func LogProxy_Log(context unsafe.Pointer, level C.int, log *C.char) {
	logger := RestorePointer(context).(Logger)
	logger.OnLog(int(level), C.GoString(log))
}

//export BlockStoreAPIProxy_Dispose
func BlockStoreAPIProxy_Dispose(api *C.struct_Longtail_API) {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	blockStore.Close()
	UnrefPointer(context)
	C.Longtail_Free(unsafe.Pointer(api))
}

//export BlockStoreAPIProxy_PutStoredBlock
func BlockStoreAPIProxy_PutStoredBlock(api *C.struct_Longtail_BlockStoreAPI, storedBlock *C.struct_Longtail_StoredBlock, async_complete_api *C.struct_Longtail_AsyncPutStoredBlockAPI) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	errno := blockStore.PutStoredBlock(Longtail_StoredBlock{cStoredBlock: storedBlock}, Longtail_AsyncPutStoredBlockAPI{cAsyncCompleteAPI: async_complete_api})
	return C.int(errno)
}

//export BlockStoreAPIProxy_GetStoredBlock
func BlockStoreAPIProxy_GetStoredBlock(api *C.struct_Longtail_BlockStoreAPI, blockHash C.uint64_t, async_complete_api *C.struct_Longtail_AsyncGetStoredBlockAPI) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	errno := blockStore.GetStoredBlock(uint64(blockHash), Longtail_AsyncGetStoredBlockAPI{cAsyncCompleteAPI: async_complete_api})
	return C.int(errno)
}

//export BlockStoreAPIProxy_PreflightGet
func BlockStoreAPIProxy_PreflightGet(api *C.struct_Longtail_BlockStoreAPI, blockCount C.uint64_t, blockHashes *C.uint64_t, blockRefCounts *C.uint32_t) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)

	size := int(blockCount)
	hashes := carray2slice64(blockHashes, size)
	refCounts := carray2slice32(blockRefCounts, size)

	errno := blockStore.PreflightGet(uint64(blockCount), hashes, refCounts)
	return C.int(errno)
}

//export BlockStoreAPIProxy_GetIndex
func BlockStoreAPIProxy_GetIndex(api *C.struct_Longtail_BlockStoreAPI, async_complete_api *C.struct_Longtail_AsyncGetIndexAPI) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	errno := blockStore.GetIndex(
		Longtail_AsyncGetIndexAPI{cAsyncCompleteAPI: async_complete_api})
	return C.int(errno)
}

//export BlockStoreAPIProxy_RetargetContent
func BlockStoreAPIProxy_RetargetContent(api *C.struct_Longtail_BlockStoreAPI, content_index *C.struct_Longtail_ContentIndex, async_complete_api *C.struct_Longtail_AsyncRetargetContentAPI) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	errno := blockStore.RetargetContent(
		Longtail_ContentIndex{cContentIndex: content_index},
		Longtail_AsyncRetargetContentAPI{cAsyncCompleteAPI: async_complete_api})
	return C.int(errno)
}

//export BlockStoreAPIProxy_GetStats
func BlockStoreAPIProxy_GetStats(api *C.struct_Longtail_BlockStoreAPI, out_stats *C.struct_Longtail_BlockStore_Stats) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	stats, errno := blockStore.GetStats()
	if errno == 0 {
		out_stats.m_IndexGetCount = C.uint64_t(stats.IndexGetCount)
		out_stats.m_BlocksGetCount = C.uint64_t(stats.BlocksGetCount)
		out_stats.m_BlocksPutCount = C.uint64_t(stats.BlocksPutCount)
		out_stats.m_ChunksGetCount = C.uint64_t(stats.ChunksGetCount)
		out_stats.m_ChunksPutCount = C.uint64_t(stats.ChunksPutCount)
		out_stats.m_BytesGetCount = C.uint64_t(stats.BytesGetCount)
		out_stats.m_BytesPutCount = C.uint64_t(stats.BytesPutCount)
		out_stats.m_IndexGetRetryCount = C.uint64_t(stats.IndexGetRetryCount)
		out_stats.m_BlockGetRetryCount = C.uint64_t(stats.BlockGetRetryCount)
		out_stats.m_BlockPutRetryCount = C.uint64_t(stats.BlockPutRetryCount)
		out_stats.m_IndexGetFailCount = C.uint64_t(stats.IndexGetFailCount)
		out_stats.m_BlockGetFailCount = C.uint64_t(stats.BlockGetFailCount)
		out_stats.m_BlockPutFailCount = C.uint64_t(stats.BlockPutFailCount)
	}
	return C.int(errno)
}

func CreateBlockStoreAPI(blockStore BlockStoreAPI) Longtail_BlockStoreAPI {
	cContext := SavePointer(blockStore)
	blockStoreAPIProxy := C.CreateBlockStoreProxyAPI(cContext)
	return Longtail_BlockStoreAPI{cBlockStoreAPI: blockStoreAPIProxy}
}

func getLoggerFunc(logger Logger) C.Longtail_Log {
	if logger == nil {
		return nil
	}
	return C.Longtail_Log(C.LogProxy_Log)
}

//SetLogger ...
func SetLogger(logger Logger) {
	cLoggerContext := SavePointer(logger)
	C.Longtail_SetLog(getLoggerFunc(logger), cLoggerContext)
}

//SetLogLevel ...
func SetLogLevel(level int) {
	C.Longtail_SetLogLevel(C.int(level))
}

func getAssertFunc(assert Assert) C.Longtail_Assert {
	if assert == nil {
		return nil
	}
	return C.Longtail_Assert(C.AssertProxy_Assert)
}

var activeAssert Assert

//SetAssert ...
func SetAssert(assert Assert) {
	C.Longtail_SetAssert(getAssertFunc(assert))
	activeAssert = assert
}

//export AssertProxy_Assert
func AssertProxy_Assert(expression *C.char, file *C.char, line C.int) {
	if activeAssert != nil {
		activeAssert.OnAssert(C.GoString(expression), C.GoString(file), int(line))
	}
}
