package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -pthread -msse4.1 -maes -O3
// #include "golongtail.h"
import "C"
import (
	"fmt"
	"reflect"
	"sync/atomic"
	"unsafe"
)

const EPERM = 1    /* Not super-user */
const ENOENT = 2   /* No such file or directory */
const ESRCH = 3    /* No such process */
const EINTR = 4    /* Interrupted system call */
const EIO = 5      /* I/O error */
const ENXIO = 6    /* No such device or address */
const E2BIG = 7    /* Arg list too long */
const ENOEXEC = 8  /* Exec format error */
const EBADF = 9    /* Bad file number */
const ECHILD = 10  /* No children */
const EAGAIN = 11  /* No more processes */
const ENOMEM = 12  /* Not enough core */
const EACCES = 13  /* Permission denied */
const EFAULT = 14  /* Bad address */
const ENOTBLK = 15 /* Block device required */
const EBUSY = 16   /* Mount device busy */
const EEXIST = 17  /* File exists */
const EXDEV = 18   /* Cross-device link */
const ENODEV = 19  /* No such device */
const ENOTDIR = 20 /* Not a directory */
const EISDIR = 21  /* Is a directory */
const EINVAL = 22  /* Invalid argument */
const ENFILE = 23  /* Too many open files in system */
const EMFILE = 24  /* Too many open files */
const ENOTTY = 25  /* Not a typewriter */
const ETXTBSY = 26 /* Text file busy */
const EFBIG = 27   /* File too large */
const ENOSPC = 28  /* No space left on device */
const ESPIPE = 29  /* Illegal seek */
const EROFS = 30   /* Read only file system */
const EMLINK = 31  /* Too many links */
const EPIPE = 32   /* Broken pipe */
const EDOM = 33    /* Math arg out of domain of func */
const ERANGE = 34  /* Math result not representable */

type ProgressAPI interface {
	OnProgress(totalCount uint32, doneCount uint32)
}
type PathFilterAPI interface {
	Include(rootPath string, assetFolder string, assetName string, isDir bool, size uint64, permissions uint16) bool
}

type AsyncPutStoredBlockAPI interface {
	OnComplete(err int) int
}

type AsyncGetStoredBlockAPI interface {
	OnComplete(stored_block Longtail_StoredBlock, err int) int
}

type AsyncGetIndexAPI interface {
	OnComplete(content_index Longtail_ContentIndex, err int) int
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
	GetStoredBlock(blockHash uint64, asyncCompleteAPI Longtail_AsyncGetStoredBlockAPI) int
	GetIndex(defaultHashAPIIdentifier uint32, asyncCompleteAPI Longtail_AsyncGetIndexAPI) int
	GetStats() (BlockStoreStats, int)
	Close()
}

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
func (storageAPI *Longtail_StorageAPI) ReadFromStorage(rootPath string, path string) ([]byte, error) {
	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cFullPath := C.Longtail_Storage_ConcatPath(storageAPI.cStorageAPI, cRootPath, cPath)
	defer C.Longtail_Free(unsafe.Pointer(cFullPath))

	var cOpenFile C.Longtail_StorageAPI_HOpenFile
	errno := C.Longtail_Storage_OpenReadFile(storageAPI.cStorageAPI, cFullPath, &cOpenFile)
	if errno != 0 {
		return nil, fmt.Errorf("ReadFromStorage: Longtail_Storage_OpenReadFile(%s/%s) failed with error %d", rootPath, path, errno)
	}
	defer C.Longtail_Storage_CloseFile(storageAPI.cStorageAPI, cOpenFile)

	var cSize C.uint64_t
	errno = C.Longtail_Storage_GetSize(storageAPI.cStorageAPI, cOpenFile, &cSize)
	if errno != 0 {
		return nil, fmt.Errorf("ReadFromStorage: Longtail_Storage_GetSize(%s/%s) failed with error %d", rootPath, path, errno)
	}

	blockData := make([]byte, int(cSize))
	if cSize > 0 {
		errno = C.Longtail_Storage_Read(storageAPI.cStorageAPI, cOpenFile, 0, cSize, unsafe.Pointer(&blockData[0]))
		if errno != 0 {
			return nil, fmt.Errorf("ReadFromStorage: Longtail_Storage_Read(%s/%s) failed with error %d", rootPath, path, errno)
		}
	}
	return blockData, nil
}

// WriteToStorage ...
func (storageAPI *Longtail_StorageAPI) WriteToStorage(rootPath string, path string, blockData []byte) error {
	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cFullPath := C.Longtail_Storage_ConcatPath(storageAPI.cStorageAPI, cRootPath, cPath)
	defer C.Longtail_Free(unsafe.Pointer(cFullPath))

	errno := C.EnsureParentPathExists(storageAPI.cStorageAPI, cFullPath)
	if errno != 0 {
		return fmt.Errorf("WriteToStorage: C.EnsureParentPathExists(`%s/%s`) failed with error %d", rootPath, path, errno)
	}

	blockSize := C.uint64_t(len(blockData))

	var cOpenFile C.Longtail_StorageAPI_HOpenFile
	errno = C.Longtail_Storage_OpenWriteFile(storageAPI.cStorageAPI, cFullPath, blockSize, &cOpenFile)
	if errno != 0 {
		return fmt.Errorf("ReadFromStorage: Longtail_Storage_OpenWriteFile(%s/%s) failed with error %d", rootPath, path, errno)
	}
	defer C.Longtail_Storage_CloseFile(storageAPI.cStorageAPI, cOpenFile)

	if blockSize > 0 {
		data := unsafe.Pointer(&blockData[0])
		errno = C.Longtail_Storage_Write(storageAPI.cStorageAPI, cOpenFile, 0, blockSize, data)
	}

	if errno != 0 {
		return fmt.Errorf("WriteToStorage: C.Longtail_Storage_Write(`%s/%s`) failed with error %d", rootPath, path, errno)
	}
	return nil
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

func carray2sliceByte(array *C.char, len int) []byte {
	var list []byte
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
	return carray2slice64(fileInfos.cFileInfos.m_FileSizes, size)
}

func (fileInfos *Longtail_FileInfos) GetFilePermissions() []uint32 {
	size := int(*fileInfos.cFileInfos.m_Paths.m_PathCount)
	return carray2slice32(fileInfos.cFileInfos.m_Permissions, size)
}

func (fileInfos *Longtail_FileInfos) GetPaths() Longtail_Paths {
	return Longtail_Paths{cPaths: &fileInfos.cFileInfos.m_Paths}
}

func (contentIndex *Longtail_ContentIndex) IsValid() bool {
	return contentIndex.cContentIndex != nil
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

func (contentIndex *Longtail_ContentIndex) GetMaxBlockSize() uint32 {
	return uint32(*contentIndex.cContentIndex.m_MaxBlockSize)
}

func (contentIndex *Longtail_ContentIndex) GetMaxChunksPerBlock() uint32 {
	return uint32(*contentIndex.cContentIndex.m_MaxChunksPerBlock)
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
	C.Longtail_Free(unsafe.Pointer(versionIndex.cVersionIndex))
}

func (versionIndex *Longtail_VersionIndex) GetVersion() uint32 {
	return uint32(*versionIndex.cVersionIndex.m_Version)
}

func (versionIndex *Longtail_VersionIndex) GetHashAPI() uint32 {
	return uint32(*versionIndex.cVersionIndex.m_HashAPI)
}

func (versionIndex *Longtail_VersionIndex) GetTargetChunkSize() uint32 {
	return uint32(*versionIndex.cVersionIndex.m_TargetChunkSize)
}

func (versionIndex *Longtail_VersionIndex) GetAssetCount() uint32 {
	return uint32(*versionIndex.cVersionIndex.m_AssetCount)
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

func (versionIndex *Longtail_VersionIndex) GetChunkTags() []uint32 {
	size := int(*versionIndex.cVersionIndex.m_ChunkCount)
	return carray2slice32(versionIndex.cVersionIndex.m_ChunkTags, size)
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
func (asyncCompleteAPI *Longtail_AsyncPutStoredBlockAPI) OnComplete(errno int) int {
	return int(C.Longtail_AsyncPutStoredBlock_OnComplete(asyncCompleteAPI.cAsyncCompleteAPI, C.int(errno)))
}

//// Longtail_AsyncGetStoredBlockAPI::OnComplete() ...
func (asyncCompleteAPI *Longtail_AsyncGetStoredBlockAPI) OnComplete(stored_block Longtail_StoredBlock, errno int) int {
	return int(C.Longtail_AsyncGetStoredBlock_OnComplete(asyncCompleteAPI.cAsyncCompleteAPI, stored_block.cStoredBlock, C.int(errno)))
}

//// Longtail_AsyncGetIndexAPI::OnComplete() ...
func (asyncCompleteAPI *Longtail_AsyncGetIndexAPI) OnComplete(content_index Longtail_ContentIndex, errno int) int {
	return int(C.Longtail_AsyncGetIndex_OnComplete(asyncCompleteAPI.cAsyncCompleteAPI, content_index.cContentIndex, C.int(errno)))
}

// CreateFSBlockStore() ...
func CreateFSBlockStore(storageAPI Longtail_StorageAPI, contentPath string) Longtail_BlockStoreAPI {
	cContentPath := C.CString(contentPath)
	defer C.free(unsafe.Pointer(cContentPath))
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateFSBlockStoreAPI(storageAPI.cStorageAPI, cContentPath)}
}

// CreateCacheBlockStore() ...
func CreateCacheBlockStore(cacheBlockStore Longtail_BlockStoreAPI, persistentBlockStore Longtail_BlockStoreAPI) Longtail_BlockStoreAPI {
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateCacheBlockStoreAPI(cacheBlockStore.cBlockStoreAPI, persistentBlockStore.cBlockStoreAPI)}
}

// CreateCompressBlockStore() ...
func CreateCompressBlockStore(backingBlockStore Longtail_BlockStoreAPI, compressionRegistry Longtail_CompressionRegistryAPI) Longtail_BlockStoreAPI {
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateCompressBlockStoreAPI(backingBlockStore.cBlockStoreAPI, compressionRegistry.cCompressionRegistryAPI)}
}

// Longtail_BlockStoreAPI.Dispose() ...
func (blockStoreAPI *Longtail_BlockStoreAPI) Dispose() {
	C.Longtail_DisposeAPI(&blockStoreAPI.cBlockStoreAPI.m_API)
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
	defaultHashAPIIdentifier uint32,
	asyncCompleteAPI Longtail_AsyncGetIndexAPI) int {

	errno := C.Longtail_BlockStore_GetIndex(
		blockStoreAPI.cBlockStoreAPI,
		C.uint32_t(defaultHashAPIIdentifier),
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
	C.Longtail_StoredBlock_Dispose(storedBlock.cStoredBlock)
}

func (blockIndex *Longtail_BlockIndex) Dispose() {
	C.Longtail_Free(unsafe.Pointer(blockIndex.cBlockIndex))
}

func WriteStoredBlockToBuffer(storedBlock Longtail_StoredBlock) ([]byte, error) {
	var buffer unsafe.Pointer
	var size C.size_t
	errno := C.Longtail_WriteStoredBlockToBuffer(storedBlock.cStoredBlock, &buffer, &size)
	if errno != 0 {
		return nil, fmt.Errorf("WriteStoredBlockToBuffer: Longtail_WriteStoredBlockToBuffer failed with %d", errno)
	}
	defer C.Longtail_Free(buffer)
	bytes := C.GoBytes(buffer, C.int(size))
	return bytes, nil
}

func ReadStoredBlockFromBuffer(buffer []byte) (Longtail_StoredBlock, error) {
	cBuffer := unsafe.Pointer(&buffer[0])
	size := C.size_t(len(buffer))
	var stored_block *C.struct_Longtail_StoredBlock
	errno := C.Longtail_ReadStoredBlockFromBuffer(cBuffer, size, &stored_block)
	if errno != 0 {
		return Longtail_StoredBlock{cStoredBlock: nil}, fmt.Errorf("ReadStoredBlockFromBuffer: Longtail_ReadStoredBlockFromBuffer failed with %d", errno)
	}
	return Longtail_StoredBlock{cStoredBlock: stored_block}, nil
}

// CreateStoredBlock() ...
func CreateStoredBlock(
	blockHash uint64,
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
	C.Longtail_DisposeAPI(&storageAPI.cStorageAPI.m_API)
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
	C.Longtail_DisposeAPI(&compressionAPI.cCompressionAPI.m_API)
}

// CreateBikeshedJobAPI ...
func CreateBikeshedJobAPI(workerCount uint32) Longtail_JobAPI {
	return Longtail_JobAPI{cJobAPI: C.Longtail_CreateBikeshedJobAPI(C.uint32_t(workerCount))}
}

// Longtail_ProgressAPI.Dispose() ...
func (progressAPI *Longtail_ProgressAPI) Dispose() {
	C.Longtail_DisposeAPI(&progressAPI.cProgressAPI.m_API)
}

// Longtail_JobAPI.Dispose() ...
func (jobAPI *Longtail_JobAPI) Dispose() {
	C.Longtail_DisposeAPI(&jobAPI.cJobAPI.m_API)
}

// CreateFullCompressionRegistry ...
//func CreateFullCompressionRegistry() Longtail_CompressionRegistryAPI {
//	return Longtail_CompressionRegistryAPI{cCompressionRegistryAPI: C.Longtail_CreateFullCompressionRegistry()}
//}

// CreateZStdCompressionRegistry ...
func CreateZStdCompressionRegistry() Longtail_CompressionRegistryAPI {
	return Longtail_CompressionRegistryAPI{cCompressionRegistryAPI: C.Longtail_CreateZStdCompressionRegistry()}
}

// Longtail_CompressionRegistryAPI ...
func (compressionRegistry *Longtail_CompressionRegistryAPI) Dispose() {
	C.Longtail_DisposeAPI(&compressionRegistry.cCompressionRegistryAPI.m_API)
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
func GetFilesRecursively(storageAPI Longtail_StorageAPI, pathFilter Longtail_PathFilterAPI, rootPath string) (Longtail_FileInfos, error) {
	cFolderPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cFolderPath))
	var fileInfos *C.struct_Longtail_FileInfos
	errno := C.Longtail_GetFilesRecursively(storageAPI.cStorageAPI, pathFilter.cPathFilterAPI, cFolderPath, &fileInfos)
	if errno != 0 {
		return Longtail_FileInfos{cFileInfos: nil}, fmt.Errorf("GetFilesRecursively: C.Longtail_GetFilesRecursively(`%s`) failed with error %d", rootPath, errno)
	}
	return Longtail_FileInfos{cFileInfos: fileInfos}, nil
}

// GetPath ...
func (paths Longtail_Paths) GetPath(index uint32) string {
	cPath := C.Longtail_Paths_GetPath(paths.cPaths, C.uint32_t(index))
	return C.GoString(cPath)
}

// WriteBlockIndexToBuffer ...
func WriteBlockIndexToBuffer(index Longtail_BlockIndex) ([]byte, error) {
	var buffer unsafe.Pointer
	size := C.size_t(0)
	errno := C.Longtail_WriteBlockIndexToBuffer(index.cBlockIndex, &buffer, &size)
	if errno != 0 {
		return nil, fmt.Errorf("WriteBlockIndexToBuffer: C.Longtail_WriteBlockIndexToBuffer() failed with error %d", errno)
	}
	defer C.Longtail_Free(buffer)
	bytes := C.GoBytes(buffer, C.int(size))
	return bytes, nil
}

// ReadBlockIndexFromBuffer ...
func ReadBlockIndexFromBuffer(buffer []byte) (Longtail_BlockIndex, error) {
	cBuffer := unsafe.Pointer(&buffer[0])
	cSize := C.size_t(len(buffer))
	var bindex *C.struct_Longtail_BlockIndex
	errno := C.Longtail_ReadBlockIndexFromBuffer(cBuffer, cSize, &bindex)
	if errno != 0 {
		return Longtail_BlockIndex{cBlockIndex: nil}, fmt.Errorf("ReadBlockIndexFromBuffer: C.Longtail_ReadBlockIndexFromBuffer() failed with error %d", errno)
	}
	return Longtail_BlockIndex{cBlockIndex: bindex}, nil
}

// CreateVersionIndex ...
func CreateVersionIndex(
	storageAPI Longtail_StorageAPI,
	hashAPI Longtail_HashAPI,
	jobAPI Longtail_JobAPI,
	progressAPI *Longtail_ProgressAPI,
	rootPath string,
	paths Longtail_Paths,
	assetSizes []uint64,
	assetPermissions []uint32,
	assetCompressionTypes []uint32,
	maxChunkSize uint32) (Longtail_VersionIndex, error) {

	var cProgressAPI *C.struct_Longtail_ProgressAPI
	if progressAPI != nil {
		cProgressAPI = progressAPI.cProgressAPI
	}

	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))

	cAssetSizes := unsafe.Pointer(nil)
	if len(assetSizes) > 0 {
		cAssetSizes = unsafe.Pointer(&assetSizes[0])
	}

	cAssetPermissions := unsafe.Pointer(nil)
	if len(assetPermissions) > 0 {
		cAssetPermissions = unsafe.Pointer(&assetPermissions[0])
	}

	cCompressionTypes := unsafe.Pointer(nil)
	if len(assetCompressionTypes) > 0 {
		cCompressionTypes = unsafe.Pointer(&assetCompressionTypes[0])
	}

	var vindex *C.struct_Longtail_VersionIndex
	errno := C.Longtail_CreateVersionIndexRaw(
		storageAPI.cStorageAPI,
		hashAPI.cHashAPI,
		jobAPI.cJobAPI,
		cProgressAPI,
		cRootPath,
		paths.cPaths,
		(*C.uint64_t)(cAssetSizes),
		(*C.uint32_t)(cAssetPermissions),
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
	chunkHashes []uint64,
	chunkSizes []uint32,
	compressionTypes []uint32,
	maxBlockSize uint32,
	maxChunksPerBlock uint32) (Longtail_ContentIndex, error) {

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
		return Longtail_ContentIndex{cContentIndex: nil}, fmt.Errorf("CreateContentIndex: C.Longtail_CreateContentIndex(%d) create content index failed with error %d", chunkCount, errno)
	}

	return Longtail_ContentIndex{cContentIndex: cindex}, nil
}

func CreateContentIndexFromBlocks(
	hashIdentifier uint32,
	maxBlockSize uint32,
	maxChunksPerBlock uint32,
	blockIndexes []Longtail_BlockIndex) (Longtail_ContentIndex, error) {
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
		C.uint32_t(hashIdentifier),
		C.uint32_t(maxBlockSize),
		C.uint32_t(maxChunksPerBlock),
		C.uint64_t(blockCount),
		(**C.struct_Longtail_BlockIndex)(cBlockIndexes),
		&cindex)
	if errno != 0 {
		return Longtail_ContentIndex{cContentIndex: nil}, fmt.Errorf("CreateContentIndexFromBlocks: C.Longtail_CreateContentIndexFromBlocks(%d) create content index failed with error %d", blockCount, errno)
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
func PathFilterAPIProxy_Include(path_filter_api *C.struct_Longtail_PathFilterAPI, root_path *C.char, asset_folder *C.char, asset_name *C.char, is_dir C.int, size C.uint64_t, permissions C.uint16_t) C.int {
	context := C.PathFilterAPIProxy_GetContext(unsafe.Pointer(path_filter_api))
	pathFilter := RestorePointer(context).(PathFilterAPI)
	isDir := is_dir != 0
	if pathFilter.Include(C.GoString(root_path), C.GoString(asset_folder), C.GoString(asset_name), isDir, uint64(size), uint16(permissions)) {
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
func AsyncPutStoredBlockAPIProxy_OnComplete(async_complete_api *C.struct_Longtail_AsyncPutStoredBlockAPI, err C.int) C.int {
	context := C.AsyncPutStoredBlockAPIProxy_GetContext(unsafe.Pointer(async_complete_api))
	asyncComplete := RestorePointer(context).(AsyncPutStoredBlockAPI)
	return C.int(asyncComplete.OnComplete(int(err)))
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
func AsyncGetStoredBlockAPIProxy_OnComplete(async_complete_api *C.struct_Longtail_AsyncGetStoredBlockAPI, stored_block *C.struct_Longtail_StoredBlock, err C.int) C.int {
	context := C.AsyncGetStoredBlockAPIProxy_GetContext(unsafe.Pointer(async_complete_api))
	asyncComplete := RestorePointer(context).(AsyncGetStoredBlockAPI)
	return C.int(asyncComplete.OnComplete(Longtail_StoredBlock{cStoredBlock: stored_block}, int(err)))
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
func AsyncGetIndexAPIProxy_OnComplete(async_complete_api *C.struct_Longtail_AsyncGetIndexAPI, content_index *C.struct_Longtail_ContentIndex, err C.int) C.int {
	context := C.AsyncGetIndexAPIProxy_GetContext(unsafe.Pointer(async_complete_api))
	asyncComplete := RestorePointer(context).(AsyncGetIndexAPI)
	return C.int(asyncComplete.OnComplete(Longtail_ContentIndex{cContentIndex: content_index}, int(err)))
}

//export AsyncGetIndexAPIProxy_Dispose
func AsyncGetIndexAPIProxy_Dispose(api *C.struct_Longtail_API) {
	context := C.AsyncGetIndexAPIProxy_GetContext(unsafe.Pointer(api))
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
	versionFolderPath string) error {

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
		block_store_content_index.cContentIndex,
		versionContentIndex.cContentIndex,
		versionIndex.cVersionIndex,
		cVersionFolderPath)
	if errno != 0 {
		return fmt.Errorf("WriteContent: C.Longtail_WriteContent(`%s`) failed with error %d", versionFolderPath, errno)
	}
	return nil
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
	contentBlockStoreAPI Longtail_BlockStoreAPI,
	versionStorageAPI Longtail_StorageAPI,
	jobAPI Longtail_JobAPI,
	progressAPI *Longtail_ProgressAPI,
	contentIndex Longtail_ContentIndex,
	versionIndex Longtail_VersionIndex,
	versionFolderPath string,
	retainPermissions bool) error {

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
		contentIndex.cContentIndex,
		versionIndex.cVersionIndex,
		cVersionFolderPath,
		cRetainPermissions)
	if errno != 0 {
		return fmt.Errorf("WriteVersion: C.Longtail_WriteVersion(`%s`) failed with error %d", versionFolderPath, errno)
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
	retainPermissions bool) error {

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
		contentIndex.cContentIndex,
		sourceVersionIndex.cVersionIndex,
		targetVersionIndex.cVersionIndex,
		versionDiff.cVersionDiff,
		cVersionFolderPath,
		cRetainPermissions)
	if errno != 0 {
		return fmt.Errorf("ChangeVersion: C.Longtail_ChangeVersion(`%s`) failed with error %d", versionFolderPath, errno)
	}
	return nil
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

//export BlockStoreAPIProxy_GetIndex
func BlockStoreAPIProxy_GetIndex(api *C.struct_Longtail_BlockStoreAPI, defaultHashApiIdentifier uint32, async_complete_api *C.struct_Longtail_AsyncGetIndexAPI) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	errno := blockStore.GetIndex(
		uint32(defaultHashApiIdentifier),
		Longtail_AsyncGetIndexAPI{cAsyncCompleteAPI: async_complete_api})
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

func CreateFSBlockStoreAPI(storageAPI Longtail_StorageAPI, contentPath string) Longtail_BlockStoreAPI {
	cContentPath := C.CString(contentPath)
	defer C.free(unsafe.Pointer(cContentPath))
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateFSBlockStoreAPI(storageAPI.cStorageAPI, cContentPath)}
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
