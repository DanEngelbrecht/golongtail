package longtaillib

// #cgo CFLAGS: -g -std=gnu99 -m64 -msse4.1 -maes -pthread -O3
// #include "golongtail.h"
import "C"
import (
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

var errnoToDescription = map[int]string{
	C.E2BIG:           "Argument list too long.",
	C.EACCES:          "Permission denied.",
	C.EADDRINUSE:      "Address in use.",
	C.EADDRNOTAVAIL:   "Address not available.",
	C.EAFNOSUPPORT:    "Address family not supported.",
	C.EAGAIN:          "Resource unavailable, try again (may be the same value as [EWOULDBLOCK]).",
	C.EALREADY:        "Connection already in progress.",
	C.EBADF:           "Bad file descriptor.",
	C.EBADMSG:         "Bad message.",
	C.EBUSY:           "Device or resource busy.",
	C.ECANCELED:       "Operation canceled.",
	C.ECHILD:          "No child processes.",
	C.ECONNABORTED:    "Connection aborted.",
	C.ECONNREFUSED:    "Connection refused.",
	C.ECONNRESET:      "Connection reset.",
	C.EDEADLK:         "Resource deadlock would occur.",
	C.EDESTADDRREQ:    "Destination address required.",
	C.EDOM:            "Mathematics argument out of domain of function.",
	C.EEXIST:          "File exists.",
	C.EFAULT:          "Bad address.",
	C.EFBIG:           "File too large.",
	C.EHOSTUNREACH:    "Host is unreachable.",
	C.EIDRM:           "Identifier removed.",
	C.EILSEQ:          "Illegal byte sequence.",
	C.EINPROGRESS:     "Operation in progress.",
	C.EINTR:           "Interrupted function.",
	C.EINVAL:          "Invalid argument.",
	C.EIO:             "I/O error.",
	C.EISCONN:         "Socket is connected.",
	C.EISDIR:          "Is a directory.",
	C.ELOOP:           "Too many levels of symbolic links.",
	C.EMFILE:          "Too many open files.",
	C.EMLINK:          "Too many links.",
	C.EMSGSIZE:        "Message too large.",
	C.ENAMETOOLONG:    "Filename too long.",
	C.ENETDOWN:        "Network is down.",
	C.ENETRESET:       "Connection aborted by network.",
	C.ENETUNREACH:     "Network unreachable.",
	C.ENFILE:          "Too many files open in system.",
	C.ENOBUFS:         "No buffer space available.",
	C.ENODATA:         "[XSR] [Option Start] No message is available on the STREAM head read queue. [Option End]",
	C.ENODEV:          "No such device.",
	C.ENOENT:          "No such file or directory.",
	C.ENOEXEC:         "Executable file format error.",
	C.ENOLCK:          "No locks available.",
	C.ENOLINK:         "Link has been severed (POSIX.1-2001).",
	C.ENOMEM:          "Not enough space.",
	C.ENOMSG:          "No message of the desired type.",
	C.ENOPROTOOPT:     "Protocol not available.",
	C.ENOSPC:          "No space left on device.",
	C.ENOSR:           "[XSR] [Option Start] No STREAM resources. [Option End]",
	C.ENOSTR:          "[XSR] [Option Start] Not a STREAM. [Option End]",
	C.ENOSYS:          "Function not supported.",
	C.ENOTCONN:        "The socket is not connected.",
	C.ENOTDIR:         "Not a directory.",
	C.ENOTEMPTY:       "Directory not empty.",
	C.ENOTSOCK:        "Not a socket.",
	C.ENOTSUP:         "Not supported.",
	C.ENOTTY:          "Inappropriate I/O control operation.",
	C.ENXIO:           "No such device or address.",
	C.EOVERFLOW:       "Value too large to be stored in data type.",
	C.EPERM:           "Operation not permitted.",
	C.EPIPE:           "Broken pipe.",
	C.EPROTO:          "Protocol error.",
	C.EPROTONOSUPPORT: "Protocol not supported.",
	C.EPROTOTYPE:      "Protocol wrong type for socket.",
	C.ERANGE:          "Result too large.",
	C.EROFS:           "Read-only file system.",
	C.ESPIPE:          "Invalid seek.",
	C.ESRCH:           "No such process.",
	C.ETIME:           "[XSR] [Option Start] Stream ioctl() timeout. [Option End]",
	C.ETIMEDOUT:       "Connection timed out.",
	C.ETXTBSY:         "Text file busy.",
	C.EXDEV:           "Cross-device link. ",
}

type longtailError struct {
	Description string
	Errno       C.int
}

func (e *longtailError) Error() string {
	return fmt.Sprintf("%d: %s", e.Errno, e.Description)
}

func errnoToError(err C.int) error {
	if err == 0 {
		return nil
	}
	description := errnoToDescription[int(err)]
	return &longtailError{Errno: err, Description: description}
}

func errorToErrno(err error, fallback C.int) C.int {
	if err == nil {
		return 0
	}
	var longtailError *longtailError
	if errors.As(err, &longtailError) {
		return longtailError.Errno
	}
	if os.IsPermission(err) {
		return C.EPERM
	}
	if os.IsExist(err) {
		return C.ENOENT
	}
	if IsBadFormat(err) {
		return C.EBADF
	}
	return fallback
}

func IsNotExist(err error) bool {
	if err == nil {
		return false
	}
	var longtailError *longtailError
	if errors.As(err, &longtailError) {
		return longtailError.Errno == C.ENOENT
	}
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	return false
}

func NotExistErr() error {
	return errnoToError(C.ENOENT)
}

func IsBadFormat(err error) bool {
	if err == nil {
		return false
	}
	var longtailError *longtailError
	if errors.As(err, &longtailError) {
		return longtailError.Errno == C.EBADF
	}
	return false
}

func BadFormatErr() error {
	return errnoToError(C.EBADF)
}

func AccessViolationErr() error {
	return errnoToError(C.EACCES)
}

func InvalidArgumentError() error {
	return errnoToError(C.EINVAL)
}

type ProgressAPI interface {
	OnProgress(totalCount uint32, doneCount uint32)
}
type PathFilterAPI interface {
	Include(rootPath string, assetPath string, assetName string, isDir bool, size uint64, permissions uint16) bool
}

type AsyncPutStoredBlockAPI interface {
	OnComplete(err error)
}

type AsyncGetStoredBlockAPI interface {
	OnComplete(stored_block Longtail_StoredBlock, err error)
}

type AsyncGetExistingContentAPI interface {
	OnComplete(store_index Longtail_StoreIndex, err error)
}

type AsyncPruneBlocksAPI interface {
	OnComplete(prune_block_count uint32, err error)
}

type AsyncFlushAPI interface {
	OnComplete(err error)
}

type Assert interface {
	OnAssert(expression string, file string, line int)
}

type LogField struct {
	Name  string
	Value string
}

type Logger interface {
	OnLog(file string, function string, line int, level int, logFields []LogField, message string)
}

type Longtail_AsyncGetStoredBlockAPI struct {
	cAsyncCompleteAPI *C.struct_Longtail_AsyncGetStoredBlockAPI
}

type Longtail_AsyncPutStoredBlockAPI struct {
	cAsyncCompleteAPI *C.struct_Longtail_AsyncPutStoredBlockAPI
}

type Longtail_AsyncGetExistingContentAPI struct {
	cAsyncCompleteAPI *C.struct_Longtail_AsyncGetExistingContentAPI
}

type Longtail_AsyncPruneBlocksAPI struct {
	cAsyncCompleteAPI *C.struct_Longtail_AsyncPruneBlocksAPI
}

type Longtail_AsyncPreflightStartedAPI struct {
	cAsyncCompleteAPI *C.struct_Longtail_AsyncPreflightStartedAPI
}

type Longtail_AsyncFlushAPI struct {
	cAsyncCompleteAPI *C.struct_Longtail_AsyncFlushAPI
}

const (
	Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count       = 0
	Longtail_BlockStoreAPI_StatU64_GetStoredBlock_RetryCount  = 1
	Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount   = 2
	Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count = 3
	Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count  = 4

	Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count       = 5
	Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount  = 6
	Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount   = 7
	Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count = 8
	Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count  = 9

	Longtail_BlockStoreAPI_StatU64_GetExistingContent_Count      = 10
	Longtail_BlockStoreAPI_StatU64_GetExistingContent_RetryCount = 11
	Longtail_BlockStoreAPI_StatU64_GetExistingContent_FailCount  = 12

	Longtail_BlockStoreAPI_StatU64_PruneBlocks_Count      = 13
	Longtail_BlockStoreAPI_StatU64_PruneBlocks_RetryCount = 14
	Longtail_BlockStoreAPI_StatU64_PruneBlocks_FailCount  = 15

	Longtail_BlockStoreAPI_StatU64_PreflightGet_Count      = 16
	Longtail_BlockStoreAPI_StatU64_PreflightGet_RetryCount = 17
	Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount  = 18

	Longtail_BlockStoreAPI_StatU64_Flush_Count     = 19
	Longtail_BlockStoreAPI_StatU64_Flush_FailCount = 20

	Longtail_BlockStoreAPI_StatU64_GetStats_Count = 21
	Longtail_BlockStoreAPI_StatU64_Count          = 22
)

type BlockStoreStats struct {
	StatU64 [Longtail_BlockStoreAPI_StatU64_Count]uint64
}

type BlockStoreAPI interface {
	PutStoredBlock(storedBlock Longtail_StoredBlock, asyncCompleteAPI Longtail_AsyncPutStoredBlockAPI) error
	PreflightGet(blockHashes []uint64, asyncCompleteAPI Longtail_AsyncPreflightStartedAPI) error
	GetStoredBlock(blockHash uint64, asyncCompleteAPI Longtail_AsyncGetStoredBlockAPI) error
	GetExistingContent(chunkHashes []uint64, minBlockUsagePercent uint32, asyncCompleteAPI Longtail_AsyncGetExistingContentAPI) error
	PruneBlocks(blockHashes []uint64, asyncCompleteAPI Longtail_AsyncPruneBlocksAPI) error
	GetStats() (BlockStoreStats, error)
	Flush(asyncCompleteAPI Longtail_AsyncFlushAPI) error
	Close()
}

type Longtail_FileInfos struct {
	cFileInfos *C.struct_Longtail_FileInfos
}

type Longtail_StoreIndex struct {
	cStoreIndex *C.struct_Longtail_StoreIndex
}

type Longtail_VersionIndex struct {
	cVersionIndex *C.struct_Longtail_VersionIndex
}

type Longtail_VersionDiff struct {
	cVersionDiff *C.struct_Longtail_VersionDiff
}

type Longtail_ArchiveIndex struct {
	cArchiveIndex *C.struct_Longtail_ArchiveIndex
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

type Longtail_StorageAPI_HOpenFile struct {
	cOpenFile C.Longtail_StorageAPI_HOpenFile
}

type Longtail_StorageAPI_Iterator struct {
	cIterator C.Longtail_StorageAPI_HIterator
}

type Longtail_StorageAPI_EntryProperties struct {
	Name        string
	Size        uint64
	Permissions uint16
	IsDir       bool
}

type Longtail_HashAPI struct {
	cHashAPI *C.struct_Longtail_HashAPI
}

type Longtail_ChunkerAPI struct {
	cChunkerAPI *C.struct_Longtail_ChunkerAPI
}

var pointerIndex uint32
var pointerStore [1024]interface{}
var pointerIndexer = (*[1 << 30]C.uint32_t)(C.malloc(4 * 1024))

func SavePointer(v interface{}) unsafe.Pointer {
	if v == nil {
		return nil
	}

	newPointerIndex := (atomic.AddUint32(&pointerIndex, 1)) % 1024
	startPointerIndex := newPointerIndex
	for pointerStore[newPointerIndex] != nil {
		newPointerIndex = (atomic.AddUint32(&pointerIndex, 1)) % 1024
		if newPointerIndex == startPointerIndex {
			return nil
		}
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
	const fname = "ReadFromStorage"
	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cFullPath := C.Longtail_Storage_ConcatPath(storageAPI.cStorageAPI, cRootPath, cPath)
	defer C.Longtail_Free(unsafe.Pointer(cFullPath))

	var cOpenFile C.Longtail_StorageAPI_HOpenFile
	errno := C.Longtail_Storage_OpenReadFile(storageAPI.cStorageAPI, cFullPath, &cOpenFile)
	if errno != 0 {
		return nil, errors.Wrap(errnoToError(errno), fname)
	}
	defer C.Longtail_Storage_CloseFile(storageAPI.cStorageAPI, cOpenFile)

	var cSize C.uint64_t
	errno = C.Longtail_Storage_GetSize(storageAPI.cStorageAPI, cOpenFile, &cSize)
	if errno != 0 {
		return nil, errors.Wrap(errnoToError(errno), fname)
	}

	blockData := make([]byte, int(cSize))
	if cSize > 0 {
		errno = C.Longtail_Storage_Read(storageAPI.cStorageAPI, cOpenFile, 0, cSize, unsafe.Pointer(&blockData[0]))
		if errno != 0 {
			return nil, errors.Wrap(errnoToError(errno), fname)
		}
	}
	return blockData, nil
}

// WriteToStorage ...
func (storageAPI *Longtail_StorageAPI) WriteToStorage(rootPath string, path string, blockData []byte) error {
	const fname = "WriteToStorage"
	cRootPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cRootPath))
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	cFullPath := C.Longtail_Storage_ConcatPath(storageAPI.cStorageAPI, cRootPath, cPath)
	defer C.Longtail_Free(unsafe.Pointer(cFullPath))

	errno := C.EnsureParentPathExists(storageAPI.cStorageAPI, cFullPath)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}

	blockSize := C.uint64_t(len(blockData))

	var cOpenFile C.Longtail_StorageAPI_HOpenFile
	errno = C.Longtail_Storage_OpenWriteFile(storageAPI.cStorageAPI, cFullPath, blockSize, &cOpenFile)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	defer C.Longtail_Storage_CloseFile(storageAPI.cStorageAPI, cOpenFile)

	if blockSize > 0 {
		data := unsafe.Pointer(&blockData[0])
		errno = C.Longtail_Storage_Write(storageAPI.cStorageAPI, cOpenFile, 0, blockSize, data)
	}

	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	return nil
}

func (storageAPI *Longtail_StorageAPI) OpenReadFile(path string) (Longtail_StorageAPI_HOpenFile, error) {
	const fname = "OpenReadFile"
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var cOpenFile C.Longtail_StorageAPI_HOpenFile
	errno := C.Longtail_Storage_OpenReadFile(storageAPI.cStorageAPI, cPath, &cOpenFile)
	if errno != 0 {
		return Longtail_StorageAPI_HOpenFile{}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_StorageAPI_HOpenFile{cOpenFile: cOpenFile}, nil
}

func (storageAPI *Longtail_StorageAPI) GetSize(f Longtail_StorageAPI_HOpenFile) (uint64, error) {
	const fname = "GetSize"
	var size C.uint64_t
	errno := C.Longtail_Storage_GetSize(storageAPI.cStorageAPI, f.cOpenFile, &size)
	if errno != 0 {
		return 0, errors.Wrap(errnoToError(errno), fname)
	}
	return uint64(size), nil
}

func (storageAPI *Longtail_StorageAPI) Read(f Longtail_StorageAPI_HOpenFile, offset uint64, size uint64) ([]byte, error) {
	const fname = "Read"
	blockData := make([]byte, size)
	errno := C.Longtail_Storage_Read(storageAPI.cStorageAPI, f.cOpenFile, C.uint64_t(offset), C.uint64_t(size), unsafe.Pointer(&blockData[0]))
	if errno != 0 {
		return nil, errors.Wrap(errnoToError(errno), fname)
	}
	return blockData, nil
}

func (storageAPI *Longtail_StorageAPI) CloseFile(f Longtail_StorageAPI_HOpenFile) {
	C.Longtail_Storage_CloseFile(storageAPI.cStorageAPI, f.cOpenFile)
	f.cOpenFile = nil
}

func (storageAPI *Longtail_StorageAPI) StartFind(path string) (Longtail_StorageAPI_Iterator, error) {
	const fname = "StartFind"
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var cIterator C.Longtail_StorageAPI_HIterator
	errno := C.Longtail_Storage_StartFind(storageAPI.cStorageAPI, cPath, &cIterator)
	if errno != 0 {
		return Longtail_StorageAPI_Iterator{}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_StorageAPI_Iterator{cIterator: cIterator}, nil
}

func (storageAPI *Longtail_StorageAPI) FindNext(iterator Longtail_StorageAPI_Iterator) error {
	const fname = "FindNext"
	errno := C.Longtail_Storage_FindNext(storageAPI.cStorageAPI, iterator.cIterator)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	return nil
}

func (storageAPI *Longtail_StorageAPI) CloseFind(iterator Longtail_StorageAPI_Iterator) {
	C.Longtail_Storage_CloseFind(storageAPI.cStorageAPI, iterator.cIterator)
	iterator.cIterator = nil
}

func (storageAPI *Longtail_StorageAPI) GetEntryProperties(iterator Longtail_StorageAPI_Iterator) (Longtail_StorageAPI_EntryProperties, error) {
	const fname = "GetEntryProperties"
	var cProperties C.struct_Longtail_StorageAPI_EntryProperties
	errno := C.Longtail_Storage_GetEntryProperties(storageAPI.cStorageAPI, iterator.cIterator, &cProperties)
	if errno != 0 {
		return Longtail_StorageAPI_EntryProperties{}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_StorageAPI_EntryProperties{
			Name:        C.GoString(cProperties.m_Name),
			Size:        uint64(cProperties.m_Size),
			Permissions: uint16(cProperties.m_Permissions),
			IsDir:       cProperties.m_IsDir != 0},
		nil
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
	if fileInfos.cFileInfos == nil {
		return 0
	}
	return uint32(fileInfos.cFileInfos.m_Count)
}

func (fileInfos *Longtail_FileInfos) GetFileSizes() []uint64 {
	if fileInfos.cFileInfos == nil {
		return nil
	}
	size := int(fileInfos.cFileInfos.m_Count)
	return carray2slice64(fileInfos.cFileInfos.m_Sizes, size)
}

func (fileInfos *Longtail_FileInfos) GetFilePermissions() []uint16 {
	if fileInfos.cFileInfos == nil {
		return nil
	}
	size := int(fileInfos.cFileInfos.m_Count)
	return carray2slice16(fileInfos.cFileInfos.m_Permissions, size)
}

func (hashAPI *Longtail_HashAPI) GetIdentifier() uint32 {
	return uint32(C.Longtail_Hash_GetIdentifier(hashAPI.cHashAPI))
}

func (storeIndex *Longtail_StoreIndex) Copy() (Longtail_StoreIndex, error) {
	const fname = "Copy"
	if storeIndex.cStoreIndex == nil {
		return Longtail_StoreIndex{}, nil
	}
	cStoreIndex := C.Longtail_CopyStoreIndex(storeIndex.cStoreIndex)
	if cStoreIndex == nil {
		return Longtail_StoreIndex{}, errors.Wrap(errnoToError(C.ENOMEM), fname)
	}
	return Longtail_StoreIndex{cStoreIndex: cStoreIndex}, nil
}

func (storeIndex *Longtail_StoreIndex) IsValid() bool {
	return storeIndex.cStoreIndex != nil
}

func (storeIndex *Longtail_StoreIndex) Dispose() {
	if storeIndex.cStoreIndex != nil {
		C.Longtail_Free(unsafe.Pointer(storeIndex.cStoreIndex))
		storeIndex.cStoreIndex = nil
	}
}

func (storeIndex *Longtail_StoreIndex) GetVersion() uint32 {
	if storeIndex.cStoreIndex == nil {
		return 0
	}
	return uint32(*storeIndex.cStoreIndex.m_Version)
}

func (storeIndex *Longtail_StoreIndex) GetHashIdentifier() uint32 {
	if storeIndex.cStoreIndex == nil {
		return 0
	}
	return uint32(*storeIndex.cStoreIndex.m_HashIdentifier)
}

func (storeIndex *Longtail_StoreIndex) GetBlockCount() uint32 {
	if storeIndex.cStoreIndex == nil {
		return 0
	}
	return uint32(*storeIndex.cStoreIndex.m_BlockCount)
}

func (storeIndex *Longtail_StoreIndex) GetChunkCount() uint32 {
	if storeIndex.cStoreIndex == nil {
		return 0
	}
	return uint32(*storeIndex.cStoreIndex.m_ChunkCount)
}

func (storeIndex *Longtail_StoreIndex) GetBlockHashes() []uint64 {
	if storeIndex.cStoreIndex == nil {
		return nil
	}
	size := int(C.Longtail_StoreIndex_GetBlockCount(storeIndex.cStoreIndex))
	return carray2slice64(C.Longtail_StoreIndex_GetBlockHashes(storeIndex.cStoreIndex), size)
}

func (storeIndex *Longtail_StoreIndex) GetChunkHashes() []uint64 {
	if storeIndex.cStoreIndex == nil {
		return nil
	}
	size := int(*storeIndex.cStoreIndex.m_ChunkCount)
	return carray2slice64(storeIndex.cStoreIndex.m_ChunkHashes, size)
}

func (storeIndex *Longtail_StoreIndex) GetChunkSizes() []uint32 {
	if storeIndex.cStoreIndex == nil {
		return nil
	}
	size := int(*storeIndex.cStoreIndex.m_ChunkCount)
	return carray2slice32(storeIndex.cStoreIndex.m_ChunkSizes, size)
}

func (versionIndex *Longtail_VersionIndex) IsValid() bool {
	return versionIndex.cVersionIndex != nil
}

func (versionIndex *Longtail_VersionIndex) Dispose() {
	if versionIndex.cVersionIndex != nil {
		C.Longtail_Free(unsafe.Pointer(versionIndex.cVersionIndex))
		versionIndex.cVersionIndex = nil
	}
}

func (archiveIndex *Longtail_ArchiveIndex) IsValid() bool {
	return archiveIndex.cArchiveIndex != nil
}

func (archiveIndex *Longtail_ArchiveIndex) Dispose() {
	if archiveIndex.cArchiveIndex != nil {
		C.Longtail_Free(unsafe.Pointer(archiveIndex.cArchiveIndex))
		archiveIndex.cArchiveIndex = nil
	}
}

func (archiveIndex *Longtail_ArchiveIndex) GetStoreIndex() Longtail_StoreIndex {
	return Longtail_StoreIndex{cStoreIndex: C.GetArchiveStoreIndex(archiveIndex.cArchiveIndex)}
}

func (archiveIndex *Longtail_ArchiveIndex) GetVersionIndex() Longtail_VersionIndex {
	return Longtail_VersionIndex{cVersionIndex: C.GetArchiveVersionIndex(archiveIndex.cArchiveIndex)}
}

func (versionIndex *Longtail_VersionIndex) GetVersion() uint32 {
	if versionIndex.cVersionIndex == nil {
		return 0
	}
	return uint32(*versionIndex.cVersionIndex.m_Version)
}

func (versionIndex *Longtail_VersionIndex) GetHashIdentifier() uint32 {
	if versionIndex.cVersionIndex == nil {
		return 0
	}
	return uint32(*versionIndex.cVersionIndex.m_HashIdentifier)
}

func (versionIndex *Longtail_VersionIndex) GetTargetChunkSize() uint32 {
	if versionIndex.cVersionIndex == nil {
		return 0
	}
	return uint32(*versionIndex.cVersionIndex.m_TargetChunkSize)
}

func (versionIndex *Longtail_VersionIndex) GetAssetCount() uint32 {
	if versionIndex.cVersionIndex == nil {
		return 0
	}
	return uint32(*versionIndex.cVersionIndex.m_AssetCount)
}

func (versionIndex *Longtail_VersionIndex) GetAssetPath(assetIndex uint32) string {
	if versionIndex.cVersionIndex == nil {
		return ""
	}
	cPath := C.GetVersionIndexPath(versionIndex.cVersionIndex, C.uint32_t(assetIndex))
	return C.GoString(cPath)
}

func (versionIndex *Longtail_VersionIndex) GetAssetHashes() []uint64 {
	if versionIndex.cVersionIndex == nil {
		return nil
	}
	size := int(*versionIndex.cVersionIndex.m_AssetCount)
	return carray2slice64(versionIndex.cVersionIndex.m_ContentHashes, size)
}

func (versionIndex *Longtail_VersionIndex) GetAssetSize(assetIndex uint32) uint64 {
	if versionIndex.cVersionIndex == nil {
		return 0
	}
	cSize := C.GetVersionAssetSize(versionIndex.cVersionIndex, C.uint32_t(assetIndex))
	return uint64(cSize)
}

func (versionIndex *Longtail_VersionIndex) GetAssetPermissions(assetIndex uint32) uint16 {
	if versionIndex.cVersionIndex == nil {
		return 0
	}
	cPermissions := C.GetVersionAssetPermissions(versionIndex.cVersionIndex, C.uint32_t(assetIndex))
	return uint16(cPermissions)
}

func (versionIndex *Longtail_VersionIndex) GetAssetChunkCounts() []uint32 {
	if versionIndex.cVersionIndex == nil {
		return nil
	}
	size := int(*versionIndex.cVersionIndex.m_AssetCount)
	return carray2slice32(versionIndex.cVersionIndex.m_AssetChunkCounts, size)
}

func (versionIndex *Longtail_VersionIndex) GetAssetChunkIndexStarts() []uint32 {
	if versionIndex.cVersionIndex == nil {
		return nil
	}
	size := int(*versionIndex.cVersionIndex.m_AssetCount)
	return carray2slice32(versionIndex.cVersionIndex.m_AssetChunkIndexStarts, size)
}

func (versionIndex *Longtail_VersionIndex) GetAssetChunkIndexes() []uint32 {
	if versionIndex.cVersionIndex == nil {
		return nil
	}
	size := int(*versionIndex.cVersionIndex.m_AssetChunkIndexCount)
	return carray2slice32(versionIndex.cVersionIndex.m_AssetChunkIndexes, size)
}

func (versionIndex *Longtail_VersionIndex) GetChunkCount() uint32 {
	if versionIndex.cVersionIndex == nil {
		return 0
	}
	return uint32(*versionIndex.cVersionIndex.m_ChunkCount)
}

func (versionIndex *Longtail_VersionIndex) GetChunkHashes() []uint64 {
	if versionIndex.cVersionIndex == nil {
		return nil
	}
	size := int(*versionIndex.cVersionIndex.m_ChunkCount)
	return carray2slice64(versionIndex.cVersionIndex.m_ChunkHashes, size)
}

func (versionIndex *Longtail_VersionIndex) GetChunkSizes() []uint32 {
	if versionIndex.cVersionIndex == nil {
		return nil
	}
	size := int(*versionIndex.cVersionIndex.m_ChunkCount)
	return carray2slice32(versionIndex.cVersionIndex.m_ChunkSizes, size)
}

func (versionIndex *Longtail_VersionIndex) GetAssetSizes() []uint64 {
	if versionIndex.cVersionIndex == nil {
		return nil
	}
	size := int(*versionIndex.cVersionIndex.m_AssetCount)
	return carray2slice64(versionIndex.cVersionIndex.m_AssetSizes, size)
}

func (versionIndex *Longtail_VersionIndex) GetChunkTags() []uint32 {
	if versionIndex.cVersionIndex == nil {
		return nil
	}
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
func (hashRegistry *Longtail_HashRegistryAPI) GetHashAPI(hashIdentifier uint32) (Longtail_HashAPI, error) {
	const fname = "GetHashAPI"
	var hash_api *C.struct_Longtail_HashAPI
	errno := C.Longtail_GetHashRegistry_GetHashAPI(hashRegistry.cHashRegistryAPI, C.uint32_t(hashIdentifier), &hash_api)
	if errno != 0 {
		return Longtail_HashAPI{cHashAPI: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_HashAPI{cHashAPI: hash_api}, nil
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

// CreateHPCDCChunkerAPI ...
func CreateHPCDCChunkerAPI() Longtail_ChunkerAPI {
	return Longtail_ChunkerAPI{cChunkerAPI: C.Longtail_CreateHPCDCChunkerAPI()}
}

// Longtail_ChunkerAPI.Dispose() ...
func (chunkerAPI *Longtail_ChunkerAPI) Dispose() {
	if chunkerAPI.cChunkerAPI != nil {
		C.Longtail_DisposeAPI(&chunkerAPI.cChunkerAPI.m_API)
		chunkerAPI.cChunkerAPI = nil
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
func (asyncCompleteAPI *Longtail_AsyncPutStoredBlockAPI) OnComplete(err error) {
	if asyncCompleteAPI.cAsyncCompleteAPI == nil {
		return
	}
	C.Longtail_AsyncPutStoredBlock_OnComplete(asyncCompleteAPI.cAsyncCompleteAPI, errorToErrno(err, C.EIO))
}

//// Longtail_AsyncGetStoredBlockAPI::OnComplete() ...
func (asyncCompleteAPI *Longtail_AsyncGetStoredBlockAPI) OnComplete(stored_block Longtail_StoredBlock, err error) {
	if asyncCompleteAPI.cAsyncCompleteAPI == nil {
		return
	}
	C.Longtail_AsyncGetStoredBlock_OnComplete(asyncCompleteAPI.cAsyncCompleteAPI, stored_block.cStoredBlock, errorToErrno(err, C.EIO))
}

//// Longtail_AsyncGetExistingContentAPI::OnComplete() ...
func (asyncCompleteAPI *Longtail_AsyncGetExistingContentAPI) OnComplete(store_index Longtail_StoreIndex, err error) {
	if asyncCompleteAPI.cAsyncCompleteAPI == nil {
		return
	}
	C.Longtail_AsyncGetExistingContent_OnComplete(asyncCompleteAPI.cAsyncCompleteAPI, store_index.cStoreIndex, errorToErrno(err, C.EIO))
}

//// Longtail_AsyncPruneBlocksAPI::OnComplete() ...
func (asyncCompleteAPI *Longtail_AsyncPruneBlocksAPI) OnComplete(pruned_block_count uint32, err error) {
	if asyncCompleteAPI.cAsyncCompleteAPI == nil {
		return
	}
	C.Longtail_AsyncPruneBlocks_OnComplete(asyncCompleteAPI.cAsyncCompleteAPI, C.uint32_t(pruned_block_count), errorToErrno(err, C.EIO))
}

//// Longtail_AsyncPreflightStartedAPI::OnComplete() ...
func (asyncCompleteAPI *Longtail_AsyncPreflightStartedAPI) OnComplete(blockHashes []uint64, err error) {
	if asyncCompleteAPI.cAsyncCompleteAPI == nil {
		return
	}
	blockCount := len(blockHashes)
	cblockHashes := (*C.TLongtail_Hash)(unsafe.Pointer(nil))
	if blockCount > 0 {
		cblockHashes = (*C.TLongtail_Hash)(unsafe.Pointer(&blockHashes[0]))
	}
	C.Longtail_AsyncPreflightStarted_OnComplete(asyncCompleteAPI.cAsyncCompleteAPI, C.uint32_t(blockCount), cblockHashes, errorToErrno(err, C.EIO))
}

//// Longtail_AsyncFlushAPI::OnComplete() ...
func (asyncCompleteAPI *Longtail_AsyncFlushAPI) OnComplete(err error) {
	if asyncCompleteAPI.cAsyncCompleteAPI == nil {
		return
	}
	C.Longtail_AsyncFlush_OnComplete(asyncCompleteAPI.cAsyncCompleteAPI, errorToErrno(err, C.EIO))
}

// CreateFSBlockStore() ...
func CreateFSBlockStore(jobAPI Longtail_JobAPI, storageAPI Longtail_StorageAPI, contentPath string, enableFileMapping bool) Longtail_BlockStoreAPI {
	cContentPath := C.CString(contentPath)
	defer C.free(unsafe.Pointer(cContentPath))
	cFileMapping := C.int(0)
	if enableFileMapping {
		cFileMapping = C.int(1)
	}
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateFSBlockStoreAPI(jobAPI.cJobAPI, storageAPI.cStorageAPI, cContentPath, nil, cFileMapping)}
}

// CreateCacheBlockStore() ...
func CreateCacheBlockStore(jobAPI Longtail_JobAPI, cacheBlockStore Longtail_BlockStoreAPI, persistentBlockStore Longtail_BlockStoreAPI) Longtail_BlockStoreAPI {
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateCacheBlockStoreAPI(jobAPI.cJobAPI, cacheBlockStore.cBlockStoreAPI, persistentBlockStore.cBlockStoreAPI)}
}

// CreateCompressBlockStore() ...
func CreateCompressBlockStore(backingBlockStore Longtail_BlockStoreAPI, compressionRegistry Longtail_CompressionRegistryAPI) Longtail_BlockStoreAPI {
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateCompressBlockStoreAPI(backingBlockStore.cBlockStoreAPI, compressionRegistry.cCompressionRegistryAPI)}
}

// CreateShareBlockStore() ...
func CreateShareBlockStore(backingBlockStore Longtail_BlockStoreAPI) Longtail_BlockStoreAPI {
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateShareBlockStoreAPI(backingBlockStore.cBlockStoreAPI)}
}

// CreateLRUBlockStoreAPI() ...
func CreateLRUBlockStoreAPI(blockStore Longtail_BlockStoreAPI, cache_block_count uint) Longtail_BlockStoreAPI {
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateLRUBlockStoreAPI(blockStore.cBlockStoreAPI, (C.uint32_t)(cache_block_count))}
}

// CreateArchiveBlockStoreAPI() ...
func CreateArchiveBlockStoreAPI(storageAPI Longtail_StorageAPI, archivePath string, archiveIndex Longtail_ArchiveIndex, enableWrite bool, enableFileMapping bool) Longtail_BlockStoreAPI {
	cArchivePath := C.CString(archivePath)
	defer C.free(unsafe.Pointer(cArchivePath))
	cEnableWrite := C.int(0)
	if enableWrite {
		cEnableWrite = C.int(1)
	}
	cEnableFileMapping := C.int(0)
	if enableFileMapping {
		cEnableFileMapping = C.int(1)
	}
	return Longtail_BlockStoreAPI{cBlockStoreAPI: C.Longtail_CreateArchiveBlockStore(storageAPI.cStorageAPI, cArchivePath, archiveIndex.cArchiveIndex, cEnableWrite, cEnableFileMapping)}
}

// CreateBlockStoreStorageAPI() ...
func CreateBlockStoreStorageAPI(
	hashAPI Longtail_HashAPI,
	jobAPI Longtail_JobAPI,
	blockStore Longtail_BlockStoreAPI,
	storeIndex Longtail_StoreIndex,
	versionIndex Longtail_VersionIndex) Longtail_StorageAPI {
	return Longtail_StorageAPI{cStorageAPI: C.Longtail_CreateBlockStoreStorageAPI(
		hashAPI.cHashAPI,
		jobAPI.cJobAPI,
		blockStore.cBlockStoreAPI,
		storeIndex.cStoreIndex,
		versionIndex.cVersionIndex)}
}

// Longtail_BlockStoreAPI.IsValid() ...
func (blockStoreAPI *Longtail_BlockStoreAPI) IsValid() bool {
	return blockStoreAPI.cBlockStoreAPI != nil
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
	asyncCompleteAPI Longtail_AsyncPutStoredBlockAPI) error {
	const fname = "Longtail_BlockStoreAPI.PutStoredBlock"
	errno := C.Longtail_BlockStore_PutStoredBlock(
		blockStoreAPI.cBlockStoreAPI,
		storedBlock.cStoredBlock,
		asyncCompleteAPI.cAsyncCompleteAPI)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	return nil
}

// GetStoredBlock() ...
func (blockStoreAPI *Longtail_BlockStoreAPI) GetStoredBlock(
	blockHash uint64,
	asyncCompleteAPI Longtail_AsyncGetStoredBlockAPI) error {
	const fname = "Longtail_BlockStoreAPI.GetStoredBlock"

	errno := C.Longtail_BlockStore_GetStoredBlock(
		blockStoreAPI.cBlockStoreAPI,
		C.uint64_t(blockHash),
		asyncCompleteAPI.cAsyncCompleteAPI)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	return nil
}

// GetExistingContent() ...
func (blockStoreAPI *Longtail_BlockStoreAPI) GetExistingContent(
	chunkHashes []uint64,
	minBlockUsagePercent uint32,
	asyncCompleteAPI Longtail_AsyncGetExistingContentAPI) error {
	const fname = "Longtail_BlockStoreAPI.GetExistingContent"

	chunkCount := len(chunkHashes)
	cChunkHashes := (*C.TLongtail_Hash)(unsafe.Pointer(nil))
	if chunkCount > 0 {
		cChunkHashes = (*C.TLongtail_Hash)(unsafe.Pointer(&chunkHashes[0]))
	}
	errno := C.Longtail_BlockStore_GetExistingContent(
		blockStoreAPI.cBlockStoreAPI,
		C.uint32_t(chunkCount),
		cChunkHashes,
		C.uint32_t(minBlockUsagePercent),
		asyncCompleteAPI.cAsyncCompleteAPI)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	return nil
}

// PruneBlocks() ...
func (blockStoreAPI *Longtail_BlockStoreAPI) PruneBlocks(
	keepBlockHashes []uint64,
	asyncCompleteAPI Longtail_AsyncPruneBlocksAPI) error {
	const fname = "Longtail_BlockStoreAPI.PruneBlocks"

	blockCount := len(keepBlockHashes)
	cBlockHashes := (*C.TLongtail_Hash)(unsafe.Pointer(nil))
	if blockCount > 0 {
		cBlockHashes = (*C.TLongtail_Hash)(unsafe.Pointer(&keepBlockHashes[0]))
	}
	errno := C.Longtail_BlockStore_PruneBlocks(
		blockStoreAPI.cBlockStoreAPI,
		C.uint32_t(blockCount),
		cBlockHashes,
		asyncCompleteAPI.cAsyncCompleteAPI)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	return nil
}

// GetStats() ...
func (blockStoreAPI *Longtail_BlockStoreAPI) GetStats() (BlockStoreStats, error) {
	const fname = "Longtail_BlockStoreAPI.GetStats"

	if blockStoreAPI.cBlockStoreAPI == nil {
		return BlockStoreStats{}, errors.Wrap(errnoToError(C.EINVAL), fname)
	}
	var cStats C.struct_Longtail_BlockStore_Stats
	errno := C.Longtail_BlockStore_GetStats(
		blockStoreAPI.cBlockStoreAPI,
		&cStats)
	if errno != 0 {
		return BlockStoreStats{}, errors.Wrap(errnoToError(errno), fname)
	}

	stats := BlockStoreStats{}
	for s := 0; s < Longtail_BlockStoreAPI_StatU64_Count; s++ {
		stats.StatU64[s] = uint64(cStats.m_StatU64[s])
	}
	return stats, nil
}

// Flush() ...
func (blockStoreAPI *Longtail_BlockStoreAPI) Flush(asyncCompleteAPI Longtail_AsyncFlushAPI) error {
	const fname = "Longtail_BlockStoreAPI.Flush"

	if blockStoreAPI.cBlockStoreAPI == nil {
		asyncCompleteAPI.OnComplete(nil)
		return nil
	}
	errno := C.Longtail_BlockStore_Flush(
		blockStoreAPI.cBlockStoreAPI,
		asyncCompleteAPI.cAsyncCompleteAPI)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	return nil
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

func (blockIndex *Longtail_BlockIndex) Copy() (Longtail_BlockIndex, error) {
	const fname = "Longtail_BlockIndex.Copy"
	if blockIndex.cBlockIndex == nil {
		return Longtail_BlockIndex{}, nil
	}
	cBlockIndex := C.Longtail_CopyBlockIndex(blockIndex.cBlockIndex)
	if cBlockIndex == nil {
		return Longtail_BlockIndex{}, errors.Wrap(errnoToError(C.ENOMEM), fname)
	}
	return Longtail_BlockIndex{cBlockIndex: cBlockIndex}, nil
}

func (blockIndex *Longtail_BlockIndex) IsValid() bool {
	return blockIndex.cBlockIndex != nil
}

func (storedBlock *Longtail_StoredBlock) IsValid() bool {
	return storedBlock.cStoredBlock != nil
}

func (storedBlock *Longtail_StoredBlock) GetBlockIndex() Longtail_BlockIndex {
	return Longtail_BlockIndex{cBlockIndex: storedBlock.cStoredBlock.m_BlockIndex}
}

func (storedBlock *Longtail_StoredBlock) GetBlockSize() int {
	blockIndex := storedBlock.GetBlockIndex()
	chunkCount := C.uint32_t(blockIndex.GetChunkCount())
	blockIndexSize := int(C.Longtail_GetBlockIndexSize(chunkCount))
	blockDataSize := int(storedBlock.cStoredBlock.m_BlockChunksDataSize)
	return blockIndexSize + blockDataSize
}

func (storedBlock *Longtail_StoredBlock) GetChunksBlockData() []byte {
	size := int(storedBlock.cStoredBlock.m_BlockChunksDataSize)
	return carray2sliceByte((*C.char)(storedBlock.cStoredBlock.m_BlockData), size)
}

func (storedBlock *Longtail_StoredBlock) GetBlockHash() uint64 {
	return uint64(*storedBlock.cStoredBlock.m_BlockIndex.m_BlockHash)
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

func WriteStoredBlockToBuffer(storedBlock Longtail_StoredBlock) ([]byte, error) {
	const fname = "WriteStoredBlockToBuffer"

	var buffer unsafe.Pointer
	var size C.size_t
	errno := C.Longtail_WriteStoredBlockToBuffer(storedBlock.cStoredBlock, &buffer, &size)
	if errno != 0 {
		return nil, errors.Wrap(errnoToError(errno), fname)
	}
	defer C.Longtail_Free(buffer)
	bytes := C.GoBytes(buffer, C.int(size))
	return bytes, nil
}

func ReadStoredBlockFromBuffer(buffer []byte) (Longtail_StoredBlock, error) {
	const fname = "ReadStoredBlockFromBuffer"

	if len(buffer) == 0 {
		return Longtail_StoredBlock{}, BadFormatErr()
	}
	cBuffer := unsafe.Pointer(&buffer[0])
	size := C.size_t(len(buffer))
	var stored_block *C.struct_Longtail_StoredBlock
	errno := C.Longtail_ReadStoredBlockFromBuffer(cBuffer, size, &stored_block)
	if errno != 0 {
		return Longtail_StoredBlock{cStoredBlock: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	if stored_block == nil {
		return Longtail_StoredBlock{cStoredBlock: nil}, errors.Wrap(errnoToError(C.EBADF), fname)
	}
	return Longtail_StoredBlock{cStoredBlock: stored_block}, nil
}

func ValidateStore(storeIndex Longtail_StoreIndex, versionIndex Longtail_VersionIndex) error {
	const fname = "ValidateStore"

	errno := C.Longtail_ValidateStore(storeIndex.cStoreIndex, versionIndex.cVersionIndex)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	return nil
}

// CreateStoredBlock() ...
func CreateStoredBlock(
	blockHash uint64,
	hashIdentifier uint32,
	compressionType uint32,
	chunkHashes []uint64,
	chunkSizes []uint32,
	blockData []uint8,
	blockDataIncludesIndex bool) (Longtail_StoredBlock, error) {
	const fname = "CreateStoredBlock"

	chunkCount := len(chunkHashes)
	if chunkCount != len(chunkSizes) {
		return Longtail_StoredBlock{cStoredBlock: nil}, errors.Wrap(errnoToError(C.EINVAL), fname)
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
		return Longtail_StoredBlock{}, errors.Wrap(errnoToError(errno), fname)
	}
	cBlockBytes := unsafe.Pointer(nil)
	if blockByteCount > 0 {
		cBlockBytes = unsafe.Pointer(&blockData[blockByteOffset])
	}
	C.memmove(cStoredBlock.m_BlockData, cBlockBytes, C.size_t(blockByteCount))
	return Longtail_StoredBlock{cStoredBlock: cStoredBlock}, nil
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

// CreateRateLimitedProgressAPI ...
func CreateRateLimitedProgressAPI(progressAPI Longtail_ProgressAPI, percentRateLimit uint32) Longtail_ProgressAPI {
	return Longtail_ProgressAPI{cProgressAPI: C.Longtail_CreateRateLimitedProgress(progressAPI.cProgressAPI, C.uint32_t(percentRateLimit))}
}

// OnProgress ...
func (progressAPI *Longtail_ProgressAPI) OnProgress(totalCount uint32, doneCount uint32) {
	C.Longtail_Progress_OnProgress(progressAPI.cProgressAPI, C.uint32_t(totalCount), C.uint32_t(doneCount))
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

// GetZStdHighCompressionType ...
func GetZStdHighCompressionType() uint32 {
	return uint32(C.Longtail_GetZStdHighQuality())
}

// GetZStdLowCompressionType ...
func GetZStdLowCompressionType() uint32 {
	return uint32(C.Longtail_GetZStdLowQuality())
}

// GetFilesRecursively ...
func GetFilesRecursively(storageAPI Longtail_StorageAPI, pathFilter Longtail_PathFilterAPI, rootPath string) (Longtail_FileInfos, error) {
	const fname = "GetFilesRecursively"

	cFolderPath := C.CString(rootPath)
	defer C.free(unsafe.Pointer(cFolderPath))
	var fileInfos *C.struct_Longtail_FileInfos
	errno := C.Longtail_GetFilesRecursively(storageAPI.cStorageAPI, pathFilter.cPathFilterAPI, nil, nil, cFolderPath, &fileInfos)
	if errno != 0 {
		return Longtail_FileInfos{cFileInfos: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_FileInfos{cFileInfos: fileInfos}, nil
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
func WriteBlockIndexToBuffer(index Longtail_BlockIndex) ([]byte, error) {
	const fname = "WriteBlockIndexToBuffer"

	var buffer unsafe.Pointer
	size := C.size_t(0)
	errno := C.Longtail_WriteBlockIndexToBuffer(index.cBlockIndex, &buffer, &size)
	if errno != 0 {
		return nil, errors.Wrap(errnoToError(errno), fname)
	}
	defer C.Longtail_Free(buffer)
	bytes := C.GoBytes(buffer, C.int(size))
	return bytes, nil
}

// ReadBlockIndexFromBuffer ...
func ReadBlockIndexFromBuffer(buffer []byte) (Longtail_BlockIndex, error) {
	const fname = "ReadBlockIndexFromBuffer"

	if len(buffer) == 0 {
		return Longtail_BlockIndex{}, BadFormatErr()
	}
	cBuffer := unsafe.Pointer(&buffer[0])
	cSize := C.size_t(len(buffer))
	var bindex *C.struct_Longtail_BlockIndex
	errno := C.Longtail_ReadBlockIndexFromBuffer(cBuffer, cSize, &bindex)
	if errno != 0 {
		return Longtail_BlockIndex{cBlockIndex: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_BlockIndex{cBlockIndex: bindex}, nil
}

// CreateVersionIndex ...
func CreateVersionIndex(
	storageAPI Longtail_StorageAPI,
	hashAPI Longtail_HashAPI,
	chunkerAPI Longtail_ChunkerAPI,
	jobAPI Longtail_JobAPI,
	progressAPI *Longtail_ProgressAPI,
	rootPath string,
	fileInfos Longtail_FileInfos,
	assetCompressionTypes []uint32,
	maxChunkSize uint32,
	enableFileMapping bool) (Longtail_VersionIndex, error) {
	const fname = "CreateVersionIndex"

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

	cEnableFileMapping := C.int(0)
	if enableFileMapping {
		cEnableFileMapping = C.int(1)
	}

	var vindex *C.struct_Longtail_VersionIndex
	errno := C.Longtail_CreateVersionIndex(
		storageAPI.cStorageAPI,
		hashAPI.cHashAPI,
		chunkerAPI.cChunkerAPI,
		jobAPI.cJobAPI,
		cProgressAPI,
		nil,
		nil,
		cRootPath,
		fileInfos.cFileInfos,
		(*C.uint32_t)(cCompressionTypes),
		C.uint32_t(maxChunkSize),
		cEnableFileMapping,
		&vindex)

	if errno != 0 {
		return Longtail_VersionIndex{cVersionIndex: nil}, errors.Wrap(errnoToError(errno), fname)
	}

	return Longtail_VersionIndex{cVersionIndex: vindex}, nil
}

// WriteVersionIndexToBuffer ...
func WriteVersionIndexToBuffer(index Longtail_VersionIndex) ([]byte, error) {
	const fname = "WriteVersionIndexToBuffer"

	var buffer unsafe.Pointer
	size := C.size_t(0)
	errno := C.Longtail_WriteVersionIndexToBuffer(index.cVersionIndex, &buffer, &size)
	if errno != 0 {
		return nil, errors.Wrap(errnoToError(errno), fname)
	}
	defer C.Longtail_Free(buffer)
	bytes := C.GoBytes(buffer, C.int(size))
	return bytes, nil
}

// WriteVersionIndex ...
func WriteVersionIndex(storageAPI Longtail_StorageAPI, index Longtail_VersionIndex, path string) error {
	const fname = "WriteVersionIndex"

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	errno := C.Longtail_WriteVersionIndex(storageAPI.cStorageAPI, index.cVersionIndex, cPath)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	return nil
}

// ReadVersionIndexFromBuffer ...
func ReadVersionIndexFromBuffer(buffer []byte) (Longtail_VersionIndex, error) {
	const fname = "ReadVersionIndexFromBuffer"

	if len(buffer) == 0 {
		return Longtail_VersionIndex{}, BadFormatErr()
	}
	cBuffer := unsafe.Pointer(&buffer[0])
	cSize := C.size_t(len(buffer))
	var vindex *C.struct_Longtail_VersionIndex
	errno := C.Longtail_ReadVersionIndexFromBuffer(cBuffer, cSize, &vindex)
	if errno != 0 {
		return Longtail_VersionIndex{cVersionIndex: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_VersionIndex{cVersionIndex: vindex}, nil
}

// ReadVersionIndex ...
func ReadVersionIndex(storageAPI Longtail_StorageAPI, path string) (Longtail_VersionIndex, error) {
	const fname = "ReadVersionIndex"

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var vindex *C.struct_Longtail_VersionIndex
	errno := C.Longtail_ReadVersionIndex(storageAPI.cStorageAPI, cPath, &vindex)
	if errno != 0 {
		return Longtail_VersionIndex{cVersionIndex: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_VersionIndex{cVersionIndex: vindex}, nil
}

func FileExists(storageAPI Longtail_StorageAPI, path string) bool {
	const fname = "FileExists"

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	exists := C.Longtail_Storage_IsFile(storageAPI.cStorageAPI, cPath)
	return exists != 0
}

func DeleteFile(storageAPI Longtail_StorageAPI, path string) error {
	const fname = "FileExists"

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	errno := C.Longtail_Storage_RemoveFile(storageAPI.cStorageAPI, cPath)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	return nil
}

// CreateStoreIndexFromBlocks ...
func CreateStoreIndexFromBlocks(blockIndexes []Longtail_BlockIndex) (Longtail_StoreIndex, error) {
	const fname = "CreateStoreIndexFromBlocks"

	rawBlockIndexes := make([]*C.struct_Longtail_BlockIndex, len(blockIndexes))
	blockCount := len(blockIndexes)
	for index, blockIndex := range blockIndexes {
		rawBlockIndexes[index] = blockIndex.cBlockIndex
	}
	var cBlockIndexes unsafe.Pointer
	if blockCount > 0 {
		cBlockIndexes = unsafe.Pointer(&rawBlockIndexes[0])
	}
	var sindex *C.struct_Longtail_StoreIndex
	errno := C.Longtail_CreateStoreIndexFromBlocks(
		C.uint32_t(blockCount),
		(**C.struct_Longtail_BlockIndex)(cBlockIndexes),
		&sindex)
	if errno != 0 {
		return Longtail_StoreIndex{cStoreIndex: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_StoreIndex{cStoreIndex: sindex}, nil
}

func CreateStoreIndex(
	hashAPI Longtail_HashAPI,
	versionIndex Longtail_VersionIndex,
	maxBlockSize uint32,
	maxChunksPerBlock uint32) (Longtail_StoreIndex, error) {
	const fname = "CreateStoreIndex"

	var sindex *C.struct_Longtail_StoreIndex
	errno := C.Longtail_CreateStoreIndex(
		hashAPI.cHashAPI,
		C.Longtail_VersionIndex_GetChunkCount(versionIndex.cVersionIndex),
		C.Longtail_VersionIndex_GetChunkHashes(versionIndex.cVersionIndex),
		C.Longtail_VersionIndex_GetChunkSizes(versionIndex.cVersionIndex),
		C.Longtail_VersionIndex_GetChunkTags(versionIndex.cVersionIndex),
		C.uint32_t(maxBlockSize),
		C.uint32_t(maxChunksPerBlock),
		&sindex)
	if errno != 0 {
		return Longtail_StoreIndex{cStoreIndex: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_StoreIndex{cStoreIndex: sindex}, nil
}

func GetExistingStoreIndex(
	storeIndex Longtail_StoreIndex,
	chunkHashes []uint64,
	minBlockUsagePercent uint32) (Longtail_StoreIndex, error) {
	const fname = "GetExistingStoreIndex"

	chunkCount := uint32(len(chunkHashes))
	var cChunkHashes *C.TLongtail_Hash
	if chunkCount > 0 {
		cChunkHashes = (*C.TLongtail_Hash)(unsafe.Pointer(&chunkHashes[0]))
	}
	var cindex *C.struct_Longtail_StoreIndex
	errno := C.Longtail_GetExistingStoreIndex(
		storeIndex.cStoreIndex,
		C.uint32_t(chunkCount),
		cChunkHashes,
		C.uint32_t(minBlockUsagePercent),
		&cindex)
	if errno != 0 {
		return Longtail_StoreIndex{cStoreIndex: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_StoreIndex{cStoreIndex: cindex}, nil
}

func PruneStoreIndex(
	storeIndex Longtail_StoreIndex,
	keepBlockHashes []uint64) (Longtail_StoreIndex, error) {
	const fname = "PruneStoreIndex"

	blockCount := uint32(len(keepBlockHashes))
	var cBlockHashes *C.TLongtail_Hash
	if blockCount > 0 {
		cBlockHashes = (*C.TLongtail_Hash)(unsafe.Pointer(&keepBlockHashes[0]))
	}
	var cindex *C.struct_Longtail_StoreIndex
	errno := C.Longtail_PruneStoreIndex(
		storeIndex.cStoreIndex,
		C.uint32_t(blockCount),
		cBlockHashes,
		&cindex)
	if errno != 0 {
		return Longtail_StoreIndex{cStoreIndex: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_StoreIndex{cStoreIndex: cindex}, nil
}

func GetRequiredChunkHashes(
	versionIndex Longtail_VersionIndex,
	versionDiff Longtail_VersionDiff) ([]uint64, error) {
	const fname = "GetRequiredChunkHashes"

	maxChunkCount := uint64(versionIndex.GetChunkCount())
	outChunkHashes := make([]uint64, maxChunkCount)
	var cChunkHashes *C.TLongtail_Hash
	if maxChunkCount > 0 {
		cChunkHashes = (*C.TLongtail_Hash)(unsafe.Pointer(&outChunkHashes[0]))
	}
	var outChunkCount C.uint32_t
	errno := C.Longtail_GetRequiredChunkHashes(
		versionIndex.cVersionIndex,
		versionDiff.cVersionDiff,
		&outChunkCount,
		cChunkHashes)
	if errno != 0 {
		return []uint64{}, errors.Wrap(errnoToError(errno), fname)
	}
	return outChunkHashes[:int(outChunkCount)], nil
}

func MergeStoreIndex(local_store_index Longtail_StoreIndex, remote_store_index Longtail_StoreIndex) (Longtail_StoreIndex, error) {
	const fname = "MergeStoreIndex"

	var sIndex *C.struct_Longtail_StoreIndex
	errno := C.Longtail_MergeStoreIndex(
		local_store_index.cStoreIndex,
		remote_store_index.cStoreIndex,
		&sIndex)
	if errno != 0 {
		return Longtail_StoreIndex{cStoreIndex: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_StoreIndex{cStoreIndex: sIndex}, nil
}

// WriteStoreIndexToBuffer ...
func WriteStoreIndexToBuffer(index Longtail_StoreIndex) ([]byte, error) {
	const fname = "WriteStoreIndexToBuffer"

	var buffer unsafe.Pointer
	size := C.size_t(0)
	errno := C.Longtail_WriteStoreIndexToBuffer(index.cStoreIndex, &buffer, &size)
	if errno != 0 {
		return nil, errors.Wrap(errnoToError(errno), fname)
	}
	defer C.Longtail_Free(buffer)
	bytes := C.GoBytes(buffer, C.int(size))
	return bytes, nil
}

// ReadStoreIndexFromBuffer ...
func ReadStoreIndexFromBuffer(buffer []byte) (Longtail_StoreIndex, error) {
	const fname = "ReadStoreIndexFromBuffer"

	if len(buffer) == 0 {
		return Longtail_StoreIndex{}, BadFormatErr()
	}
	cBuffer := unsafe.Pointer(&buffer[0])
	cSize := C.size_t(len(buffer))
	var cindex *C.struct_Longtail_StoreIndex
	errno := C.Longtail_ReadStoreIndexFromBuffer(cBuffer, cSize, &cindex)
	if errno != 0 {
		return Longtail_StoreIndex{cStoreIndex: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_StoreIndex{cStoreIndex: cindex}, nil
}

// ReadArchiveIndex ...
func ReadArchiveIndex(storageAPI Longtail_StorageAPI, path string) (Longtail_ArchiveIndex, error) {
	const fname = "ReadArchiveIndex"

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var aindex *C.struct_Longtail_ArchiveIndex
	errno := C.Longtail_ReadArchiveIndex(storageAPI.cStorageAPI, cPath, &aindex)
	if errno != 0 {
		return Longtail_ArchiveIndex{cArchiveIndex: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_ArchiveIndex{cArchiveIndex: aindex}, nil
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
	if progress == nil {
		return
	}
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
func PathFilterAPIProxy_Include(path_filter_api *C.struct_Longtail_PathFilterAPI, root_path *C.char, asset_path *C.char, asset_name *C.char, is_dir C.int, size C.uint64_t, permissions C.uint16_t) (keep C.int) {
	const fname = "PathFilterAPIProxy_Include"

	context := C.PathFilterAPIProxy_GetContext(unsafe.Pointer(path_filter_api))
	pathFilter := RestorePointer(context).(PathFilterAPI)
	if pathFilter == nil {
		return 0
	}
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
func AsyncPutStoredBlockAPIProxy_OnComplete(async_complete_api *C.struct_Longtail_AsyncPutStoredBlockAPI, err C.int) {
	const fname = "AsyncPutStoredBlockAPIProxy_OnComplete"
	context := C.AsyncPutStoredBlockAPIProxy_GetContext(unsafe.Pointer(async_complete_api))
	asyncComplete := RestorePointer(context).(AsyncPutStoredBlockAPI)
	if asyncComplete == nil {
		return
	}
	asyncComplete.OnComplete(errors.Wrap(errnoToError(err), fname))
	C.Longtail_DisposeAPI(&async_complete_api.m_API)
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
func AsyncGetStoredBlockAPIProxy_OnComplete(async_complete_api *C.struct_Longtail_AsyncGetStoredBlockAPI, stored_block *C.struct_Longtail_StoredBlock, err C.int) {
	const fname = "AsyncGetStoredBlockAPIProxy_OnComplete"
	context := C.AsyncGetStoredBlockAPIProxy_GetContext(unsafe.Pointer(async_complete_api))
	asyncComplete := RestorePointer(context).(AsyncGetStoredBlockAPI)
	if asyncComplete == nil {
		return
	}
	asyncComplete.OnComplete(Longtail_StoredBlock{cStoredBlock: stored_block}, errors.Wrap(errnoToError(err), fname))
	C.Longtail_DisposeAPI(&async_complete_api.m_API)
}

//export AsyncGetStoredBlockAPIProxy_Dispose
func AsyncGetStoredBlockAPIProxy_Dispose(api *C.struct_Longtail_API) {
	context := C.AsyncGetStoredBlockAPIProxy_GetContext(unsafe.Pointer(api))
	UnrefPointer(context)
	C.Longtail_Free(unsafe.Pointer(api))
}

// CreateAsyncGetExistingContentAPI ...
func CreateAsyncGetExistingContentAPI(asyncComplete AsyncGetExistingContentAPI) Longtail_AsyncGetExistingContentAPI {
	cContext := SavePointer(asyncComplete)
	asyncCompleteAPIProxy := C.CreateAsyncGetExistingContentAPI(cContext)
	return Longtail_AsyncGetExistingContentAPI{cAsyncCompleteAPI: asyncCompleteAPIProxy}
}

//export AsyncGetExistingContentAPIProxy_OnComplete
func AsyncGetExistingContentAPIProxy_OnComplete(async_complete_api *C.struct_Longtail_AsyncGetExistingContentAPI, store_index *C.struct_Longtail_StoreIndex, err C.int) {
	const fname = "AsyncGetExistingContentAPIProxy_OnComplete"
	context := C.AsyncGetExistingContentAPIProxy_GetContext(unsafe.Pointer(async_complete_api))
	asyncComplete := RestorePointer(context).(AsyncGetExistingContentAPI)
	if asyncComplete == nil {
		return
	}
	asyncComplete.OnComplete(Longtail_StoreIndex{cStoreIndex: store_index}, errors.Wrap(errnoToError(err), fname))
	C.Longtail_DisposeAPI(&async_complete_api.m_API)
}

//export AsyncGetExistingContentAPIProxy_Dispose
func AsyncGetExistingContentAPIProxy_Dispose(api *C.struct_Longtail_API) {
	context := C.AsyncGetExistingContentAPIProxy_GetContext(unsafe.Pointer(api))
	UnrefPointer(context)
	C.Longtail_Free(unsafe.Pointer(api))
}

// CreateAsyncPruneBlocksAPI ...
func CreateAsyncPruneBlocksAPI(asyncComplete AsyncPruneBlocksAPI) Longtail_AsyncPruneBlocksAPI {
	cContext := SavePointer(asyncComplete)
	asyncCompleteAPIProxy := C.CreateAsyncPruneBlocksAPI(cContext)
	return Longtail_AsyncPruneBlocksAPI{cAsyncCompleteAPI: asyncCompleteAPIProxy}
}

//export AsyncPruneBlocksAPIProxy_OnComplete
func AsyncPruneBlocksAPIProxy_OnComplete(async_complete_api *C.struct_Longtail_AsyncPruneBlocksAPI, pruned_block_count uint32, err C.int) {
	const fname = "AsyncPruneBlocksAPIProxy_OnComplete"
	context := C.AsyncPruneBlocksAPIProxy_GetContext(unsafe.Pointer(async_complete_api))
	asyncComplete := RestorePointer(context).(AsyncPruneBlocksAPI)
	if asyncComplete == nil {
		return
	}
	asyncComplete.OnComplete(pruned_block_count, errors.Wrap(errnoToError(err), fname))
	C.Longtail_DisposeAPI(&async_complete_api.m_API)
}

//export AsyncPruneBlocksAPIProxy_Dispose
func AsyncPruneBlocksAPIProxy_Dispose(api *C.struct_Longtail_API) {
	context := C.AsyncPruneBlocksAPIProxy_GetContext(unsafe.Pointer(api))
	UnrefPointer(context)
	C.Longtail_Free(unsafe.Pointer(api))
}

// CreateAsyncFlushAPI ...
func CreateAsyncFlushAPI(asyncComplete AsyncFlushAPI) Longtail_AsyncFlushAPI {
	cContext := SavePointer(asyncComplete)
	asyncCompleteAPIProxy := C.CreateAsyncFlushAPI(cContext)
	return Longtail_AsyncFlushAPI{cAsyncCompleteAPI: asyncCompleteAPIProxy}
}

// Dispose ...
func (asyncCompleteAPI *Longtail_AsyncFlushAPI) Dispose() {
	if asyncCompleteAPI.cAsyncCompleteAPI != nil {
		//		C.Longtail_DisposeAPI(&asyncCompleteAPI.cAsyncCompleteAPI.m_API)
		asyncCompleteAPI.cAsyncCompleteAPI = nil
	}
}

//export AsyncFlushAPIProxy_OnComplete
func AsyncFlushAPIProxy_OnComplete(async_complete_api *C.struct_Longtail_AsyncFlushAPI, err C.int) {
	const fname = "AsyncFlushAPIProxy_OnComplete"
	context := C.AsyncFlushAPIProxy_GetContext(unsafe.Pointer(async_complete_api))
	asyncComplete := RestorePointer(context).(AsyncFlushAPI)
	if asyncComplete == nil {
		return
	}
	asyncComplete.OnComplete(errors.Wrap(errnoToError(err), fname))
	C.Longtail_DisposeAPI(&async_complete_api.m_API)
}

//export AsyncFlushAPIProxy_Dispose
func AsyncFlushAPIProxy_Dispose(api *C.struct_Longtail_API) {
	context := C.AsyncFlushAPIProxy_GetContext(unsafe.Pointer(api))
	UnrefPointer(context)
	C.Longtail_Free(unsafe.Pointer(api))
}

// WriteContent ...
func WriteContent(
	sourceStorageAPI Longtail_StorageAPI,
	targetBlockStoreAPI Longtail_BlockStoreAPI,
	jobAPI Longtail_JobAPI,
	progressAPI *Longtail_ProgressAPI,
	store_index Longtail_StoreIndex,
	versionIndex Longtail_VersionIndex,
	versionFolderPath string) error {
	const fname = "WriteContent"

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
		store_index.cStoreIndex,
		versionIndex.cVersionIndex,
		cVersionFolderPath)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	return nil
}

// CreateMissingContent ...
func CreateMissingContent(
	hashAPI Longtail_HashAPI,
	storeIndex Longtail_StoreIndex,
	versionIndex Longtail_VersionIndex,
	maxBlockSize uint32,
	maxChunksPerBlock uint32) (Longtail_StoreIndex, error) {
	const fname = "CreateMissingContent"

	var missingStoreIndex *C.struct_Longtail_StoreIndex
	errno := C.Longtail_CreateMissingContent(
		hashAPI.cHashAPI,
		storeIndex.cStoreIndex,
		versionIndex.cVersionIndex,
		C.uint32_t(maxBlockSize),
		C.uint32_t(maxChunksPerBlock),
		&missingStoreIndex)
	if errno != 0 {
		return Longtail_StoreIndex{cStoreIndex: nil}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_StoreIndex{cStoreIndex: missingStoreIndex}, nil
}

// WriteVersion ...
func WriteVersion(
	contentBlockStoreAPI Longtail_BlockStoreAPI,
	versionStorageAPI Longtail_StorageAPI,
	jobAPI Longtail_JobAPI,
	progressAPI *Longtail_ProgressAPI,
	storeIndex Longtail_StoreIndex,
	versionIndex Longtail_VersionIndex,
	versionFolderPath string,
	retainPermissions bool) error {
	const fname = "WriteVersion"

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
		storeIndex.cStoreIndex,
		versionIndex.cVersionIndex,
		cVersionFolderPath,
		cRetainPermissions)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	return nil
}

//CreateVersionDiff do we really need this? Maybe ChangeVersion should create one on the fly?
func CreateVersionDiff(
	hashAPI Longtail_HashAPI,
	sourceVersionIndex Longtail_VersionIndex,
	targetVersionIndex Longtail_VersionIndex) (Longtail_VersionDiff, error) {
	const fname = "CreateVersionDiff"

	var versionDiff *C.struct_Longtail_VersionDiff
	errno := C.Longtail_CreateVersionDiff(
		hashAPI.cHashAPI,
		sourceVersionIndex.cVersionIndex,
		targetVersionIndex.cVersionIndex,
		&versionDiff)
	if errno != 0 {
		return Longtail_VersionDiff{cVersionDiff: nil}, errors.Wrap(errnoToError(errno), fname)
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
	storeIndex Longtail_StoreIndex,
	sourceVersionIndex Longtail_VersionIndex,
	targetVersionIndex Longtail_VersionIndex,
	versionDiff Longtail_VersionDiff,
	versionFolderPath string,
	retainPermissions bool) error {
	const fname = "ChangeVersion"

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
		storeIndex.cStoreIndex,
		sourceVersionIndex.cVersionIndex,
		targetVersionIndex.cVersionIndex,
		versionDiff.cVersionDiff,
		cVersionFolderPath,
		cRetainPermissions)
	if errno != 0 {
		return errors.Wrap(errnoToError(errno), fname)
	}
	return nil
}

func CreateArchiveIndex(
	storeIndex Longtail_StoreIndex,
	versionIndex Longtail_VersionIndex) (Longtail_ArchiveIndex, error) {
	const fname = "CreateArchiveIndex"

	var archiveIndex *C.struct_Longtail_ArchiveIndex
	errno := C.Longtail_CreateArchiveIndex(
		storeIndex.cStoreIndex,
		versionIndex.cVersionIndex,
		&archiveIndex)
	if errno != 0 {
		return Longtail_ArchiveIndex{}, errors.Wrap(errnoToError(errno), fname)
	}
	return Longtail_ArchiveIndex{cArchiveIndex: archiveIndex}, nil
}

//export LogProxy_Log
func LogProxy_Log(log_context *C.struct_Longtail_LogContext, log *C.char) {
	logger := RestorePointer(log_context.context).(Logger)
	if logger == nil {
		return
	}
	logFields := make([]LogField, log_context.field_count)
	for i := 0; i < int(log_context.field_count); i++ {
		logField := C.GetLogField(log_context, C.int(i))
		logFields[i].Name = C.GoString(logField.name)
		logFields[i].Value = C.GoString(logField.value)
	}
	logger.OnLog(C.GoString(log_context.file), C.GoString(log_context.function), int(log_context.line), int(log_context.level), logFields, C.GoString(log))
}

//export BlockStoreAPIProxy_Dispose
func BlockStoreAPIProxy_Dispose(api *C.struct_Longtail_API) {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	if blockStore == nil {
		return
	}
	blockStore.Close()
	UnrefPointer(context)
	C.Longtail_Free(unsafe.Pointer(api))
}

//export BlockStoreAPIProxy_PutStoredBlock
func BlockStoreAPIProxy_PutStoredBlock(api *C.struct_Longtail_BlockStoreAPI, storedBlock *C.struct_Longtail_StoredBlock, async_complete_api *C.struct_Longtail_AsyncPutStoredBlockAPI) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	if blockStore == nil {
		return C.EINVAL
	}
	err := blockStore.PutStoredBlock(Longtail_StoredBlock{cStoredBlock: storedBlock}, Longtail_AsyncPutStoredBlockAPI{cAsyncCompleteAPI: async_complete_api})
	return errorToErrno(err, C.EIO)
}

//export BlockStoreAPIProxy_GetStoredBlock
func BlockStoreAPIProxy_GetStoredBlock(api *C.struct_Longtail_BlockStoreAPI, blockHash C.uint64_t, async_complete_api *C.struct_Longtail_AsyncGetStoredBlockAPI) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	if blockStore == nil {
		return C.EINVAL
	}
	err := blockStore.GetStoredBlock(uint64(blockHash), Longtail_AsyncGetStoredBlockAPI{cAsyncCompleteAPI: async_complete_api})
	return errorToErrno(err, C.EIO)
}

//export BlockStoreAPIProxy_PreflightGet
func BlockStoreAPIProxy_PreflightGet(api *C.struct_Longtail_BlockStoreAPI, block_count C.uint32_t, block_hashes *C.TLongtail_Hash, async_complete_api *C.struct_Longtail_AsyncPreflightStartedAPI) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	if blockStore == nil {
		return C.EINVAL
	}
	blockCount := int(block_count)
	blockHashes := carray2slice64(block_hashes, blockCount)
	copyBlockHashes := append([]uint64{}, blockHashes...)
	err := blockStore.PreflightGet(copyBlockHashes, Longtail_AsyncPreflightStartedAPI{cAsyncCompleteAPI: async_complete_api})
	return errorToErrno(err, C.EIO)
}

//export BlockStoreAPIProxy_GetExistingContent
func BlockStoreAPIProxy_GetExistingContent(api *C.struct_Longtail_BlockStoreAPI, chunk_count C.uint32_t, chunk_hashes *C.TLongtail_Hash, min_block_usage_percent C.uint32_t, async_complete_api *C.struct_Longtail_AsyncGetExistingContentAPI) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	if blockStore == nil {
		return C.EINVAL
	}
	chunkCount := int(chunk_count)
	minBlockUsagePercent := uint32(min_block_usage_percent)
	chunkHashes := carray2slice64(chunk_hashes, chunkCount)
	copyChunkHashes := append([]uint64{}, chunkHashes...)
	err := blockStore.GetExistingContent(
		copyChunkHashes,
		minBlockUsagePercent,
		Longtail_AsyncGetExistingContentAPI{cAsyncCompleteAPI: async_complete_api})
	return errorToErrno(err, C.EIO)
}

//export BlockStoreAPIProxy_PruneBlocks
func BlockStoreAPIProxy_PruneBlocks(api *C.struct_Longtail_BlockStoreAPI, keep_block_count C.uint32_t, keep_block_hashes *C.TLongtail_Hash, async_complete_api *C.struct_Longtail_AsyncPruneBlocksAPI) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	if blockStore == nil {
		return C.EINVAL
	}
	keepBlockCount := int(keep_block_count)
	keepBlockHashes := carray2slice64(keep_block_hashes, keepBlockCount)
	copyBlockHashes := append([]uint64{}, keepBlockHashes...)
	err := blockStore.PruneBlocks(
		copyBlockHashes,
		Longtail_AsyncPruneBlocksAPI{cAsyncCompleteAPI: async_complete_api})
	return errorToErrno(err, C.EIO)
}

//export BlockStoreAPIProxy_GetStats
func BlockStoreAPIProxy_GetStats(api *C.struct_Longtail_BlockStoreAPI, out_stats *C.struct_Longtail_BlockStore_Stats) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	if blockStore == nil {
		return C.EINVAL
	}
	stats, err := blockStore.GetStats()
	if err == nil {
		for s := 0; s < Longtail_BlockStoreAPI_StatU64_Count; s++ {
			out_stats.m_StatU64[s] = C.uint64_t(stats.StatU64[s])
		}
	}
	return errorToErrno(err, C.EIO)
}

//export BlockStoreAPIProxy_Flush
func BlockStoreAPIProxy_Flush(api *C.struct_Longtail_BlockStoreAPI, async_complete_api *C.struct_Longtail_AsyncFlushAPI) C.int {
	context := C.BlockStoreAPIProxy_GetContext(unsafe.Pointer(api))
	blockStore := RestorePointer(context).(BlockStoreAPI)
	if nil == blockStore {
		return C.EINVAL
	}
	err := blockStore.Flush(Longtail_AsyncFlushAPI{cAsyncCompleteAPI: async_complete_api})
	return errorToErrno(err, C.EIO)
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

//EnableMemtrace ...
func EnableMemtrace() {
	C.EnableMemtrace()
}

//MemTraceSummary ...
const MemTraceSummary = 0

//MemTraceDetailed ...
const MemTraceDetailed = 1

//GetMemTraceStats ...
func GetMemTraceStats(logLevel int) string {
	var cLogLevel C.uint32_t
	switch logLevel {
	case MemTraceSummary:
		cLogLevel = C.Longtail_GetMemTracerSummary()
	case MemTraceDetailed:
		cLogLevel = C.Longtail_GetMemTracerDetailed()
	}
	cStats := C.Longtail_MemTracer_GetStats(cLogLevel)
	stats := C.GoString(cStats)
	C.Longtail_Free(unsafe.Pointer(cStats))
	return stats
}

//DisableMemtrace ...
func DisableMemtrace() {
	C.DisableMemtrace()
}

//MemTraceDumpStats ...
func MemTraceDumpStats(path string) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	C.Longtail_MemTracer_DumpStats(cPath)
}
