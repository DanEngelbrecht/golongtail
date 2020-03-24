package lib

import (
	"crypto/rand"
	"fmt"
	"runtime"
	"sync"
	"testing"
)

type testProgress struct {
	inited     bool
	oldPercent uint32
	task       string
	t          *testing.T
}

func (p *testProgress) OnProgress(total uint32, current uint32) {
	if current < total {
		if !p.inited {
			p.t.Logf("%s: ", p.task)
			p.inited = true
		}
		percentDone := (100 * current) / total
		if (percentDone - p.oldPercent) >= 5 {
			p.t.Logf("%d%% ", percentDone)
			p.oldPercent = percentDone
		}
		return
	}
	if p.inited {
		if p.oldPercent != 100 {
			p.t.Logf("100%%")
		}
		p.t.Logf(" Done\n")
	}
}

type testCompletionAPI struct {
	err int
}

func (a *testCompletionAPI) OnComplete(err int) int {
	a.err = err
	return 0
}

type testLogger struct {
	t *testing.T
}

func (l *testLogger) OnLog(level int, log string) {
	l.t.Logf("%d: %s", level, log)
}

type testAssert struct {
	t *testing.T
}

func (a *testAssert) OnAssert(expression string, file string, line int) {
	fmt.Printf("ASSERT: %s %s:%d", expression, file, line)
}

func TestDebugging(t *testing.T) {
	SetLogger(&testLogger{t: t})
	defer SetLogger(nil)
	SetAssert(&testAssert{t: t})
	defer SetAssert(nil)
	SetLogLevel(3)
}

func TestInMemStorage(t *testing.T) {
	SetLogger(&testLogger{t: t})
	defer SetLogger(nil)
	SetAssert(&testAssert{t: t})
	defer SetAssert(nil)
	SetLogLevel(1)

	storageAPI := CreateInMemStorageAPI()
	defer storageAPI.Dispose()
	myString := "my string"
	err := storageAPI.WriteToStorage("folder", "file", []byte(myString))
	expected := error(nil)
	if err != nil {
		t.Errorf("WriteToStorage() %q != %q", err, expected)
	}

	rbytes, err := storageAPI.ReadFromStorage("folder", "file")
	testString := string(rbytes)
	if myString != myString {
		t.Errorf("ReadFromStorage() %s != %s", rbytes, testString)
	}
}

func TestAPICreate(t *testing.T) {
	SetLogger(&testLogger{t: t})
	defer SetLogger(nil)
	SetAssert(&testAssert{t: t})
	defer SetAssert(nil)
	SetLogLevel(1)

	blake2 := CreateBlake2HashAPI()
	defer blake2.Dispose()

	blake3 := CreateBlake3HashAPI()
	defer blake3.Dispose()

	meow := CreateMeowHashAPI()
	defer meow.Dispose()

	lz4 := CreateLZ4CompressionAPI()
	defer lz4.Dispose()

	brotli := CreateBrotliCompressionAPI()
	defer brotli.Dispose()

	zstd := CreateZStdCompressionAPI()
	defer zstd.Dispose()

	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()

	compressionRegistry := CreateFullCompressionRegistry()
	defer compressionRegistry.Dispose()
}

func createStoredBlock(chunkCount uint32) (Longtail_StoredBlock, int) {
	blockHash := uint64(0xdeadbeef500177aa) + uint64(chunkCount)
	chunkHashes := make([]uint64, chunkCount)
	chunkSizes := make([]uint32, chunkCount)
	blockOffset := uint32(0)
	for index, _ := range chunkHashes {
		chunkHashes[index] = uint64(index+1) * 4711
		chunkSizes[index] = uint32(index+1) * 10
		blockOffset += uint32(chunkSizes[index])
	}
	blockData := make([]uint8, blockOffset)
	blockOffset = 0
	for chunkIndex, _ := range chunkHashes {
		for index := uint32(0); index < blockOffset; index++ {
			blockData[blockOffset+index] = uint8(chunkIndex + 1)
		}
		blockOffset += uint32(chunkSizes[chunkIndex])
	}

	return CreateStoredBlock(
		blockHash,
		chunkCount+uint32(10000),
		chunkHashes,
		chunkSizes,
		blockData,
		false)
}

func validateStoredBlock(t *testing.T, storedBlock Longtail_StoredBlock) {
	if storedBlock.cStoredBlock == nil {
		t.Errorf("validateStoredBlock() %p == %p", storedBlock, Longtail_StoredBlock{cStoredBlock: nil})
	}
	blockIndex := storedBlock.GetBlockIndex()
	chunkCount := blockIndex.GetChunkCount()
	if blockIndex.GetBlockHash() != uint64(0xdeadbeef500177aa)+uint64(chunkCount) {
		t.Errorf("validateStoredBlock() %q != %q", uint64(0xdeadbeef500177aa)+uint64(chunkCount), blockIndex.GetBlockHash())
	}
	if blockIndex.GetTag() != chunkCount+uint32(10000) {
		t.Errorf("validateStoredBlock() %q != %q", chunkCount+uint32(10000), blockIndex.GetTag())

	}
	chunkHashes := blockIndex.GetChunkHashes()
	if uint32(len(chunkHashes)) != chunkCount {
		t.Errorf("validateStoredBlock() %q != %q", chunkCount, uint32(len(chunkHashes)))
	}
	chunkSizes := blockIndex.GetChunkSizes()
	if uint32(len(chunkSizes)) != chunkCount {
		t.Errorf("validateStoredBlock() %q != %q", chunkCount, uint32(len(chunkSizes)))
	}
	blockOffset := uint32(0)
	for index, _ := range chunkHashes {
		if chunkHashes[index] != uint64(index+1)*4711 {
			t.Errorf("validateStoredBlock() %q != %q", uint64(index)*4711, chunkHashes[index])
		}
		if chunkSizes[index] != uint32(index+1)*10 {
			t.Errorf("validateStoredBlock() %q != %q", uint32(index)*10, chunkSizes[index])
		}
		blockOffset += uint32(chunkSizes[index])
	}
	blockData := storedBlock.GetChunksBlockData()
	if uint32(len(blockData)) != blockOffset {
		t.Errorf("validateStoredBlock() %q != %q", uint32(len(blockData)), blockOffset)
	}
	blockOffset = 0
	for chunkIndex, _ := range chunkHashes {
		for index := uint32(0); index < blockOffset; index++ {
			if blockData[blockOffset+index] != uint8(chunkIndex+1) {
				t.Errorf("validateStoredBlock() %q != %q", uint8(chunkIndex+1), blockData[blockOffset+index])
			}
		}
		blockOffset += uint32(chunkSizes[chunkIndex])
	}
}

func TestStoredblock(t *testing.T) {
	SetLogger(&testLogger{t: t})
	defer SetLogger(nil)
	SetAssert(&testAssert{t: t})
	defer SetAssert(nil)
	SetLogLevel(1)

	storedBlock, errno := createStoredBlock(2)
	if errno != 0 {
		t.Errorf("CreateStoredBlock() %d!= %d", errno, 0)
	}
	validateStoredBlock(t, storedBlock)
}

func Test_ReadWriteStoredBlockBuffer(t *testing.T) {
	SetLogger(&testLogger{t: t})
	defer SetLogger(nil)
	SetAssert(&testAssert{t: t})
	defer SetAssert(nil)
	SetLogLevel(1)

	originalBlock, errno := createStoredBlock(2)
	if errno != 0 {
		t.Errorf("createStoredBlock() %d != %d", errno, 0)
	}

	blockIndexData, err := WriteBlockIndexToBuffer(originalBlock.GetBlockIndex())
	if err != nil {
		t.Errorf("WriteBlockIndexToBuffer() %q != %q", err, error(nil))
	}

	blockData := originalBlock.GetChunksBlockData()
	storedBlockData := append(blockIndexData, blockData...)
	originalBlock.Dispose()
	blockIndexData = nil

	copyBlock, err := InitStoredBlockFromData(storedBlockData)
	if err != nil {
		t.Errorf("InitStoredBlockFromData() %q != %q", err, error(nil))
	}
	defer copyBlock.Dispose()
	validateStoredBlock(t, copyBlock)
}

func TestFSBlockStore(t *testing.T) {
	SetLogger(&testLogger{t: t})
	defer SetLogger(nil)
	SetAssert(&testAssert{t: t})
	defer SetAssert(nil)
	SetLogLevel(1)

	storageAPI := CreateInMemStorageAPI()
	defer storageAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()
	blockStoreAPI := CreateFSBlockStore(storageAPI, "content")
	defer blockStoreAPI.Dispose()
	blake3 := CreateBlake3HashAPI()
	defer blake3.Dispose()

	contentIndex, errno := blockStoreAPI.GetIndex(blake3.GetIdentifier(), jobAPI, nil)
	defer contentIndex.Dispose()
	if errno != 0 {
		t.Errorf("TestFSBlockStore() GetIndex () %q != %q", errno, 0)
	}

	block1, errno := createStoredBlock(1)
	if errno != 0 {
		t.Errorf("TestFSBlockStore() createStoredBlock() %d != %d", errno, 0)
	}
	defer block1.Dispose()

	block2, errno := createStoredBlock(2)
	if errno != 0 {
		t.Errorf("TestFSBlockStore() createStoredBlock() %d != %d", errno, 0)
	}
	defer block2.Dispose()

	block3, errno := createStoredBlock(3)
	if errno != 0 {
		t.Errorf("TestFSBlockStore() createStoredBlock() %d != %d", errno, 0)
	}
	defer block3.Dispose()

	storedBlock1Index := block1.GetBlockIndex()
	storedBlock1Hash := storedBlock1Index.GetBlockHash()
	var storedBlock1 Longtail_StoredBlock
	completion_api := &testCompletionAPI{}
	errno = blockStoreAPI.GetStoredBlock(storedBlock1Hash, storedBlock1.GetPtr(), CreateAsyncCompleteAPI(completion_api))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() GetStoredBlock() %d == %d", errno, 0)
	}
	if completion_api.err != ENOENT {
		t.Errorf("TestFSBlockStore() GetStoredBlock::OnComplete() %d == %d", completion_api.err, ENOENT)
	}
	if storedBlock1.cStoredBlock != nil {
		t.Errorf("TestFSBlockStore() GetStoredBlock() %p != %p", storedBlock1, Longtail_StoredBlock{cStoredBlock: nil})
	}

	errno = blockStoreAPI.PutStoredBlock(block1, CreateAsyncCompleteAPI(completion_api))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() PutStoredBlock() %d != %d", errno, 0)
	}
	if completion_api.err != 0 {
		t.Errorf("TestFSBlockStore() PutStoredBlock::OnComplete() %d != %d", completion_api.err, 0)
	}
	errno = blockStoreAPI.GetStoredBlock(storedBlock1Hash, storedBlock1.GetPtr(), CreateAsyncCompleteAPI(completion_api))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() GetStoredBlock() %d != %d", errno, 0)
	}
	if completion_api.err != 0 {
		t.Errorf("TestFSBlockStore() GetStoredBlock::OnComplete() %d != %d", completion_api.err, 0)
	}
	defer storedBlock1.Dispose()
	validateStoredBlock(t, storedBlock1)

	errno = blockStoreAPI.PutStoredBlock(block2, CreateAsyncCompleteAPI(completion_api))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() PutStoredBlock() %d != %d", errno, 0)
	}
	if completion_api.err != 0 {
		t.Errorf("TestFSBlockStore() PutStoredBlock::OnComplete() %d != %d", completion_api.err, 0)
	}

	storedBlock2Index := block2.GetBlockIndex()
	storedBlock2Hash := storedBlock2Index.GetBlockHash()
	var storedBlock2 Longtail_StoredBlock
	errno = blockStoreAPI.GetStoredBlock(storedBlock2Hash, storedBlock2.GetPtr(), CreateAsyncCompleteAPI(completion_api))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() HasStoredBlock() %d != %d", errno, 0)
	}
	if completion_api.err != 0 {
		t.Errorf("TestFSBlockStore() PutStoredBlock::OnComplete() %d != %d", completion_api.err, 0)
	}
	expected := Longtail_StoredBlock{}
	if storedBlock2.cStoredBlock == nil {
		t.Errorf("TestFSBlockStore() HasStoredBlock() %v == %v", storedBlock2, expected)
	}

	errno = blockStoreAPI.PutStoredBlock(block3, CreateAsyncCompleteAPI(completion_api))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() PutStoredBlock() %d != %d", errno, 0)
	}
	if completion_api.err != 0 {
		t.Errorf("TestFSBlockStore() PutStoredBlock::OnComplete() %d != %d", completion_api.err, 0)
	}

	storedBlock3Index := block3.GetBlockIndex()
	storedBlock3Hash := storedBlock3Index.GetBlockHash()
	var storedBlock3 Longtail_StoredBlock
	errno = blockStoreAPI.GetStoredBlock(storedBlock3Hash, storedBlock3.GetPtr(), CreateAsyncCompleteAPI(completion_api))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() GetStoredBlock() %d != %d", errno, 0)
	}
	if completion_api.err != 0 {
		t.Errorf("TestFSBlockStore() PutStoredBlock::OnComplete() %d != %d", completion_api.err, 0)
	}
	defer storedBlock3.Dispose()
	validateStoredBlock(t, storedBlock3)

	var storedBlock2Again Longtail_StoredBlock
	errno = blockStoreAPI.GetStoredBlock(storedBlock2Hash, storedBlock2Again.GetPtr(), CreateAsyncCompleteAPI(completion_api))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() GetStoredBlock() %d != %d", errno, 0)
	}
	if completion_api.err != 0 {
		t.Errorf("TestFSBlockStore() PutStoredBlock::OnComplete() %d != %d", completion_api.err, 0)
	}
	defer storedBlock2.Dispose()
	validateStoredBlock(t, storedBlock2)

	contentIndex2, errno := blockStoreAPI.GetIndex(blake3.GetIdentifier(), jobAPI, nil)
	defer contentIndex2.Dispose()
	if errno != 0 {
		t.Errorf("TestFSBlockStore() GetIndex () %d != %d", errno, 0)
	}
	if contentIndex2.GetBlockCount() != uint64(3) {
		t.Errorf("TestFSBlockStore() GetIndex () %q != %q", contentIndex2.GetBlockCount(), uint64(3))
	}
	if contentIndex2.GetChunkCount() != uint64(1+2+3) {
		t.Errorf("TestFSBlockStore() GetIndex () %q != %q", contentIndex2.GetBlockCount(), uint64(1+2+3))
	}
}

type TestBlockStore struct {
	blocks        map[uint64]Longtail_StoredBlock
	blockStoreAPI Longtail_BlockStoreAPI
	lock          sync.Mutex
}

func (b *TestBlockStore) PutStoredBlock(
	storedBlock Longtail_StoredBlock,
	asyncCompleteAPI Longtail_AsyncCompleteAPI) int {
	b.lock.Lock()
	defer b.lock.Unlock()
	blockIndex := storedBlock.GetBlockIndex()
	blockHash := blockIndex.GetBlockHash()
	if _, ok := b.blocks[blockHash]; ok {
		return 0
	}
	blockCopy, errno := CreateStoredBlock(
		blockHash,
		blockIndex.GetTag(),
		blockIndex.GetChunkHashes(),
		blockIndex.GetChunkSizes(),
		storedBlock.GetChunksBlockData(),
		false)
	if errno == 0 {
		b.blocks[blockHash] = blockCopy
	}
	return asyncCompleteAPI.OnComplete(errno)
}

func (b *TestBlockStore) GetStoredBlock(
	blockHash uint64,
	outStoredBlock Longtail_StoredBlockPtr,
	asyncCompleteAPI Longtail_AsyncCompleteAPI) int {
	b.lock.Lock()
	defer b.lock.Unlock()
	if storedBlock, ok := b.blocks[blockHash]; ok {
		if outStoredBlock.cStoredBlockPtr == nil {
			return 0
		}
		blockIndex := storedBlock.GetBlockIndex()
		blockCopy, errno := CreateStoredBlock(
			blockIndex.GetBlockHash(),
			blockIndex.GetTag(),
			blockIndex.GetChunkHashes(),
			blockIndex.GetChunkSizes(),
			storedBlock.GetChunksBlockData(),
			false)
		if errno == 0 {
			*outStoredBlock.cStoredBlockPtr = blockCopy.cStoredBlock
		}
		return asyncCompleteAPI.OnComplete(errno)
	}
	return asyncCompleteAPI.OnComplete(ENOENT)
}

func (b *TestBlockStore) GetIndex(
	defaultHashAPIIdentifier uint32,
	jobAPI Longtail_JobAPI,
	progress Longtail_ProgressAPI) (Longtail_ContentIndex, int) {
	b.lock.Lock()
	defer b.lock.Unlock()
	blockCount := len(b.blocks)
	blockIndexes := make([]Longtail_BlockIndex, blockCount)
	arrayIndex := 0
	for _, value := range b.blocks {
		blockIndexes[arrayIndex] = value.GetBlockIndex()
		arrayIndex++
	}
	cIndex, err := CreateContentIndexFromBlocks(
		defaultHashAPIIdentifier,
		blockIndexes)
	if err != nil {
		return Longtail_ContentIndex{cContentIndex: nil}, ENOMEM
	}
	return cIndex, 0
}

func (b *TestBlockStore) GetStoredBlockPath(blockHash uint64) (string, int) {
	b.lock.Lock()
	defer b.lock.Unlock()
	return fmt.Sprintf("%v", blockHash), 0
}

func (b *TestBlockStore) Close() {
}

func TestBlockStoreProxy(t *testing.T) {
	SetLogger(&testLogger{t: t})
	defer SetLogger(nil)
	SetAssert(&testAssert{t: t})
	defer SetAssert(nil)
	SetLogLevel(1)

	blockStore := &TestBlockStore{blocks: make(map[uint64]Longtail_StoredBlock)}
	blockStoreProxy := CreateBlockStoreAPI(blockStore)
	blockStore.blockStoreAPI = blockStoreProxy
	defer blockStoreProxy.Dispose()

	storedBlock, errno := createStoredBlock(2)
	if errno != 0 {
		t.Errorf("TestBlockStoreProxy() createStoredBlock() %d != %d", errno, 0)
	}
	defer storedBlock.Dispose()

	completion_api := &testCompletionAPI{}
	errno = blockStoreProxy.PutStoredBlock(storedBlock, CreateAsyncCompleteAPI(completion_api))
	if errno != 0 {
		t.Errorf("TestBlockStoreProxy() PutStoredBlock() %d != %d", errno, 0)
	}
	if completion_api.err != 0 {
		t.Errorf("TestBlockStoreProxy() PutStoredBlock::OnComplete() %d != %d", completion_api.err, 0)
	}
	storedBlockIndex := storedBlock.GetBlockIndex()
	var getBlock Longtail_StoredBlock
	errno = blockStoreProxy.GetStoredBlock(storedBlockIndex.GetBlockHash(), getBlock.GetPtr(), CreateAsyncCompleteAPI(completion_api))
	if errno != 0 {
		t.Errorf("TestBlockStoreProxy() GetStoredBlock() %d!= %d", errno, 0)
	}
	if completion_api.err != 0 {
		t.Errorf("TestFSBlockStore() PutStoredBlock::OnComplete() %d != %d", completion_api.err, 0)
	}
	defer getBlock.Dispose()
	validateStoredBlock(t, getBlock)
	getBlockIndex := getBlock.GetBlockIndex()
	blockPath, errno := blockStoreProxy.GetStoredBlockPath(getBlockIndex.GetBlockHash())
	if errno != 0 {
		t.Errorf("TestBlockStoreProxy() GetStoredBlockPath() %d != %d", errno, 0)
	}
	if blockPath != fmt.Sprintf("%v", getBlockIndex.GetBlockHash()) {
		t.Errorf("TestBlockStoreProxy() GetStoredBlockPath() %s != %s", blockPath, fmt.Sprintf("%v", getBlockIndex.GetBlockHash()))
	}
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	if jobAPI.cJobAPI == nil {
		t.Errorf("TestBlockStoreProxy() CreateBikeshedJobAPI() jobAPI.cJobAPI == nil")
	}
	defer jobAPI.Dispose()
	contentIndex, errno := blockStoreProxy.GetIndex(GetBlake3HashIdentifier(), jobAPI, nil)
	if errno != 0 {
		t.Errorf("TestBlockStoreProxy() GetIndex() %d != %d", errno, 0)
	}
	defer contentIndex.Dispose()
}

func TestBlockStoreProxyFull(t *testing.T) {
	storageAPI := createFilledStorage("content")
	defer storageAPI.Dispose()
	hashAPI := CreateBlake3HashAPI()
	defer hashAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()
	testBlockStore := &TestBlockStore{blocks: make(map[uint64]Longtail_StoredBlock)}
	blockStoreAPI := CreateBlockStoreAPI(testBlockStore)
	defer blockStoreAPI.Dispose()
	fileInfos, err := GetFilesRecursively(storageAPI, "content")
	if err != nil {
		t.Errorf("TestBlockStoreProxyFull() GetFilesRecursively() %q != %v", err, nil)
	}
	defer fileInfos.Dispose()
	tags := make([]uint32, fileInfos.GetFileCount())
	versionIndex, err := CreateVersionIndex(
		storageAPI,
		hashAPI,
		jobAPI,
		nil,
		"content",
		fileInfos.GetPaths(),
		fileInfos.GetFileSizes(),
		fileInfos.GetFilePermissions(),
		tags,
		32768)
	if err != nil {
		t.Errorf("TestBlockStoreProxyFull() CreateVersionIndex() %q != %v", err, nil)
	}
	defer versionIndex.Dispose()
	contentIndex, err := CreateContentIndex(
		hashAPI,
		versionIndex.GetChunkHashes(),
		versionIndex.GetChunkSizes(),
		versionIndex.GetChunkTags(),
		32768*2,
		8)
	if err != nil {
		t.Errorf("TestBlockStoreProxyFull() CreateContentIndex() %q != %v", err, nil)
	}
	defer contentIndex.Dispose()
	blockStoreContentIndex, errno := blockStoreAPI.GetIndex(hashAPI.GetIdentifier(), jobAPI, nil)
	if errno != 0 {
		t.Errorf("TestBlockStoreProxyFull() blockStoreAPI.GetIndex() %d != %d", errno, 0)
	}
	defer blockStoreContentIndex.Dispose()
	err = WriteContent(
		storageAPI,
		blockStoreAPI,
		jobAPI,
		nil,
		blockStoreContentIndex,
		contentIndex,
		versionIndex,
		"content")
	if err != nil {
		t.Errorf("TestBlockStoreProxyFull() WriteContent() %q != %v", err, nil)
	}
}

func randomArray(size int) []byte {
	r := make([]byte, size)
	rand.Read(r)
	return r
}

func createFilledStorage(rootPath string) Longtail_StorageAPI {
	storageAPI := CreateInMemStorageAPI()
	storageAPI.WriteToStorage(rootPath, "first_folder/my_file.txt", []byte("the content of my_file"))
	storageAPI.WriteToStorage(rootPath, "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	storageAPI.WriteToStorage(rootPath, "top_level.txt", []byte("the top level file is also a text file with dummy content"))
	storageAPI.WriteToStorage(rootPath, "first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})
	storageAPI.WriteToStorage(rootPath, "bin/small.bin", randomArray(8192))
	storageAPI.WriteToStorage(rootPath, "bin/huge.bin", randomArray(65535*16))
	storageAPI.WriteToStorage(rootPath, "bin/medium.bin", randomArray(32768))
	storageAPI.WriteToStorage(rootPath, "bin/large.bin", randomArray(65535))
	return storageAPI
}

func TestGetFileRecursively(t *testing.T) {
	storageAPI := createFilledStorage("content")
	fileInfos, err := GetFilesRecursively(storageAPI, "content")
	if err != nil {
		t.Errorf("TestGetFileRecursively() GetFilesRecursively() %q != %q", err, error(nil))
	}
	defer fileInfos.Dispose()
	fileCount := fileInfos.GetFileCount()
	if fileCount != 19 {
		t.Errorf("TestGetFileRecursively() GetFileCount() %d != %d", fileCount, 19)
	}
	fileSizes := fileInfos.GetFileSizes()
	if len(fileSizes) != int(fileCount) {
		t.Errorf("TestGetFileRecursively() GetFileSizes() %d != %d", len(fileSizes), fileCount)
	}
	permissions := fileInfos.GetFilePermissions()
	if len(permissions) != int(fileCount) {
		t.Errorf("TestGetFileRecursively() GetFilePermissions() %d != %d", len(permissions), fileCount)
	}
	path := fileInfos.GetPaths().GetPath(0)
	if path != "first_folder/" {
		t.Errorf("TestGetFileRecursively() GetPaths().GetPath() %s != %s", path, "first_folder/")
	}
}

func TestCreateVersionIndex(t *testing.T) {
	storageAPI := createFilledStorage("content")
	fileInfos, err := GetFilesRecursively(storageAPI, "content")
	if err != nil {
		t.Errorf("TestCreateVersionIndex() GetFilesRecursively() %q != %q", err, error(nil))
	}
	hashAPI := CreateBlake2HashAPI()
	defer hashAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()

	compressionTypes := make([]uint32, fileInfos.GetFileCount())

	versionIndex, err := CreateVersionIndex(
		storageAPI,
		hashAPI,
		jobAPI,
		nil,
		"content",
		fileInfos.GetPaths(),
		fileInfos.GetFileSizes(),
		fileInfos.GetFilePermissions(),
		compressionTypes,
		32768)

	if err != nil {
		t.Errorf("TestCreateVersionIndex() CreateVersionIndex() %q != %q", err, error(nil))
	}
	defer versionIndex.Dispose()
	if versionIndex.GetHashAPI() != hashAPI.GetIdentifier() {
		t.Errorf("TestCreateVersionIndex() GetHashAPI() %d != %d", versionIndex.GetHashAPI(), hashAPI.GetIdentifier())
	}
	if versionIndex.GetAssetCount() != fileInfos.GetFileCount() {
		t.Errorf("TestCreateVersionIndex() GetAssetCount() %d != %d", versionIndex.GetAssetCount(), fileInfos.GetFileCount())
	}
}

func TestCreateContentIndex(t *testing.T) {
	hashAPI := CreateBlake3HashAPI()
	defer hashAPI.Dispose()
	chunkHashes := make([]uint64, 2)
	chunkHashes[0] = 4711
	chunkHashes[1] = 1147
	chunkSizes := make([]uint32, 2)
	chunkSizes[0] = 889
	chunkSizes[0] = 998
	compressionTypes := make([]uint32, 2)
	compressionTypes[0] = GetZStdDefaultCompressionType()
	compressionTypes[1] = GetZStdDefaultCompressionType()
	contentIndex, err := CreateContentIndex(
		hashAPI,
		chunkHashes,
		chunkSizes,
		compressionTypes,
		65536,
		4096)
	if err != error(nil) {
		t.Errorf("TestCreateContentIndex() CreateContentIndex() %q != %q", err, error(nil))
	}
	defer contentIndex.Dispose()
	if contentIndex.GetChunkCount() != uint64(len(chunkHashes)) {
		t.Errorf("TestCreateContentIndex() GetChunkCount() %d != %d", contentIndex.GetChunkCount(), len(chunkHashes))
	}
	if contentIndex.GetBlockCount() != 1 {
		t.Errorf("TestCreateContentIndex() GetChunkCount() %d != %d", contentIndex.GetBlockCount(), 1)
	}
}

func TestRewriteVersion(t *testing.T) {
	storageAPI := createFilledStorage("content")
	fileInfos, err := GetFilesRecursively(storageAPI, "content")
	if err != nil {
		t.Errorf("TestRewriteVersion() GetFilesRecursively() %q != %q", err, error(nil))
	}
	hashAPI := CreateBlake2HashAPI()
	defer hashAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()

	compressionTypes := make([]uint32, fileInfos.GetFileCount())

	createVersionProgress := CreateProgressAPI(&testProgress{task: "CreateVersionIndex", t: t})
	versionIndex, err := CreateVersionIndex(
		storageAPI,
		hashAPI,
		jobAPI,
		&createVersionProgress,
		"content",
		fileInfos.GetPaths(),
		fileInfos.GetFileSizes(),
		fileInfos.GetFilePermissions(),
		compressionTypes,
		32768)
	if err != nil {
		t.Errorf("TestRewriteVersion() CreateVersionIndex() %q != %q", err, error(nil))
	}

	contentIndex, err := CreateContentIndex(
		hashAPI,
		versionIndex.GetChunkHashes(),
		versionIndex.GetChunkSizes(),
		versionIndex.GetChunkTags(),
		65536,
		4096)
	if err != nil {
		t.Errorf("TestRewriteVersion() CreateContentIndex() %q != %q", err, error(nil))
	}
	defer contentIndex.Dispose()
	blockStorageAPI := CreateFSBlockStoreAPI(storageAPI, "block_store")
	defer blockStorageAPI.Dispose()
	compressionRegistry := CreateFullCompressionRegistry()
	compressionRegistry.Dispose()
	writeContentProgress := CreateProgressAPI(&testProgress{task: "WriteContent", t: t})
	defer writeContentProgress.Dispose()
	blockStoreContentIndex, errno := blockStorageAPI.GetIndex(hashAPI.GetIdentifier(), jobAPI, nil)
	if errno != 0 {
		t.Errorf("TestBlockStoreProxyFull() blockStoreAPI.GetIndex() %d != %d", errno, 0)
	}
	defer blockStoreContentIndex.Dispose()
	err = WriteContent(
		storageAPI,
		blockStorageAPI,
		jobAPI,
		&writeContentProgress,
		blockStoreContentIndex,
		contentIndex,
		versionIndex,
		"content")
	if err != nil {
		t.Errorf("TestRewriteVersion() WriteContent() %q != %q", err, error(nil))
	}

	writeVersionProgress2 := CreateProgressAPI(&testProgress{task: "WriteVersion", t: t})
	err = WriteVersion(
		blockStorageAPI,
		storageAPI,
		jobAPI,
		&writeVersionProgress2,
		contentIndex,
		versionIndex,
		"content_copy",
		true)
	if err != nil {
		t.Errorf("TestRewriteVersion() WriteVersion() %q != %q", err, error(nil))
	}
}

/*
// CreateVersionIndexUtil ...
func CreateVersionIndexUtil(
	storageAPI Longtail_StorageAPI,
	hashAPI Longtail_HashAPI,
	jobAPI Longtail_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	versionPath string,
	compressionType uint32,
	targetChunkSize uint32) (Longtail_VersionIndex, error) {

	fileInfos, err := GetFilesRecursively(storageAPI, versionPath)
	if err != nil {
		return Longtail_VersionIndex{cVersionIndex: nil}, err
	}
	defer fileInfos.Dispose()

	pathCount := fileInfos.GetFileCount()
	compressionTypes := make([]uint32, pathCount)
	for i := uint32(0); i < pathCount; i++ {
		compressionTypes[i] = compressionType
	}

	vindex, err := CreateVersionIndex(
		storageAPI,
		hashAPI,
		jobAPI,
		progressFunc,
		progressContext,
		versionPath,
		fileInfos.GetPaths(),
		fileInfos.GetFileSizes(),
		fileInfos.GetFilePermissions(),
		compressionTypes,
		targetChunkSize)

	return vindex, err
}

//GetMissingContentUtil ... this is handy, but not what we should expose other than for tests!
func GetMissingContentUtil(
	contentBlockStoreAPI Longtail_BlockStoreAPI,
	missingContentBlockStoreAPI Longtail_BlockStoreAPI,
	versionStorageAPI Longtail_StorageAPI,
	hashAPI Longtail_HashAPI,
	jobAPI Longtail_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	versionPath string,
	versionIndexPath string,
	contentIndexStorageAPI Longtail_StorageAPI,
	contentIndexPath string,
	compressionType uint32,
	maxChunksPerBlock uint32,
	targetBlockSize uint32,
	targetChunkSize uint32) (Longtail_ContentIndex, error) {

	var vindex Longtail_VersionIndex
	err := error(nil)

	if len(versionIndexPath) > 0 {
		vindex, err = ReadVersionIndex(versionStorageAPI, versionIndexPath)
	}
	if err != nil {
		vindex, err = CreateVersionIndexUtil(
			versionStorageAPI,
			hashAPI,
			jobAPI,
			progressFunc,
			progressContext,
			versionPath,
			compressionType,
			targetChunkSize)

		if err != nil {
			return Longtail_ContentIndex{cContentIndex: nil}, err
		}
	}
	defer vindex.Dispose()

	var cindex Longtail_ContentIndex

	if len(contentIndexPath) > 0 {
		cindex, err = ReadContentIndex(contentIndexStorageAPI, contentIndexPath)
	}
	if err != nil {
		cindex, err = contentBlockStoreAPI.GetIndex(
			jobAPI,
			hashAPI.GetIdentifier(),
			progressFunc,
			progressContext)
		if err != nil {
			return Longtail_ContentIndex{cContentIndex: nil}, err
		}
	}
	defer cindex.Dispose()

	missingContentIndex, err := CreateMissingContent(
		hashAPI,
		cindex,
		vindex,
		targetBlockSize,
		maxChunksPerBlock)

	if err != nil {
		return Longtail_ContentIndex{cContentIndex: nil}, err
	}

	compressionRegistry := CreateFullCompressionRegistry()
	defer compressionRegistry.Dispose()

	err = WriteContent(
		versionStorageAPI,
		missingContentBlockStoreAPI,
		compressionRegistry,
		jobAPI,
		progressFunc,
		progressContext,
		missingContentIndex,
		vindex,
		versionPath)

	if err != nil {
		missingContentIndex.Dispose()
		return Longtail_ContentIndex{cContentIndex: nil}, err
	}

	if len(versionIndexPath) > 0 {
		err = WriteVersionIndex(
			contentIndexStorageAPI,
			vindex,
			versionIndexPath)
		if err != nil {
			missingContentIndex.Dispose()
			return Longtail_ContentIndex{cContentIndex: nil}, err
		}
	}

	if len(contentIndexPath) > 0 {
		err = WriteContentIndex(
			contentIndexStorageAPI,
			cindex,
			contentIndexPath)
		if err != nil {
			missingContentIndex.Dispose()
			return Longtail_ContentIndex{cContentIndex: nil}, err
		}
	}
	return missingContentIndex, nil
}

func TestInit(t *testing.T) {
	l := SetLogger(logger, &loggerData{t: t})
	defer ClearLogger(l)
	SetLogLevel(3)

	storageAPI := CreateInMemStorageAPI()
	if storageAPI.cStorageAPI == nil {
		t.Errorf("CreateInMemStorageAPI() storageAPI.cStorageAPI == nil")
	}
	defer storageAPI.Dispose()
	blake2API := CreateBlake2HashAPI()
	if blake2API.cHashAPI == nil {
		t.Errorf("CreateBlake2HashAPI() hashAPI.cHashAPI == nil")
	}
	defer blake2API.Dispose()
	blake3API := CreateBlake3HashAPI()
	if blake3API.cHashAPI == nil {
		t.Errorf("CreateBlake3HashAPI() hashAPI.cHashAPI == nil")
	}
	defer blake3API.Dispose()
	meowAPI := CreateMeowHashAPI()
	if meowAPI.cHashAPI == nil {
		t.Errorf("CreateMeowHashAPI() hashAPI.cHashAPI == nil")
	}
	defer meowAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	if jobAPI.cJobAPI == nil {
		t.Errorf("CreateBikeshedJobAPI() jobAPI.cJobAPI == nil")
	}
	defer jobAPI.Dispose()
}

func TestCreateVersionIndex(t *testing.T) {
	l := SetLogger(logger, &loggerData{t: t})
	defer ClearLogger(l)
	SetLogLevel(3)

	storageAPI := CreateInMemStorageAPI()
	defer storageAPI.Dispose()
	hashAPI := CreateBlake2HashAPI()
	defer hashAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()

	WriteToStorage(storageAPI, "", "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(storageAPI, "", "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(storageAPI, "", "top_level.txt", []byte("the top level file is also a text file with dummy content"))
	WriteToStorage(storageAPI, "", "first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})

	vi, err := CreateVersionIndexUtil(
		storageAPI,
		hashAPI,
		jobAPI,
		progress,
		&testProgress{task: "Indexing", t: t},
		"",
		GetBrotliGenericDefaultCompressionType(),
		32768)

	expected := error(nil)
	if err != nil {
		t.Errorf("CreateVersionIndex() %q != %q", err, expected)
	}
	defer vi.Dispose()

	expectedAssetCount := uint32(14)
	if ret := vi.GetAssetCount(); ret != expectedAssetCount {
		t.Errorf("CreateVersionIndex() asset count = %d, want %d", ret, expectedAssetCount)
	}
	expectedChunkCount := uint32(3)
	if ret := vi.GetChunkCount(); ret != expectedChunkCount {
		t.Errorf("CreateVersionIndex() chunk count = %d, want %d", ret, expectedChunkCount)
	}
}

func TestReadWriteVersionIndex(t *testing.T) {
	l := SetLogger(logger, &loggerData{t: t})
	defer ClearLogger(l)
	SetLogLevel(3)

	storageAPI := CreateInMemStorageAPI()
	defer storageAPI.Dispose()
	hashAPI := CreateBlake3HashAPI()
	defer hashAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()

	WriteToStorage(storageAPI, "", "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(storageAPI, "", "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(storageAPI, "", "top_level.txt", []byte("the top level file is also a text file with dummy content"))
	WriteToStorage(storageAPI, "", "first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})

	fileInfos, err := GetFilesRecursively(storageAPI, "")
	expected := error(nil)
	if err != nil {
		t.Errorf("GetFilesRecursively() %q != %q", err, expected)
	}
	defer fileInfos.Dispose()

	pathCount := fileInfos.GetFileCount()
	compressionTypes := make([]uint32, pathCount)
	for i := uint32(0); i < pathCount; i++ {
		compressionTypes[i] = GetLZ4DefaultCompressionType()
	}

	vi, err := CreateVersionIndex(
		storageAPI,
		hashAPI,
		jobAPI,
		progress,
		&testProgress{task: "Indexing", t: t},
		"",
		fileInfos.GetPaths(),
		fileInfos.GetFileSizes(),
		fileInfos.GetFilePermissions(),
		compressionTypes,
		32768)

	if err != nil {
		t.Errorf("CreateVersionIndex() %q != %q", err, expected)
	}
	defer vi.Dispose()

	if ret := WriteVersionIndex(storageAPI, vi, "test.lvi"); ret != expected {
		t.Errorf("WriteVersionIndex() = %q, want %q", ret, expected)
	}

	vi2, ret := ReadVersionIndex(storageAPI, "test.lvi")
	if ret != expected {
		t.Errorf("WriteVersionIndex() = %q, want %q", ret, expected)
	}
	defer vi2.Dispose()

	if vi2.GetAssetCount() != vi.GetAssetCount() {
		t.Errorf("ReadVersionIndex() asset count = %d, want %d", vi2.GetAssetCount(), vi.GetAssetCount())
	}

	for i := uint32(0); i < vi.GetAssetCount(); i++ {
		expected := GetVersionIndexPath(vi, i)
		if ret := GetVersionIndexPath(vi2, i); ret != expected {
			t.Errorf("ReadVersionIndex() path %d = %s, want %s", int(i), ret, expected)
		}
	}

	buf, err := WriteVersionIndexToBuffer(vi)
	if err != nil {
		t.Errorf("WriteVersionIndexToBuffer() %q != %q", err, expected)
	}
	viCopy, err := ReadVersionIndexFromBuffer(buf)
	if err != nil {
		t.Errorf("WriteVersionIndexToBuffer() %q != %q", err, expected)
	}
	defer viCopy.Dispose()
}

type assertData struct {
	t *testing.T
}

func testAssertFunc(context interface{}, expression string, file string, line int) {
	fmt.Printf("ASSERT: %s %s:%d", expression, file, line)
}

func TestUpSyncVersion(t *testing.T) {
	l := SetLogger(logger, &loggerData{t: t})
	defer ClearLogger(l)
	SetLogLevel(0)

	SetAssert(testAssertFunc, &assertData{t: t})
	defer ClearAssert()

	upsyncStorageAPI := CreateInMemStorageAPI()
	defer upsyncStorageAPI.Dispose()
	hashAPI := CreateMeowHashAPI()
	defer hashAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()
	remoteStorageAPI := CreateInMemStorageAPI()
	defer remoteStorageAPI.Dispose()

	t.Logf("Creating `current`")
	WriteToStorage(upsyncStorageAPI, "current", "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(upsyncStorageAPI, "current", "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(upsyncStorageAPI, "current", "top_level.txt", []byte("the top level file is also a text file with dummy content"))
	WriteToStorage(upsyncStorageAPI, "current", "first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})

	t.Logf("Reading remote `store.lci`")
	storeIndex, err := ReadContentIndex(remoteStorageAPI, "store.lci")
	if err != nil {
		storeIndex, err = CreateContentIndex(hashAPI, 0, nil, nil, nil, 32768*12, 4096)
		if err != nil {
			t.Errorf("CreateContentIndex() err = %q, want %q", err, error(nil))
		}
	}
	defer storeIndex.Dispose()
	err = WriteContentIndex(upsyncStorageAPI, storeIndex, "store.lci")
	if err != nil {
		t.Errorf("WriteContentIndex() err = %q, want %q", err, error(nil))
	}

	blockStore := CreateFSBlockStore(upsyncStorageAPI, "cache")
	defer blockStore.Dispose()

	t.Logf("Get missing content for `current` / `cache`")
	missingContentIndex, err := GetMissingContentUtil(
		blockStore,
		blockStore,
		upsyncStorageAPI,
		hashAPI,
		jobAPI,
		progress,
		&testProgress{task: "Indexing", t: t},
		"current",
		"current.lvi",
		upsyncStorageAPI,
		"cache.lci",
		GetZStdDefaultCompressionType(),
		4096,
		32768,
		32758*12)
	if err != nil {
		t.Errorf("GetMissingContent() err = %q, want %q", err, error(nil))
	}
	defer missingContentIndex.Dispose()

	expected := error(nil)
	buf, err := WriteContentIndexToBuffer(missingContentIndex)
	if err != nil {
		t.Errorf("WriteContentIndexToBuffer() %q != %q", err, expected)
	}
	ciCopy, err := ReadContentIndexFromBuffer(buf)
	if err != nil {
		t.Errorf("ReadContentIndexFromBuffer() %q != %q", err, expected)
	}
	defer ciCopy.Dispose()

	var expectedBlockCount uint64 = 1
	if missingContentIndex.GetBlockCount() != expectedBlockCount {
		t.Errorf("UpSyncVersion() len(blockHashes) = %d, want %d", missingContentIndex.GetBlockCount(), expectedBlockCount)
	}

	t.Logf("Copying blocks from `cache` / `store`")
	for i := uint64(0); i < missingContentIndex.GetBlockCount(); i++ {
		blockHash := missingContentIndex.GetBlockHash(i)
		path, err := blockStore.GetStoredBlockPath(blockHash)
		if err != nil {
			t.Errorf("UpSyncVersion() ReadFromStorage(%d, %s) = %q, want %q", "cache", blockHash, err, error(nil))
		}
		block, err := ReadFromStorage(upsyncStorageAPI, "cache", path)
		if err != nil {
			t.Errorf("UpSyncVersion() ReadFromStorage(%s, %s) = %q, want %q", "cache", path, err, error(nil))
		}
		err = WriteToStorage(remoteStorageAPI, "store", path, block)
		if err != nil {
			t.Errorf("UpSyncVersion() WriteToStorage(%s, %s) = %q, want %q", "store", path, err, error(nil))
		}
		t.Logf("Copied block: `%s` from `%s` to `%s`", path, "cache", "store")
	}

	t.Logf("Reading remote index from `store`")
	version, err := ReadFromStorage(upsyncStorageAPI, "", "current.lvi")
	if err != nil {
		t.Errorf("UpSyncVersion() ReadFromStorage(%s, %s) = %q, want %q", "", "local.lci", err, error(nil))
	}
	err = WriteToStorage(remoteStorageAPI, "", "version.lvi", version)
	if err != nil {
		t.Errorf("UpSyncVersion() WriteToStorage(%s, %s) = %q, want %q", "", "version.lvi", err, error(nil))
	}

	t.Logf("Updating remote index from `store`")
	storeIndex, err = MergeContentIndex(storeIndex, missingContentIndex)
	if err != nil {
		t.Errorf("UpSyncVersion() MergeContentIndex() err = %q, want %q", err, error(nil))
	}
	WriteContentIndex(remoteStorageAPI, storeIndex, "store.lci")

	t.Logf("Starting downsync to `current`")
	downSyncStorageAPI := CreateInMemStorageAPI()
	defer downSyncStorageAPI.Dispose()

	t.Logf("Reading remote index from `store`")
	remoteStorageIndex, err := ReadContentIndex(remoteStorageAPI, "store.lci")
	if err != nil {
		t.Errorf("UpSyncVersion() ReadContentIndex(%s) = %q, want %q", "store.lci", err, error(nil))
	}
	defer remoteStorageIndex.Dispose()
	t.Logf("Blocks in store: %d", remoteStorageIndex.GetBlockCount())

	t.Logf("Reading version index from `version.lvi`")
	targetVersionIndex, err := ReadVersionIndex(remoteStorageAPI, "version.lvi")
	if err != nil {
		t.Errorf("UpSyncVersion() ReadVersionIndex(%s) = %q, want %q", "version.lvi", err, error(nil))
	}
	defer targetVersionIndex.Dispose()
	t.Logf("Assets in version: %d", targetVersionIndex.GetAssetCount())

	cacheContentIndex, err := ReadContentIndex(downSyncStorageAPI, "cache.lci")
	if err != nil {
		cacheContentIndex, err = ReadContent(
			downSyncStorageAPI,
			hashAPI,
			jobAPI,
			progress,
			&testProgress{task: "Reading local cache", t: t},
			"cache")
		if err != nil {
			t.Errorf("UpSyncVersion() ReadContent(%s) = %q, want %q", "cache", err, error(nil))
		}
	}
	defer cacheContentIndex.Dispose()
	t.Logf("Blocks in cacheContentIndex: %d", cacheContentIndex.GetBlockCount())

	missingContentIndex, err = CreateMissingContent(
		hashAPI,
		cacheContentIndex,
		targetVersionIndex,
		32758*12,
		4096)
	if err != nil {
		t.Errorf("UpSyncVersion() CreateMissingContent() = %q, want %q", err, error(nil))
	}
	t.Logf("Blocks in missingContentIndex: %d", missingContentIndex.GetBlockCount())

	requestContent, err := RetargetContent(
		remoteStorageIndex,
		missingContentIndex)
	if err != nil {
		t.Errorf("UpSyncVersion() RetargetContent() = %q, want %q", err, error(nil))
	}
	defer requestContent.Dispose()
	t.Logf("Blocks in requestContent: %d", requestContent.GetBlockCount())

	missingPaths, err = GetPathsForContentBlocks(requestContent)
	if err != nil {
		t.Errorf("UpSyncVersion() GetPathsForContentBlocks() = %q, want %q", err, error(nil))
	}
	t.Logf("Path count for content: %d", missingPaths.GetPathCount())
	for i := uint32(0); i < missingPaths.GetPathCount(); i++ {
		path := GetPath(missingPaths, uint32(i))
		block, err := ReadFromStorage(remoteStorageAPI, "store", path)
		if err != nil {
			t.Errorf("UpSyncVersion() ReadFromStorage(%s, %s) = %q, want %q", "store", path, err, error(nil))
		}
		err = WriteToStorage(downSyncStorageAPI, "cache", path, block)
		if err != nil {
			t.Errorf("UpSyncVersion() WriteToStorage(%s, %s) = %q, want %q", "cache", path, err, error(nil))
		}
		t.Logf("Copied block: `%s` from `%s` to `%s`", path, "store", "cache")
	}
	defer missingPaths.Dispose()

	mergedCacheContentIndex, err := MergeContentIndex(cacheContentIndex, requestContent)
	if err != nil {
		t.Errorf("UpSyncVersion() MergeContentIndex(%s, %s) = %q, want %q", "cache", "store", err, error(nil))
	}
	t.Logf("Blocks in cacheContentIndex after merge: %d", mergedCacheContentIndex.GetBlockCount())
	defer mergedCacheContentIndex.Dispose()

	compressionRegistry := CreateFullCompressionRegistry()
	defer compressionRegistry.Dispose()
	t.Log("Created compression registry")

	currentVersionIndex, err := CreateVersionIndexUtil(
		downSyncStorageAPI,
		hashAPI,
		jobAPI,
		progress,
		&testProgress{task: "Indexing version", t: t},
		"current",
		GetBrotliGenericDefaultCompressionType(),
		32768)
	if err != nil {
		t.Errorf("UpSyncVersion() CreateVersionIndex(%s) = %q, want %q", "current", err, error(nil))
	}
	defer currentVersionIndex.Dispose()
	t.Logf("Asset count for currentVersionIndex: %d", currentVersionIndex.GetAssetCount())

	versionDiff, err := CreateVersionDiff(currentVersionIndex, targetVersionIndex)
	if err != nil {
		t.Errorf("UpSyncVersion() CreateVersionDiff(%s, %s) = %q, want %q", "current", "version.lvi", err, error(nil))
	}
	defer versionDiff.Dispose()

	err = ChangeVersion(
		downSyncStorageAPI,
		downSyncStorageAPI,
		hashAPI,
		jobAPI,
		progress,
		&testProgress{task: "Updating version", t: t},
		compressionRegistry,
		mergedCacheContentIndex,
		currentVersionIndex,
		targetVersionIndex,
		versionDiff,
		"cache",
		"current",
		true)
	if err != nil {
		t.Errorf("UpSyncVersion() ChangeVersion(%s, %s) = %q, want %q", "cache", "current", err, error(nil))
	}
}
*/
