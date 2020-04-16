package longtaillib

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

type testPutBlockCompletionAPI struct {
	wg  sync.WaitGroup
	err int
}

func (a *testPutBlockCompletionAPI) OnComplete(err int) {
	a.err = err
	a.wg.Done()
}

type testGetBlockCompletionAPI struct {
	wg          sync.WaitGroup
	storedBlock Longtail_StoredBlock
	err         int
}

func (a *testGetBlockCompletionAPI) OnComplete(storedBlock Longtail_StoredBlock, err int) {
	a.storedBlock = storedBlock
	a.err = err
	a.wg.Done()
}

type testGetIndexCompletionAPI struct {
	wg           sync.WaitGroup
	contentIndex Longtail_ContentIndex
	err          int
}

func (a *testGetIndexCompletionAPI) OnComplete(contentIndex Longtail_ContentIndex, err int) {
	a.err = err
	a.contentIndex = contentIndex
	a.wg.Done()
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

	compressionRegistry := CreateZStdCompressionRegistry()
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
		for index := uint32(0); index < uint32(chunkSizes[chunkIndex]); index++ {
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
		for index := uint32(0); index < uint32(chunkSizes[chunkIndex]); index++ {
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

	storedBlockData, err := WriteStoredBlockToBuffer(originalBlock)
	if err != nil {
		t.Errorf("WriteStoredBlockToBuffer() %q != %q", err, error(nil))
	}
	originalBlock.Dispose()

	copyBlock, err := ReadStoredBlockFromBuffer(storedBlockData)

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
	blockStoreAPI := CreateFSBlockStore(storageAPI, "content")
	defer blockStoreAPI.Dispose()
	blake3 := CreateBlake3HashAPI()
	defer blake3.Dispose()

	getIndexComplete := &testGetIndexCompletionAPI{}
	getIndexComplete.wg.Add(1)
	errno := blockStoreAPI.GetIndex(blake3.GetIdentifier(), CreateAsyncGetIndexAPI(getIndexComplete))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() GetIndex () %q != %q", errno, 0)
		getIndexComplete.wg.Done()
	}
	getIndexComplete.wg.Wait()
	contentIndex := getIndexComplete.contentIndex
	defer contentIndex.Dispose()

	block1, errno := createStoredBlock(1)
	if errno != 0 {
		t.Errorf("TestFSBlockStore() createStoredBlock() %d != %d", errno, 0)
	}
	defer block1.Dispose()

	block2, errno := createStoredBlock(5)
	if errno != 0 {
		t.Errorf("TestFSBlockStore() createStoredBlock() %d != %d", errno, 0)
	}
	defer block2.Dispose()

	block3, errno := createStoredBlock(9)
	if errno != 0 {
		t.Errorf("TestFSBlockStore() createStoredBlock() %d != %d", errno, 0)
	}
	defer block3.Dispose()

	storedBlock1Index := block1.GetBlockIndex()
	storedBlock1Hash := storedBlock1Index.GetBlockHash()
	getStoredBlockComplete := &testGetBlockCompletionAPI{}
	getStoredBlockComplete.wg.Add(1)
	errno = blockStoreAPI.GetStoredBlock(storedBlock1Hash, CreateAsyncGetStoredBlockAPI(getStoredBlockComplete))
	if errno != ENOENT {
		t.Errorf("TestFSBlockStore() GetStoredBlock() %d == %d", errno, ENOENT)
		getStoredBlockComplete.wg.Done()
	}
	getStoredBlockComplete.wg.Done()
	nullBlock := Longtail_StoredBlock{}
	if getStoredBlockComplete.storedBlock != nullBlock {
		t.Errorf("TestFSBlockStore() getStoredBlockComplete.storedBlock %p != %p", getStoredBlockComplete.storedBlock, nullBlock)
	}

	putStoredBlockComplete := &testPutBlockCompletionAPI{}
	putStoredBlockComplete.wg.Add(1)
	errno = blockStoreAPI.PutStoredBlock(block1, CreateAsyncPutStoredBlockAPI(putStoredBlockComplete))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() PutStoredBlock() %d != %d", errno, 0)
		putStoredBlockComplete.wg.Done()
	}
	putStoredBlockComplete.wg.Wait()
	if putStoredBlockComplete.err != 0 {
		t.Errorf("TestFSBlockStore() putStoredBlockComplete.err %d != %d", putStoredBlockComplete.err, 0)
	}

	getStoredBlockComplete.wg.Add(1)
	errno = blockStoreAPI.GetStoredBlock(storedBlock1Hash, CreateAsyncGetStoredBlockAPI(getStoredBlockComplete))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() GetStoredBlock() %d != %d", errno, 0)
		getStoredBlockComplete.wg.Done()
	}
	getStoredBlockComplete.wg.Wait()
	if getStoredBlockComplete.err != 0 {
		t.Errorf("TestFSBlockStore() getStoredBlockComplete.err %d != %d", getStoredBlockComplete.err, 0)
	}
	storedBlock1 := getStoredBlockComplete.storedBlock
	getStoredBlockComplete.storedBlock = nullBlock
	if storedBlock1 == nullBlock {
		t.Errorf("TestFSBlockStore() HasStoredBlock() %v != %v", storedBlock1, nullBlock)
	}
	defer storedBlock1.Dispose()
	validateStoredBlock(t, storedBlock1)

	putStoredBlockComplete.wg.Add(1)
	errno = blockStoreAPI.PutStoredBlock(block2, CreateAsyncPutStoredBlockAPI(putStoredBlockComplete))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() PutStoredBlock() %d != %d", errno, 0)
		putStoredBlockComplete.wg.Done()
	}
	putStoredBlockComplete.wg.Wait()
	if putStoredBlockComplete.err != 0 {
		t.Errorf("TestFSBlockStore() putStoredBlockComplete.err %d != %d", putStoredBlockComplete.err, 0)
	}

	storedBlock2Index := block2.GetBlockIndex()
	storedBlock2Hash := storedBlock2Index.GetBlockHash()
	getStoredBlockComplete.wg.Add(1)
	errno = blockStoreAPI.GetStoredBlock(storedBlock2Hash, CreateAsyncGetStoredBlockAPI(getStoredBlockComplete))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() HasStoredBlock() %d != %d", errno, 0)
		getStoredBlockComplete.wg.Done()
	}
	getStoredBlockComplete.wg.Wait()
	if getStoredBlockComplete.err != 0 {
		t.Errorf("TestFSBlockStore() getStoredBlockComplete.err %d != %d", getStoredBlockComplete.err, 0)
	}
	storedBlock2 := getStoredBlockComplete.storedBlock
	getStoredBlockComplete.storedBlock = nullBlock
	if storedBlock2 == nullBlock {
		t.Errorf("TestFSBlockStore() HasStoredBlock() %v != %v", storedBlock2, nullBlock)
	}
	defer storedBlock2.Dispose()
	validateStoredBlock(t, storedBlock2)

	putStoredBlockComplete.wg.Add(1)
	errno = blockStoreAPI.PutStoredBlock(block3, CreateAsyncPutStoredBlockAPI(putStoredBlockComplete))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() PutStoredBlock() %d != %d", errno, 0)
		putStoredBlockComplete.wg.Done()
	}
	putStoredBlockComplete.wg.Wait()
	if putStoredBlockComplete.err != 0 {
		t.Errorf("TestFSBlockStore() putStoredBlockComplete.err %d != %d", putStoredBlockComplete.err, 0)
	}

	storedBlock3Index := block3.GetBlockIndex()
	storedBlock3Hash := storedBlock3Index.GetBlockHash()
	getStoredBlockComplete.wg.Add(1)
	errno = blockStoreAPI.GetStoredBlock(storedBlock3Hash, CreateAsyncGetStoredBlockAPI(getStoredBlockComplete))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() GetStoredBlock() %d != %d", errno, 0)
		getStoredBlockComplete.wg.Done()
	}
	getStoredBlockComplete.wg.Wait()
	if getStoredBlockComplete.err != 0 {
		t.Errorf("TestFSBlockStore() getStoredBlockComplete.err %d != %d", getStoredBlockComplete.err, 0)
	}
	storedBlock3 := getStoredBlockComplete.storedBlock
	getStoredBlockComplete.storedBlock = nullBlock
	if storedBlock3 == nullBlock {
		t.Errorf("TestFSBlockStore() HasStoredBlock() %v != %v", storedBlock3, nullBlock)
	}
	defer storedBlock3.Dispose()
	validateStoredBlock(t, storedBlock3)

	getStoredBlockComplete.wg.Add(1)
	errno = blockStoreAPI.GetStoredBlock(storedBlock2Hash, CreateAsyncGetStoredBlockAPI(getStoredBlockComplete))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() GetStoredBlock() %d != %d", errno, 0)
		getStoredBlockComplete.wg.Done()
	}
	getStoredBlockComplete.wg.Wait()
	if getStoredBlockComplete.err != 0 {
		t.Errorf("TestFSBlockStore() getStoredBlockComplete.err %d != %d", getStoredBlockComplete.err, 0)
	}
	storedBlock2Again := getStoredBlockComplete.storedBlock
	getStoredBlockComplete.storedBlock = nullBlock
	if storedBlock2Again == nullBlock {
		t.Errorf("TestFSBlockStore() HasStoredBlock() %v != %v", storedBlock2Again, nullBlock)
	}
	defer storedBlock2Again.Dispose()
	validateStoredBlock(t, storedBlock2Again)

	getIndexComplete.wg.Add(1)
	errno = blockStoreAPI.GetIndex(blake3.GetIdentifier(), CreateAsyncGetIndexAPI(getIndexComplete))
	if errno != 0 {
		t.Errorf("TestFSBlockStore() GetIndex () %d != %d", errno, 0)
		getIndexComplete.wg.Done()
	}
	getIndexComplete.wg.Wait()
	contentIndex2 := getIndexComplete.contentIndex
	defer contentIndex2.Dispose()
	// FSBlockStore does not use "MergeContentIndex" only "AddContentIndex" so the index will contain redunant blocks due way this test is written
	if contentIndex2.GetBlockCount() != uint64(3) {
		t.Errorf("TestFSBlockStore() contentIndex2.GetBlockCount() %q != %q", contentIndex2.GetBlockCount(), uint64(1))
	}
	if contentIndex2.GetChunkCount() != uint64(15) {
		t.Errorf("TestFSBlockStore() contentIndex2.GetChunkCount() %q != %q", contentIndex2.GetChunkCount(), uint64(9))
	}
}

type TestBlockStore struct {
	blocks            map[uint64]Longtail_StoredBlock
	maxBlockSize      uint32
	maxChunksPerBlock uint32
	blockStoreAPI     Longtail_BlockStoreAPI
	lock              sync.Mutex
	getIndexCount     uint64
	putBlockCount     uint64
	getBlockCount     uint64
}

func (b *TestBlockStore) PutStoredBlock(
	storedBlock Longtail_StoredBlock,
	asyncCompleteAPI Longtail_AsyncPutStoredBlockAPI) int {
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
		b.putBlockCount = b.putBlockCount + 1
		asyncCompleteAPI.OnComplete(0)
		return 0
	}
	asyncCompleteAPI.OnComplete(errno)
	return 0
}

func (b *TestBlockStore) PreflightGet(blockCount uint64, hashes []uint64, refCounts []uint32) int {
	return 0
}

func (b *TestBlockStore) GetStoredBlock(
	blockHash uint64,
	asyncCompleteAPI Longtail_AsyncGetStoredBlockAPI) int {
	b.lock.Lock()
	defer b.lock.Unlock()
	if storedBlock, ok := b.blocks[blockHash]; ok {
		blockIndex := storedBlock.GetBlockIndex()
		blockCopy, errno := CreateStoredBlock(
			blockIndex.GetBlockHash(),
			blockIndex.GetTag(),
			blockIndex.GetChunkHashes(),
			blockIndex.GetChunkSizes(),
			storedBlock.GetChunksBlockData(),
			false)
		b.getBlockCount = b.getBlockCount + 1
		if errno == 0 {
			asyncCompleteAPI.OnComplete(blockCopy, errno)
			return 0
		}
	}
	asyncCompleteAPI.OnComplete(Longtail_StoredBlock{}, ENOENT)
	return 0
}

func (b *TestBlockStore) GetIndex(
	defaultHashAPIIdentifier uint32,
	asyncCompleteAPI Longtail_AsyncGetIndexAPI) int {
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
		b.maxBlockSize,
		b.maxChunksPerBlock,
		blockIndexes)
	if err != nil {
		asyncCompleteAPI.OnComplete(Longtail_ContentIndex{}, ENOMEM)
		return 0
	}
	b.getIndexCount = b.getIndexCount + 1
	asyncCompleteAPI.OnComplete(cIndex, 0)
	return 0
}

func (b *TestBlockStore) Close() {
}

// GetStats ...
func (b *TestBlockStore) GetStats() (BlockStoreStats, int) {
	return BlockStoreStats{IndexGetCount: b.getIndexCount, BlocksGetCount: b.getBlockCount, BlocksPutCount: b.putBlockCount}, 0
}

func TestBlockStoreProxy(t *testing.T) {
	SetLogger(&testLogger{t: t})
	defer SetLogger(nil)
	SetAssert(&testAssert{t: t})
	defer SetAssert(nil)
	SetLogLevel(1)

	blockStore := &TestBlockStore{blocks: make(map[uint64]Longtail_StoredBlock), maxBlockSize: 65536, maxChunksPerBlock: 1024}
	blockStoreProxy := CreateBlockStoreAPI(blockStore)
	blockStore.blockStoreAPI = blockStoreProxy
	defer blockStoreProxy.Dispose()

	storedBlock, errno := createStoredBlock(2)
	if errno != 0 {
		t.Errorf("TestBlockStoreProxy() createStoredBlock() %d != %d", errno, 0)
	}
	defer storedBlock.Dispose()

	putStoredBlockComplete := &testPutBlockCompletionAPI{}
	putStoredBlockComplete.wg.Add(1)
	errno = blockStoreProxy.PutStoredBlock(storedBlock, CreateAsyncPutStoredBlockAPI(putStoredBlockComplete))
	if errno != 0 {
		t.Errorf("TestBlockStoreProxy() PutStoredBlock() %d != %d", errno, 0)
		putStoredBlockComplete.wg.Done()
	}
	putStoredBlockComplete.wg.Wait()
	if putStoredBlockComplete.err != 0 {
		t.Errorf("TestBlockStoreProxy() putStoredBlockComplete.err %d != %d", putStoredBlockComplete.err, 0)
	}

	getStoredBlockComplete := &testGetBlockCompletionAPI{}
	storedBlockIndex := storedBlock.GetBlockIndex()
	getStoredBlockComplete.wg.Add(1)
	errno = blockStoreProxy.GetStoredBlock(storedBlockIndex.GetBlockHash(), CreateAsyncGetStoredBlockAPI(getStoredBlockComplete))
	if errno != 0 {
		t.Errorf("TestBlockStoreProxy() GetStoredBlock() %d!= %d", errno, 0)
		getStoredBlockComplete.wg.Done()
	}
	getStoredBlockComplete.wg.Wait()
	if getStoredBlockComplete.err != 0 {
		t.Errorf("TestFSBlockStore() getStoredBlockComplete.err %d != %d", getStoredBlockComplete.err, 0)
	}
	getBlock := getStoredBlockComplete.storedBlock
	getStoredBlockComplete.storedBlock = Longtail_StoredBlock{}
	defer getBlock.Dispose()
	validateStoredBlock(t, getBlock)

	getIndexComplete := &testGetIndexCompletionAPI{}
	getIndexComplete.wg.Add(1)
	errno = blockStoreProxy.GetIndex(GetBlake3HashIdentifier(), CreateAsyncGetIndexAPI(getIndexComplete))
	if errno != 0 {
		t.Errorf("TestBlockStoreProxy() GetIndex() %d != %d", errno, 0)
		getIndexComplete.wg.Done()
	}
	getIndexComplete.wg.Wait()

	stats, errno := blockStoreProxy.GetStats()
	if errno != 0 {
		t.Errorf("TestBlockStoreProxy() GetStats() %d != %d", errno, 0)
	}
	if stats.IndexGetCount != 1 {
		t.Errorf("TestBlockStoreProxy() stats.BlocksGetCount %d != %d", stats.IndexGetCount, 1)
	}
	if stats.BlocksGetCount != 1 {
		t.Errorf("TestBlockStoreProxy() stats.BlocksGetCount %d != %d", stats.BlocksGetCount, 1)
	}
	if stats.BlocksPutCount != 1 {
		t.Errorf("TestBlockStoreProxy() stats.BlocksPutCount %d != %d", stats.BlocksPutCount, 1)
	}
	contentIndex := getIndexComplete.contentIndex
	defer contentIndex.Dispose()
}

type testPathFilter struct {
}

func (p *testPathFilter) Include(rootPath string, assetFolder string, assetName string, isDir bool, size uint64, permissions uint16) bool {
	return true
}

func TestBlockStoreProxyFull(t *testing.T) {
	storageAPI := createFilledStorage("content")
	defer storageAPI.Dispose()
	hashAPI := CreateBlake3HashAPI()
	defer hashAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()
	testBlockStore := &TestBlockStore{blocks: make(map[uint64]Longtail_StoredBlock), maxBlockSize: 65536, maxChunksPerBlock: 1024}
	blockStoreAPI := CreateBlockStoreAPI(testBlockStore)
	defer blockStoreAPI.Dispose()

	pathFilter := CreatePathFilterAPI(&testPathFilter{})

	fileInfos, err := GetFilesRecursively(storageAPI, pathFilter, "content")
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

	getIndexComplete := &testGetIndexCompletionAPI{}
	getIndexComplete.wg.Add(1)
	errno := blockStoreAPI.GetIndex(hashAPI.GetIdentifier(), CreateAsyncGetIndexAPI(getIndexComplete))
	if errno != 0 {
		t.Errorf("TestBlockStoreProxyFull() blockStoreAPI.GetIndex() %d != %d", errno, 0)
		getIndexComplete.wg.Done()
	}
	getIndexComplete.wg.Wait()
	blockStoreContentIndex := getIndexComplete.contentIndex
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
	fileInfos, err := GetFilesRecursively(storageAPI, Longtail_PathFilterAPI{}, "content")
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
	fileInfos, err := GetFilesRecursively(storageAPI, Longtail_PathFilterAPI{}, "content")
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
	fileInfos, err := GetFilesRecursively(storageAPI, Longtail_PathFilterAPI{}, "content")
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
	compressionRegistry := CreateZStdCompressionRegistry()
	compressionRegistry.Dispose()
	writeContentProgress := CreateProgressAPI(&testProgress{task: "WriteContent", t: t})
	defer writeContentProgress.Dispose()
	getIndexComplete := &testGetIndexCompletionAPI{}
	getIndexComplete.wg.Add(1)
	errno := blockStorageAPI.GetIndex(hashAPI.GetIdentifier(), CreateAsyncGetIndexAPI(getIndexComplete))
	if errno != 0 {
		t.Errorf("TestBlockStoreProxyFull() blockStoreAPI.GetIndex() %d != %d", errno, 0)
		getIndexComplete.wg.Done()
	}
	getIndexComplete.wg.Wait()
	blockStoreContentIndex := getIndexComplete.contentIndex
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
