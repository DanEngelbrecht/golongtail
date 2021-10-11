package longtaillib

import (
	"crypto/rand"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
)

type testProgress struct {
	inited bool
	task   string
	t      *testing.T
}

func (p *testProgress) OnProgress(total uint32, current uint32) {
	if current == total {
		if p.inited {
			p.t.Logf("100%%")
			p.t.Logf(" Done\n")
		}
		return
	}
	if !p.inited {
		p.t.Logf("%s: ", p.task)
		p.inited = true
	}
	percentDone := (100 * current) / total
	p.t.Logf("%d%% ", percentDone)
}

func CreateProgress(t *testing.T, task string) Longtail_ProgressAPI {
	baseProgress := CreateProgressAPI(&testProgress{task: task, t: t})
	return CreateRateLimitedProgressAPI(baseProgress, 5)
}

type testPutBlockCompletionAPI struct {
	wg  sync.WaitGroup
	err error
}

func (a *testPutBlockCompletionAPI) OnComplete(err error) {
	a.err = err
	a.wg.Done()
}

type testGetBlockCompletionAPI struct {
	wg          sync.WaitGroup
	storedBlock Longtail_StoredBlock
	err         error
}

func (a *testGetBlockCompletionAPI) OnComplete(storedBlock Longtail_StoredBlock, err error) {
	a.storedBlock = storedBlock
	a.err = err
	a.wg.Done()
}

type testGetExistingContentCompletionAPI struct {
	wg         sync.WaitGroup
	storeIndex Longtail_StoreIndex
	err        error
}

func (a *testGetExistingContentCompletionAPI) OnComplete(storeIndex Longtail_StoreIndex, err error) {
	a.storeIndex = storeIndex
	a.err = err
	a.wg.Done()
}

type testPruneBlocksCompletionAPI struct {
	wg               sync.WaitGroup
	prunedBlockCount uint32
	err              error
}

func (a *testPruneBlocksCompletionAPI) OnComplete(prunedBlockCount uint32, err error) {
	a.prunedBlockCount = prunedBlockCount
	a.err = err
	a.wg.Done()
}

type testLogger struct {
	t *testing.T
}

func (l *testLogger) OnLog(file string, function string, line int, level int, logFields []LogField, message string) {
	l.t.Logf("%d: %s", level, message)
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
	if err != nil {
		t.Errorf("WriteToStorage() %w", err)
	}

	rbytes, err := storageAPI.ReadFromStorage("folder", "file")
	if err != nil {
		t.Errorf("ReadFromStorage() %w", err)
	}
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

	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobAPI.Dispose()

	compressionRegistry := CreateZStdCompressionRegistry()
	defer compressionRegistry.Dispose()
}

func createStoredBlock(chunkCount uint32, hashIdentifier uint32, hashOffset uint64) (Longtail_StoredBlock, error) {
	blockHash := uint64(0xdeadbeef500177aa) + uint64(chunkCount)
	chunkHashes := make([]uint64, chunkCount)
	chunkSizes := make([]uint32, chunkCount)
	blockOffset := uint32(0)
	for index := range chunkHashes {
		chunkHashes[index] = uint64(index+1)*4711 + hashOffset
		chunkSizes[index] = uint32(index+1) * 10
		blockOffset += uint32(chunkSizes[index])
	}
	blockData := make([]uint8, blockOffset)
	blockOffset = 0
	for chunkIndex := range chunkHashes {
		for index := uint32(0); index < uint32(chunkSizes[chunkIndex]); index++ {
			blockData[blockOffset+index] = uint8(chunkIndex + 1)
		}
		blockOffset += uint32(chunkSizes[chunkIndex])
	}

	return CreateStoredBlock(
		blockHash,
		hashIdentifier,
		chunkCount+uint32(10000),
		chunkHashes,
		chunkSizes,
		blockData,
		false)
}

func validateStoredBlock(t *testing.T, storedBlock Longtail_StoredBlock, hashIdentifier uint32) {
	if storedBlock.cStoredBlock == nil {
		t.Errorf("validateStoredBlock() %p == %p", storedBlock, Longtail_StoredBlock{cStoredBlock: nil})
	}
	blockIndex := storedBlock.GetBlockIndex()
	if blockIndex.GetHashIdentifier() != hashIdentifier {
		t.Errorf("validateStoredBlock() %d == %d", blockIndex.GetHashIdentifier(), hashIdentifier)
	}
	chunkCount := blockIndex.GetChunkCount()
	if blockIndex.GetBlockHash() != uint64(0xdeadbeef500177aa)+uint64(chunkCount) {
		t.Errorf("validateStoredBlock() %d != %d", uint64(0xdeadbeef500177aa)+uint64(chunkCount), blockIndex.GetBlockHash())
	}
	if blockIndex.GetTag() != chunkCount+uint32(10000) {
		t.Errorf("validateStoredBlock() %d != %d", chunkCount+uint32(10000), blockIndex.GetTag())

	}
	chunkHashes := blockIndex.GetChunkHashes()
	if uint32(len(chunkHashes)) != chunkCount {
		t.Errorf("validateStoredBlock() %d != %d", chunkCount, uint32(len(chunkHashes)))
	}
	chunkSizes := blockIndex.GetChunkSizes()
	if uint32(len(chunkSizes)) != chunkCount {
		t.Errorf("validateStoredBlock() %d != %d", chunkCount, uint32(len(chunkSizes)))
	}
	blockOffset := uint32(0)
	for index, _ := range chunkHashes {
		if chunkHashes[index] != uint64(index+1)*4711 {
			t.Errorf("validateStoredBlock() %d != %d", uint64(index)*4711, chunkHashes[index])
		}
		if chunkSizes[index] != uint32(index+1)*10 {
			t.Errorf("validateStoredBlock() %d != %d", uint32(index)*10, chunkSizes[index])
		}
		blockOffset += uint32(chunkSizes[index])
	}
	blockData := storedBlock.GetChunksBlockData()
	if uint32(len(blockData)) != blockOffset {
		t.Errorf("validateStoredBlock() %d != %d", uint32(len(blockData)), blockOffset)
	}
	blockOffset = 0
	for chunkIndex, _ := range chunkHashes {
		for index := uint32(0); index < uint32(chunkSizes[chunkIndex]); index++ {
			if blockData[blockOffset+index] != uint8(chunkIndex+1) {
				t.Errorf("validateStoredBlock() %d != %d", uint8(chunkIndex+1), blockData[blockOffset+index])
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

	storedBlock, err := createStoredBlock(2, 0xdeadbeef, 0)
	if err != nil {
		t.Errorf("CreateStoredBlock() %w", err)
	}
	validateStoredBlock(t, storedBlock, 0xdeadbeef)
}

func Test_ReadWriteStoredBlockBuffer(t *testing.T) {
	SetLogger(&testLogger{t: t})
	defer SetLogger(nil)
	SetAssert(&testAssert{t: t})
	defer SetAssert(nil)
	SetLogLevel(1)

	originalBlock, err := createStoredBlock(2, 0xdeadbeef, 0)
	if err != nil {
		t.Errorf("createStoredBlock() %w", err)
	}

	storedBlockData, err := WriteStoredBlockToBuffer(originalBlock)
	if err != nil {
		t.Errorf("WriteStoredBlockToBuffer() %w", err)
	}
	originalBlock.Dispose()

	copyBlock, err := ReadStoredBlockFromBuffer(storedBlockData)

	if err != nil {
		t.Errorf("InitStoredBlockFromData() %w", err)
	}
	defer copyBlock.Dispose()
	validateStoredBlock(t, copyBlock, 0xdeadbeef)
}

func TestFSBlockStore(t *testing.T) {
	SetLogger(&testLogger{t: t})
	defer SetLogger(nil)
	SetAssert(&testAssert{t: t})
	defer SetAssert(nil)
	SetLogLevel(1)

	storageAPI := CreateInMemStorageAPI()
	defer storageAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobAPI.Dispose()
	blockStoreAPI := CreateFSBlockStore(jobAPI, storageAPI, "content")
	defer blockStoreAPI.Dispose()
	blake3 := CreateBlake3HashAPI()
	defer blake3.Dispose()

	block1, err := createStoredBlock(1, blake3.GetIdentifier(), 0)
	if err != nil {
		t.Errorf("TestFSBlockStore() createStoredBlock() %w", err)
	}
	defer block1.Dispose()

	block2, err := createStoredBlock(5, blake3.GetIdentifier(), 0)
	if err != nil {
		t.Errorf("TestFSBlockStore() createStoredBlock() %w", err)
	}
	defer block2.Dispose()

	block3, err := createStoredBlock(9, blake3.GetIdentifier(), 0)
	if err != nil {
		t.Errorf("TestFSBlockStore() createStoredBlock() %w", err)
	}
	defer block3.Dispose()

	storedBlock1Index := block1.GetBlockIndex()
	storedBlock1Hash := storedBlock1Index.GetBlockHash()
	getStoredBlockComplete := &testGetBlockCompletionAPI{}
	getStoredBlockComplete.wg.Add(1)
	err = blockStoreAPI.GetStoredBlock(storedBlock1Hash, CreateAsyncGetStoredBlockAPI(getStoredBlockComplete))
	if !IsNotExist(err) {
		t.Errorf("TestFSBlockStore() GetStoredBlock() %w", err)
		getStoredBlockComplete.wg.Done()
	}
	getStoredBlockComplete.wg.Done()
	nullBlock := Longtail_StoredBlock{}
	if getStoredBlockComplete.storedBlock != nullBlock {
		t.Errorf("TestFSBlockStore() getStoredBlockComplete.storedBlock %p != %p", getStoredBlockComplete.storedBlock, nullBlock)
	}

	putStoredBlockComplete := &testPutBlockCompletionAPI{}
	putStoredBlockComplete.wg.Add(1)
	err = blockStoreAPI.PutStoredBlock(block1, CreateAsyncPutStoredBlockAPI(putStoredBlockComplete))
	if err != nil {
		t.Errorf("TestFSBlockStore() PutStoredBlock() %w", err)
		putStoredBlockComplete.wg.Done()
	}
	putStoredBlockComplete.wg.Wait()
	if putStoredBlockComplete.err != nil {
		t.Errorf("TestFSBlockStore() putStoredBlockComplete.err %w", putStoredBlockComplete.err)
	}

	getStoredBlockComplete.wg.Add(1)
	err = blockStoreAPI.GetStoredBlock(storedBlock1Hash, CreateAsyncGetStoredBlockAPI(getStoredBlockComplete))
	if err != nil {
		t.Errorf("TestFSBlockStore() GetStoredBlock() %w", err)
		getStoredBlockComplete.wg.Done()
	}
	getStoredBlockComplete.wg.Wait()
	if getStoredBlockComplete.err != nil {
		t.Errorf("TestFSBlockStore() getStoredBlockComplete.err %w", getStoredBlockComplete.err)
	}
	storedBlock1 := getStoredBlockComplete.storedBlock
	getStoredBlockComplete.storedBlock = nullBlock
	if storedBlock1 == nullBlock {
		t.Errorf("TestFSBlockStore() HasStoredBlock() %v != %v", storedBlock1, nullBlock)
	}
	defer storedBlock1.Dispose()
	validateStoredBlock(t, storedBlock1, blake3.GetIdentifier())

	putStoredBlockComplete.wg.Add(1)
	err = blockStoreAPI.PutStoredBlock(block2, CreateAsyncPutStoredBlockAPI(putStoredBlockComplete))
	if err != nil {
		t.Errorf("TestFSBlockStore() PutStoredBlock() %w", err)
		putStoredBlockComplete.wg.Done()
	}
	putStoredBlockComplete.wg.Wait()
	if putStoredBlockComplete.err != nil {
		t.Errorf("TestFSBlockStore() putStoredBlockComplete.err %w", putStoredBlockComplete.err)
	}

	storedBlock2Index := block2.GetBlockIndex()
	storedBlock2Hash := storedBlock2Index.GetBlockHash()
	getStoredBlockComplete.wg.Add(1)
	err = blockStoreAPI.GetStoredBlock(storedBlock2Hash, CreateAsyncGetStoredBlockAPI(getStoredBlockComplete))
	if err != nil {
		t.Errorf("TestFSBlockStore() HasStoredBlock() %w", err)
		getStoredBlockComplete.wg.Done()
	}
	getStoredBlockComplete.wg.Wait()
	if getStoredBlockComplete.err != nil {
		t.Errorf("TestFSBlockStore() getStoredBlockComplete.err %w", getStoredBlockComplete.err)
	}
	storedBlock2 := getStoredBlockComplete.storedBlock
	getStoredBlockComplete.storedBlock = nullBlock
	if storedBlock2 == nullBlock {
		t.Errorf("TestFSBlockStore() HasStoredBlock() %v != %v", storedBlock2, nullBlock)
	}
	defer storedBlock2.Dispose()
	validateStoredBlock(t, storedBlock2, blake3.GetIdentifier())

	putStoredBlockComplete.wg.Add(1)
	err = blockStoreAPI.PutStoredBlock(block3, CreateAsyncPutStoredBlockAPI(putStoredBlockComplete))
	if err != nil {
		t.Errorf("TestFSBlockStore() PutStoredBlock() %w", err)
		putStoredBlockComplete.wg.Done()
	}
	putStoredBlockComplete.wg.Wait()
	if putStoredBlockComplete.err != nil {
		t.Errorf("TestFSBlockStore() putStoredBlockComplete.err %w", putStoredBlockComplete.err)
	}

	storedBlock3Index := block3.GetBlockIndex()
	storedBlock3Hash := storedBlock3Index.GetBlockHash()
	getStoredBlockComplete.wg.Add(1)
	err = blockStoreAPI.GetStoredBlock(storedBlock3Hash, CreateAsyncGetStoredBlockAPI(getStoredBlockComplete))
	if err != nil {
		t.Errorf("TestFSBlockStore() GetStoredBlock() %w", err)
		getStoredBlockComplete.wg.Done()
	}
	getStoredBlockComplete.wg.Wait()
	if getStoredBlockComplete.err != nil {
		t.Errorf("TestFSBlockStore() getStoredBlockComplete.err %w", getStoredBlockComplete.err)
	}
	storedBlock3 := getStoredBlockComplete.storedBlock
	getStoredBlockComplete.storedBlock = nullBlock
	if storedBlock3 == nullBlock {
		t.Errorf("TestFSBlockStore() HasStoredBlock() %v != %v", storedBlock3, nullBlock)
	}
	defer storedBlock3.Dispose()
	validateStoredBlock(t, storedBlock3, blake3.GetIdentifier())

	getStoredBlockComplete.wg.Add(1)
	err = blockStoreAPI.GetStoredBlock(storedBlock2Hash, CreateAsyncGetStoredBlockAPI(getStoredBlockComplete))
	if err != nil {
		t.Errorf("TestFSBlockStore() GetStoredBlock() %w", err)
		getStoredBlockComplete.wg.Done()
	}
	getStoredBlockComplete.wg.Wait()
	if getStoredBlockComplete.err != nil {
		t.Errorf("TestFSBlockStore() getStoredBlockComplete.err %w", getStoredBlockComplete.err)
	}
	storedBlock2Again := getStoredBlockComplete.storedBlock
	getStoredBlockComplete.storedBlock = nullBlock
	if storedBlock2Again == nullBlock {
		t.Errorf("TestFSBlockStore() HasStoredBlock() %v != %v", storedBlock2Again, nullBlock)
	}
	defer storedBlock2Again.Dispose()
	validateStoredBlock(t, storedBlock2Again, blake3.GetIdentifier())
}

type TestBlockStore struct {
	blocks        map[uint64]Longtail_StoredBlock
	blockStoreAPI Longtail_BlockStoreAPI
	lock          sync.Mutex
	stats         [Longtail_BlockStoreAPI_StatU64_Count]uint64
	didClose      bool
}

func (b *TestBlockStore) PutStoredBlock(
	storedBlock Longtail_StoredBlock,
	asyncCompleteAPI Longtail_AsyncPutStoredBlockAPI) error {
	b.lock.Lock()
	b.stats[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count] += 1
	defer b.lock.Unlock()
	blockIndex := storedBlock.GetBlockIndex()
	blockHash := blockIndex.GetBlockHash()
	if _, ok := b.blocks[blockHash]; ok {
		return nil
	}
	blockCopy, err := CreateStoredBlock(
		blockHash,
		blockIndex.GetHashIdentifier(),
		blockIndex.GetTag(),
		blockIndex.GetChunkHashes(),
		blockIndex.GetChunkSizes(),
		storedBlock.GetChunksBlockData(),
		false)
	if err == nil {
		b.blocks[blockHash] = blockCopy
		asyncCompleteAPI.OnComplete(nil)
		return nil
	}
	asyncCompleteAPI.OnComplete(err)
	return nil
}

func (b *TestBlockStore) PreflightGet(blockHashes []uint64, asyncCompleteAPI Longtail_AsyncPreflightStartedAPI) error {
	b.stats[Longtail_BlockStoreAPI_StatU64_PreflightGet_Count] += 1
	asyncCompleteAPI.OnComplete(blockHashes, nil)
	return nil
}

func (b *TestBlockStore) GetStoredBlock(
	blockHash uint64,
	asyncCompleteAPI Longtail_AsyncGetStoredBlockAPI) error {
	b.lock.Lock()
	b.stats[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count] += 1
	defer b.lock.Unlock()
	if storedBlock, ok := b.blocks[blockHash]; ok {
		blockIndex := storedBlock.GetBlockIndex()
		blockCopy, err := CreateStoredBlock(
			blockIndex.GetBlockHash(),
			blockIndex.GetHashIdentifier(),
			blockIndex.GetTag(),
			blockIndex.GetChunkHashes(),
			blockIndex.GetChunkSizes(),
			storedBlock.GetChunksBlockData(),
			false)
		if err == nil {
			asyncCompleteAPI.OnComplete(blockCopy, nil)
			return nil
		}
	}
	asyncCompleteAPI.OnComplete(Longtail_StoredBlock{}, NotExist())
	return nil
}

func (b *TestBlockStore) GetIndexSync() (Longtail_StoreIndex, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	blockCount := len(b.blocks)
	blockIndexes := make([]Longtail_BlockIndex, blockCount)
	arrayIndex := 0
	for _, value := range b.blocks {
		blockIndexes[arrayIndex] = value.GetBlockIndex()
		arrayIndex++
	}
	sIndex, err := CreateStoreIndexFromBlocks(blockIndexes)
	return sIndex, err
}

func (b *TestBlockStore) GetExistingContent(
	chunkHashes []uint64,
	minBlockUsagePercent uint32,
	asyncCompleteAPI Longtail_AsyncGetExistingContentAPI) error {
	b.stats[Longtail_BlockStoreAPI_StatU64_GetExistingContent_Count] += 1
	sIndex, err := b.GetIndexSync()
	if err != nil {
		asyncCompleteAPI.OnComplete(Longtail_StoreIndex{}, err)
		return nil
	}
	defer sIndex.Dispose()

	sExistingIndex, err := GetExistingStoreIndex(
		sIndex,
		chunkHashes,
		minBlockUsagePercent)
	if err != nil {
		asyncCompleteAPI.OnComplete(Longtail_StoreIndex{}, err)
		return nil
	}
	asyncCompleteAPI.OnComplete(sExistingIndex, nil)
	return nil
}

func (b *TestBlockStore) PruneBlocks(
	keepBlockHashes []uint64,
	asyncCompleteAPI Longtail_AsyncPruneBlocksAPI) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.stats[Longtail_BlockStoreAPI_StatU64_PruneBlocks_Count] += 1
	keepMap := make(map[uint64]bool)
	for _, b := range keepBlockHashes {
		keepMap[b] = true
	}
	var removeBlocks []uint64
	for h, _ := range b.blocks {
		if _, exists := keepMap[h]; exists {
			continue
		}
		removeBlocks = append(removeBlocks, h)
	}
	removeCount := uint32(0)
	for _, h := range removeBlocks {
		if _, exists := keepMap[h]; exists {
			continue
		}
		storedBlock := b.blocks[h]
		delete(b.blocks, h)
		storedBlock.Dispose()
		removeCount++
	}
	asyncCompleteAPI.OnComplete(removeCount, nil)
	return nil
}

// GetStats ...
func (b *TestBlockStore) GetStats() (BlockStoreStats, error) {
	b.stats[Longtail_BlockStoreAPI_StatU64_GetStats_Count] += 1
	var stats BlockStoreStats
	for i := 0; i < Longtail_BlockStoreAPI_StatU64_Count; i++ {
		stats.StatU64[i] = b.stats[i]
	}
	return stats, nil
}

func (b *TestBlockStore) Flush(asyncCompleteAPI Longtail_AsyncFlushAPI) error {
	b.stats[Longtail_BlockStoreAPI_StatU64_Flush_Count] += 1
	asyncCompleteAPI.OnComplete(nil)
	return nil
}

func (b *TestBlockStore) Close() {
	b.didClose = true
}

func TestPutGetStoredBlock(t *testing.T) {
	SetLogger(&testLogger{t: t})
	defer SetLogger(nil)
	SetAssert(&testAssert{t: t})
	defer SetAssert(nil)
	SetLogLevel(1)

	blockStore := &TestBlockStore{blocks: make(map[uint64]Longtail_StoredBlock), didClose: false}
	blockStoreProxy := CreateBlockStoreAPI(blockStore)
	blockStore.blockStoreAPI = blockStoreProxy
	defer blockStoreProxy.Dispose()

	storedBlock, err := createStoredBlock(2, 0xdeadbeef, 0)
	if err != nil {
		t.Errorf("TestBlockStoreProxy() createStoredBlock() %w", err)
	}
	defer storedBlock.Dispose()

	putStoredBlockComplete := &testPutBlockCompletionAPI{}
	putStoredBlockComplete.wg.Add(1)
	err = blockStoreProxy.PutStoredBlock(storedBlock, CreateAsyncPutStoredBlockAPI(putStoredBlockComplete))
	if err != nil {
		t.Errorf("TestBlockStoreProxy() PutStoredBlock() %w", err)
		putStoredBlockComplete.wg.Done()
	}
	putStoredBlockComplete.wg.Wait()
	if putStoredBlockComplete.err != nil {
		t.Errorf("TestBlockStoreProxy() putStoredBlockComplete.err %w", putStoredBlockComplete.err)
	}

	getStoredBlockComplete := &testGetBlockCompletionAPI{}
	storedBlockIndex := storedBlock.GetBlockIndex()
	getStoredBlockComplete.wg.Add(1)
	err = blockStoreProxy.GetStoredBlock(storedBlockIndex.GetBlockHash(), CreateAsyncGetStoredBlockAPI(getStoredBlockComplete))
	if err != nil {
		t.Errorf("TestBlockStoreProxy() GetStoredBlock() %w", err)
		getStoredBlockComplete.wg.Done()
	}
	getStoredBlockComplete.wg.Wait()
	if getStoredBlockComplete.err != nil {
		t.Errorf("TestFSBlockStore() getStoredBlockComplete.err %w", getStoredBlockComplete.err)
	}
	getBlock := getStoredBlockComplete.storedBlock
	getStoredBlockComplete.storedBlock = Longtail_StoredBlock{}
	defer getBlock.Dispose()
	validateStoredBlock(t, getBlock, 0xdeadbeef)

	stats, err := blockStoreProxy.GetStats()
	if err != nil {
		t.Errorf("TestBlockStoreProxy() GetStats() %wd", err)
	}
	if stats.StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count] != 1 {
		t.Errorf("TestBlockStoreProxy() stats.BlocksGetCount %d != %d", stats.StatU64[Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count], 1)
	}
	if stats.StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count] != 1 {
		t.Errorf("TestBlockStoreProxy() stats.BlocksPutCount %d != %d", stats.StatU64[Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count], 1)
	}

	blockStoreProxy.Dispose()
}

func TestPruneStoredBlocks(t *testing.T) {
	SetLogger(&testLogger{t: t})
	defer SetLogger(nil)
	SetAssert(&testAssert{t: t})
	defer SetAssert(nil)
	SetLogLevel(1)

	blockStore := &TestBlockStore{blocks: make(map[uint64]Longtail_StoredBlock), didClose: false}
	blockStoreProxy := CreateBlockStoreAPI(blockStore)
	blockStore.blockStoreAPI = blockStoreProxy
	defer blockStoreProxy.Dispose()

	var allChunkHashes []uint64

	allBlockHashes := [4]uint64{0, 0, 0, 0}
	{
		storedBlock, err := createStoredBlock(2, 0xdeadbeef, 0)
		if err != nil {
			t.Errorf("TestBlockStoreProxy() createStoredBlock() %w", err)
		}
		defer storedBlock.Dispose()
		blockIndex := storedBlock.GetBlockIndex()
		chunkHashes := blockIndex.GetChunkHashes()
		allChunkHashes = append(allChunkHashes, chunkHashes...)
		allBlockHashes[0] = storedBlock.GetBlockHash()

		putStoredBlockComplete := &testPutBlockCompletionAPI{}
		putStoredBlockComplete.wg.Add(1)
		err = blockStoreProxy.PutStoredBlock(storedBlock, CreateAsyncPutStoredBlockAPI(putStoredBlockComplete))
		if err != nil {
			t.Errorf("TestBlockStoreProxy() PutStoredBlock() %w", err)
			putStoredBlockComplete.wg.Done()
		}
		putStoredBlockComplete.wg.Wait()
		if putStoredBlockComplete.err != nil {
			t.Errorf("TestBlockStoreProxy() putStoredBlockComplete.err %w", putStoredBlockComplete.err)
		}
	}

	{
		storedBlock, err := createStoredBlock(3, 0xdeadbeef, 10000)
		if err != nil {
			t.Errorf("TestBlockStoreProxy() createStoredBlock() %w", err)
		}
		defer storedBlock.Dispose()
		blockIndex := storedBlock.GetBlockIndex()
		chunkHashes := blockIndex.GetChunkHashes()
		allChunkHashes = append(allChunkHashes, chunkHashes...)
		allBlockHashes[1] = storedBlock.GetBlockHash()

		putStoredBlockComplete := &testPutBlockCompletionAPI{}
		putStoredBlockComplete.wg.Add(1)
		err = blockStoreProxy.PutStoredBlock(storedBlock, CreateAsyncPutStoredBlockAPI(putStoredBlockComplete))
		if err != nil {
			t.Errorf("TestBlockStoreProxy() PutStoredBlock() %w", err)
			putStoredBlockComplete.wg.Done()
		}
		putStoredBlockComplete.wg.Wait()
		if putStoredBlockComplete.err != nil {
			t.Errorf("TestBlockStoreProxy() putStoredBlockComplete.err %w", putStoredBlockComplete.err)
		}
	}
	{
		storedBlock, err := createStoredBlock(1, 0xdeadbeef, 20000)
		if err != nil {
			t.Errorf("TestBlockStoreProxy() createStoredBlock() %w", err)
		}
		defer storedBlock.Dispose()
		blockIndex := storedBlock.GetBlockIndex()
		chunkHashes := blockIndex.GetChunkHashes()
		allChunkHashes = append(allChunkHashes, chunkHashes...)
		allBlockHashes[2] = storedBlock.GetBlockHash()

		putStoredBlockComplete := &testPutBlockCompletionAPI{}
		putStoredBlockComplete.wg.Add(1)
		err = blockStoreProxy.PutStoredBlock(storedBlock, CreateAsyncPutStoredBlockAPI(putStoredBlockComplete))
		if err != nil {
			t.Errorf("TestBlockStoreProxy() PutStoredBlock() %w", err)
			putStoredBlockComplete.wg.Done()
		}
		putStoredBlockComplete.wg.Wait()
		if putStoredBlockComplete.err != nil {
			t.Errorf("TestBlockStoreProxy() putStoredBlockComplete.err %w", putStoredBlockComplete.err)
		}
	}
	{
		storedBlock, err := createStoredBlock(4, 0xdeadbeef, 30000)
		if err != nil {
			t.Errorf("TestBlockStoreProxy() createStoredBlock() %w", err)
		}
		defer storedBlock.Dispose()
		blockIndex := storedBlock.GetBlockIndex()
		chunkHashes := blockIndex.GetChunkHashes()
		allChunkHashes = append(allChunkHashes, chunkHashes...)
		allBlockHashes[3] = storedBlock.GetBlockHash()

		putStoredBlockComplete := &testPutBlockCompletionAPI{}
		putStoredBlockComplete.wg.Add(1)
		err = blockStoreProxy.PutStoredBlock(storedBlock, CreateAsyncPutStoredBlockAPI(putStoredBlockComplete))
		if err != nil {
			t.Errorf("TestBlockStoreProxy() PutStoredBlock() %w", err)
			putStoredBlockComplete.wg.Done()
		}
		putStoredBlockComplete.wg.Wait()
		if putStoredBlockComplete.err != nil {
			t.Errorf("TestBlockStoreProxy() putStoredBlockComplete.err %w", putStoredBlockComplete.err)
		}
	}

	{
		getExistingContentComplete := &testGetExistingContentCompletionAPI{}
		getExistingContentComplete.wg.Add(1)
		err := blockStoreProxy.GetExistingContent(allChunkHashes, 0, CreateAsyncGetExistingContentAPI(getExistingContentComplete))
		if err != nil {
			t.Errorf("TestBlockStoreProxyFull() blockStoreProxy.GetExistingContent() %w", err)
			getExistingContentComplete.wg.Done()
		}
		getExistingContentComplete.wg.Wait()
		fullStoreIndex := getExistingContentComplete.storeIndex
		defer fullStoreIndex.Dispose()
	}

	keepBlockHashes := []uint64{allBlockHashes[1], allBlockHashes[3]}
	pruneBlocksComplete := &testPruneBlocksCompletionAPI{}
	pruneBlocksComplete.wg.Add(1)
	err := blockStoreProxy.PruneBlocks(keepBlockHashes, CreateAsyncPruneBlocksAPI(pruneBlocksComplete))
	if err != nil {
		t.Errorf("TestBlockStoreProxyFull() blockStoreProxy.PruneBlocks() %w", err)
		pruneBlocksComplete.wg.Done()
	}
	pruneBlocksComplete.wg.Wait()
	if pruneBlocksComplete.err != nil {
		t.Errorf("TestBlockStoreProxyFull() pruneBlocksComplete.err %w", err)
	}
	if pruneBlocksComplete.prunedBlockCount != 2 {
		t.Errorf("TestBlockStoreProxyFull() pruneBlocksComplete.prunedBlockCount %d != %d", 2, pruneBlocksComplete.prunedBlockCount)
	}

	{
		getExistingContentComplete := &testGetExistingContentCompletionAPI{}
		getExistingContentComplete.wg.Add(1)
		err = blockStoreProxy.GetExistingContent(allChunkHashes, 0, CreateAsyncGetExistingContentAPI(getExistingContentComplete))
		if err != nil {
			t.Errorf("TestBlockStoreProxyFull() blockStoreProxy.GetExistingContent() %w", err)
			getExistingContentComplete.wg.Done()
		}
		getExistingContentComplete.wg.Wait()
		prunedStoreIndex := getExistingContentComplete.storeIndex
		defer prunedStoreIndex.Dispose()
		blockHashes := prunedStoreIndex.GetBlockHashes()
		if len(blockHashes) != 2 {
			t.Errorf("TestBlockStoreProxyFull() len(blockHashes) %d != %d", 2, len(blockHashes))
		}
		expectedBlockHashes := []uint64{allBlockHashes[1], allBlockHashes[3]}
		sort.Slice(expectedBlockHashes, func(i, j int) bool { return expectedBlockHashes[i] < expectedBlockHashes[j] })
		sort.Slice(blockHashes, func(i, j int) bool { return blockHashes[i] < blockHashes[j] })
		if expectedBlockHashes[0] != blockHashes[0] {
			t.Errorf("TestBlockStoreProxyFull() blockHashes[0] %d != %d", expectedBlockHashes[0], blockHashes[0])
		}
		if expectedBlockHashes[1] != blockHashes[1] {
			t.Errorf("TestBlockStoreProxyFull() blockHashes[1] %d != %d", expectedBlockHashes[1], blockHashes[1])
		}
	}

	blockStoreProxy.Dispose()
}

type testPathFilter struct {
}

func (p *testPathFilter) Include(rootPath string, assetFolder string, assetName string, isDir bool, size uint64, permissions uint16) bool {
	return true
}

func TestWriteContent(t *testing.T) {
	storageAPI := createFilledStorage("content")
	defer storageAPI.Dispose()
	hashAPI := CreateBlake3HashAPI()
	defer hashAPI.Dispose()
	chunkerAPI := CreateHPCDCChunkerAPI()
	defer chunkerAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobAPI.Dispose()
	testBlockStore := &TestBlockStore{blocks: make(map[uint64]Longtail_StoredBlock)}
	blockStoreAPI := CreateBlockStoreAPI(testBlockStore)
	defer blockStoreAPI.Dispose()

	pathFilter := CreatePathFilterAPI(&testPathFilter{})

	fileInfos, err := GetFilesRecursively(storageAPI, pathFilter, "content")
	if err != nil {
		t.Errorf("TestBlockStoreProxyFull() GetFilesRecursively() %w", err)
	}
	defer fileInfos.Dispose()
	tags := make([]uint32, fileInfos.GetFileCount())
	versionIndex, err := CreateVersionIndex(
		storageAPI,
		hashAPI,
		chunkerAPI,
		jobAPI,
		nil,
		"content",
		fileInfos,
		tags,
		32768)
	if err != nil {
		t.Errorf("TestBlockStoreProxyFull() CreateVersionIndex() %w", err)
	}
	defer versionIndex.Dispose()

	getExistingContentComplete := &testGetExistingContentCompletionAPI{}
	getExistingContentComplete.wg.Add(1)
	err = blockStoreAPI.GetExistingContent(versionIndex.GetChunkHashes(), 0, CreateAsyncGetExistingContentAPI(getExistingContentComplete))
	if err != nil {
		t.Errorf("TestBlockStoreProxyFull() blockStoreAPI.GetExistingContent() %w", err)
		getExistingContentComplete.wg.Done()
	}
	getExistingContentComplete.wg.Wait()
	blockStoreIndex := getExistingContentComplete.storeIndex
	defer blockStoreIndex.Dispose()

	missingStoreIndex, err := CreateMissingContent(
		hashAPI,
		blockStoreIndex,
		versionIndex,
		32768*2,
		8)
	if err != nil {
		t.Errorf("TestBlockStoreProxyFull() CreateMissingContent() %w", err)
		getExistingContentComplete.wg.Done()
	}
	defer missingStoreIndex.Dispose()

	err = WriteContent(
		storageAPI,
		blockStoreAPI,
		jobAPI,
		nil,
		missingStoreIndex,
		versionIndex,
		"content")
	if err != nil {
		t.Errorf("TestBlockStoreProxyFull() WriteContent() %w", err)
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
		t.Errorf("TestGetFileRecursively() GetFilesRecursively() %w", err)
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
	path := fileInfos.GetPath(0)
	if path != "first_folder/" {
		t.Errorf("TestGetFileRecursively() GetPaths().GetPath() %s != %s", path, "first_folder/")
	}
}

func TestCreateVersionIndex(t *testing.T) {
	storageAPI := createFilledStorage("content")
	fileInfos, err := GetFilesRecursively(storageAPI, Longtail_PathFilterAPI{}, "content")
	if err != nil {
		t.Errorf("TestCreateVersionIndex() GetFilesRecursively() %w", err)
	}
	hashAPI := CreateBlake2HashAPI()
	defer hashAPI.Dispose()
	chunkerAPI := CreateHPCDCChunkerAPI()
	defer chunkerAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobAPI.Dispose()

	compressionTypes := make([]uint32, fileInfos.GetFileCount())

	versionIndex, err := CreateVersionIndex(
		storageAPI,
		hashAPI,
		chunkerAPI,
		jobAPI,
		nil,
		"content",
		fileInfos,
		compressionTypes,
		32768)

	if err != nil {
		t.Errorf("TestCreateVersionIndex() CreateVersionIndex() %w", err)
	}
	defer versionIndex.Dispose()
	if versionIndex.GetHashIdentifier() != hashAPI.GetIdentifier() {
		t.Errorf("TestCreateVersionIndex() GetHashIdentifier() %d != %d", versionIndex.GetHashIdentifier(), hashAPI.GetIdentifier())
	}
	if versionIndex.GetAssetCount() != fileInfos.GetFileCount() {
		t.Errorf("TestCreateVersionIndex() GetAssetCount() %d != %d", versionIndex.GetAssetCount(), fileInfos.GetFileCount())
	}
}

func TestRewriteVersion(t *testing.T) {
	storageAPI := createFilledStorage("content")
	fileInfos, err := GetFilesRecursively(storageAPI, Longtail_PathFilterAPI{}, "content")
	if err != nil {
		t.Errorf("TestRewriteVersion() GetFilesRecursively() %w", err)
	}
	hashAPI := CreateBlake2HashAPI()
	defer hashAPI.Dispose()
	chunkerAPI := CreateHPCDCChunkerAPI()
	defer chunkerAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobAPI.Dispose()

	compressionTypes := make([]uint32, fileInfos.GetFileCount())

	createVersionProgress := CreateProgress(t, "CreateVersionIndex")
	versionIndex, err := CreateVersionIndex(
		storageAPI,
		hashAPI,
		chunkerAPI,
		jobAPI,
		&createVersionProgress,
		"content",
		fileInfos,
		compressionTypes,
		32768)
	if err != nil {
		t.Errorf("TestRewriteVersion() CreateVersionIndex() %w", err)
	}

	storeIndex, err := CreateStoreIndex(
		hashAPI,
		versionIndex,
		65536,
		4096)
	if err != nil {
		t.Errorf("TestRewriteVersion() CreateStoreIndex() %w", err)
	}
	defer storeIndex.Dispose()
	blockStorageAPI := CreateFSBlockStore(jobAPI, storageAPI, "block_store")
	defer blockStorageAPI.Dispose()
	compressionRegistry := CreateZStdCompressionRegistry()
	compressionRegistry.Dispose()
	writeContentProgress := CreateProgress(t, "WriteContent")
	defer writeContentProgress.Dispose()

	err = WriteContent(
		storageAPI,
		blockStorageAPI,
		jobAPI,
		&writeContentProgress,
		storeIndex,
		versionIndex,
		"content")
	if err != nil {
		t.Errorf("TestRewriteVersion() WriteContent() %w", err)
	}

	getExistingContentComplete := &testGetExistingContentCompletionAPI{}
	getExistingContentComplete.wg.Add(1)
	err = blockStorageAPI.GetExistingContent(versionIndex.GetChunkHashes(), 0, CreateAsyncGetExistingContentAPI(getExistingContentComplete))
	if err != nil {
		t.Errorf("TestBlockStoreProxyFull() blockStoreAPI.GetExistingContent() %w", err)
		getExistingContentComplete.wg.Done()
	}
	getExistingContentComplete.wg.Wait()
	existingStoreIndex := getExistingContentComplete.storeIndex
	defer existingStoreIndex.Dispose()

	writeVersionProgress2 := CreateProgress(t, "WriteVersion")
	err = WriteVersion(
		blockStorageAPI,
		storageAPI,
		jobAPI,
		&writeVersionProgress2,
		existingStoreIndex,
		versionIndex,
		"content_copy",
		true)
	if err != nil {
		t.Errorf("TestRewriteVersion() WriteVersion() %w", err)
	}
}
