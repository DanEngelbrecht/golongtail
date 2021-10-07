package remotestore

import (
	"context"
	"log"
	"net/url"
	"runtime"
	"sync"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
)

func TestCreateRemoteBlobStore(t *testing.T) {
	blobStore, _ := longtailstorelib.NewMemBlobStore("the_path", true)
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		"",
		runtime.NumCPU(),
		ReadOnly)
	if err != nil {
		t.Errorf("TestCreateRemoveBlobStore() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()
}

type getExistingContentCompletionAPI struct {
	wg         sync.WaitGroup
	storeIndex longtaillib.Longtail_StoreIndex
	err        int
}

func (a *getExistingContentCompletionAPI) OnComplete(storeIndex longtaillib.Longtail_StoreIndex, errno int) {
	a.storeIndex = storeIndex
	a.err = errno
	a.wg.Done()
}

type putStoredBlockCompletionAPI struct {
	wg  sync.WaitGroup
	err int
}

func (a *putStoredBlockCompletionAPI) OnComplete(errno int) {
	a.err = errno
	a.wg.Done()
}

type getStoredBlockCompletionAPI struct {
	wg          sync.WaitGroup
	storedBlock longtaillib.Longtail_StoredBlock
	err         int
}

func (a *getStoredBlockCompletionAPI) OnComplete(storedBlock longtaillib.Longtail_StoredBlock, errno int) {
	a.storedBlock = storedBlock
	a.err = errno
	a.wg.Done()
}

func getExistingContent(t *testing.T, storeAPI longtaillib.Longtail_BlockStoreAPI, chunkHashes []uint64, minBlockUsagePercent uint32) (longtaillib.Longtail_StoreIndex, int) {
	g := &getExistingContentCompletionAPI{}
	g.wg.Add(1)
	errno := storeAPI.GetExistingContent(chunkHashes, minBlockUsagePercent, longtaillib.CreateAsyncGetExistingContentAPI(g))
	if errno != 0 {
		g.wg.Done()
		return longtaillib.Longtail_StoreIndex{}, errno
	}
	g.wg.Wait()
	return g.storeIndex, g.err
}

type pruneBlocksCompletionAPI struct {
	wg               sync.WaitGroup
	prunedBlockCount uint32
	err              int
}

func (a *pruneBlocksCompletionAPI) OnComplete(prunedBlockCount uint32, err int) {
	a.err = err
	a.prunedBlockCount = prunedBlockCount
	a.wg.Done()
}

func pruneBlocksSync(indexStore longtaillib.Longtail_BlockStoreAPI, keepBlockHashes []uint64) (uint32, int) {
	pruneBlocksComplete := &pruneBlocksCompletionAPI{}
	pruneBlocksComplete.wg.Add(1)
	errno := indexStore.PruneBlocks(keepBlockHashes, longtaillib.CreateAsyncPruneBlocksAPI(pruneBlocksComplete))
	if errno != 0 {
		pruneBlocksComplete.wg.Done()
		pruneBlocksComplete.wg.Wait()
		return 0, errno
	}
	pruneBlocksComplete.wg.Wait()
	return pruneBlocksComplete.prunedBlockCount, pruneBlocksComplete.err
}

func TestEmptyGetExistingContent(t *testing.T) {
	blobStore, _ := longtailstorelib.NewMemBlobStore("the_path", true)
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		"",
		runtime.NumCPU(),
		ReadOnly)
	if err != nil {
		t.Errorf("TestCreateRemoveBlobStore() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)

	chunkHashes := []uint64{1, 2, 3, 4}

	existingContent, errno := getExistingContent(t, storeAPI, chunkHashes, 0)
	defer existingContent.Dispose()
	if errno != 0 {
		t.Errorf("TestEmptyGetExistingContent() getExistingContent(t, storeAPI, chunkHashes) %d != %d", errno, 0)
	}

	if !existingContent.IsValid() {
		t.Errorf("TestEmptyGetExistingContent() g.err %t != %t", existingContent.IsValid(), true)
	}

	if existingContent.GetBlockCount() != 0 {
		t.Errorf("TestEmptyGetExistingContent() existingContent.GetBlockCount() %d != %d", existingContent.GetBlockCount(), 0)
	}

	defer storeAPI.Dispose()
}

func TestPutGetStoredBlock(t *testing.T) {
	blobStore, _ := longtailstorelib.NewMemBlobStore("the_path", true)
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		"",
		runtime.NumCPU(),
		ReadWrite)
	if err != nil {
		t.Errorf("TestPutGetStoredBlock() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)

	storedBlock, errno := storeBlockFromSeed(t, storeAPI, 0)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 0) %d != %d", errno, 0)
	}
	blockHash := storedBlock.GetBlockHash()

	storedBlockCopy, errno := fetchBlockFromStore(t, storeAPI, blockHash)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() fetchBlockFromStore(t, storeAPI, 0) %d != %d", errno, 0)
	}
	defer storedBlockCopy.Dispose()

	if !storedBlockCopy.IsValid() {
		t.Errorf("TestPutGetStoredBlock() g.err %t != %t", storedBlockCopy.IsValid(), true)
	}

	validateBlockFromSeed(t, 0, storedBlockCopy)

	defer storeAPI.Dispose()
}

type flushCompletionAPI struct {
	wg  sync.WaitGroup
	err int
}

func (a *flushCompletionAPI) OnComplete(err int) {
	a.err = err
	a.wg.Done()
}

func TestGetExistingContent(t *testing.T) {
	blobStore, _ := longtailstorelib.NewMemBlobStore("the_path", true)
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		"",
		runtime.NumCPU(),
		ReadWrite)
	if err != nil {
		t.Errorf("TestPutGetStoredBlock() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()

	_, errno := storeBlockFromSeed(t, storeAPI, 0)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 0) %d != %d", errno, 0)
	}
	_, errno = storeBlockFromSeed(t, storeAPI, 10)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 0) %d != %d", errno, 0)
	}
	_, errno = storeBlockFromSeed(t, storeAPI, 20)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 0) %d != %d", errno, 0)
	}
	_, errno = storeBlockFromSeed(t, storeAPI, 30)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 0) %d != %d", errno, 0)
	}
	_, errno = storeBlockFromSeed(t, storeAPI, 40)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 0) %d != %d", errno, 0)
	}
	_, errno = storeBlockFromSeed(t, storeAPI, 50)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 0) %d != %d", errno, 0)
	}

	chunkHashes := []uint64{uint64(0) + 1, uint64(0) + 2, uint64(10) + 1, uint64(10) + 3, uint64(20) + 1, uint64(20) + 2, uint64(30) + 2, uint64(30) + 3, uint64(40) + 1, uint64(40) + 3, uint64(50) + 1}

	remoteStoreFlushComplete := &flushCompletionAPI{}
	remoteStoreFlushComplete.wg.Add(1)
	_ = remoteStore.Flush(longtaillib.CreateAsyncFlushAPI(remoteStoreFlushComplete))
	remoteStoreFlushComplete.wg.Wait()

	existingContent, errno := getExistingContent(t, storeAPI, chunkHashes, 0)
	defer existingContent.Dispose()
	if !existingContent.IsValid() {
		t.Errorf("TestGetExistingContent() g.err %t != %t", existingContent.IsValid(), true)
	}

	if existingContent.GetBlockCount() != 6 {
		t.Errorf("TestGetExistingContent() existingContent.GetBlockCount() %d != %d", existingContent.GetBlockCount(), 6)
	}

	if existingContent.GetChunkCount() != 18 {
		t.Errorf("TestGetExistingContent() existingContent.GetChunkCount() %d != %d", existingContent.GetChunkCount(), 18)
	}
}

func TestRestoreStore(t *testing.T) {
	blobStore, _ := longtailstorelib.NewMemBlobStore("the_path", true)
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		"",
		runtime.NumCPU(),
		ReadWrite)
	if err != nil {
		t.Errorf("TestPutGetStoredBlock() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)

	blocks := make([]longtaillib.Longtail_StoredBlock, 3)

	errno := 0

	blocks[0], errno = storeBlockFromSeed(t, storeAPI, 0)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 0) %d != %d", errno, 0)
	}
	blocks[1], errno = storeBlockFromSeed(t, storeAPI, 10)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 10) %d != %d", errno, 0)
	}
	blocks[2], errno = storeBlockFromSeed(t, storeAPI, 20)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 20) %d != %d", errno, 0)
	}

	defer blocks[0].Dispose()
	defer blocks[1].Dispose()
	defer blocks[1].Dispose()

	storeAPI.Dispose()

	remoteStore, err = NewRemoteBlockStore(
		jobs,
		blobStore,
		"",
		runtime.NumCPU(),
		ReadWrite)
	if err != nil {
		t.Errorf("TestPutGetStoredBlock() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI = longtaillib.CreateBlockStoreAPI(remoteStore)

	chunkHashes := []uint64{uint64(0) + 1, uint64(0) + 2, uint64(10) + 1, uint64(10) + 3}

	existingContent, errno := getExistingContent(t, storeAPI, chunkHashes, 0)
	if !existingContent.IsValid() {
		t.Errorf("TestRestoreStore() g.err %t != %t", existingContent.IsValid(), true)
	}

	if existingContent.GetBlockCount() != 2 {
		t.Errorf("TestRestoreStore() existingContent.GetBlockCount() %d != %d", existingContent.GetBlockCount(), 2)
	}

	if existingContent.GetChunkCount() != 6 {
		t.Errorf("TestRestoreStore() existingContent.GetChunkCount() %d != %d", existingContent.GetChunkCount(), 6)
	}

	chunkHashes = []uint64{uint64(0) + 1, uint64(0) + 2, uint64(10) + 1, uint64(10) + 3, uint64(30) + 1}

	existingContent, errno = getExistingContent(t, storeAPI, chunkHashes, 0)
	if !existingContent.IsValid() {
		t.Errorf("TestRestoreStore() g.err %t != %t", existingContent.IsValid(), true)
	}

	if existingContent.GetBlockCount() != 2 {
		t.Errorf("TestRestoreStore() existingContent.GetBlockCount() %d != %d", existingContent.GetBlockCount(), 2)
	}

	if existingContent.GetChunkCount() != 6 {
		t.Errorf("TestRestoreStore() existingContent.GetChunkCount() %d != %d", existingContent.GetChunkCount(), 6)
	}

	_, errno = storeBlockFromSeed(t, storeAPI, 30)
	if errno != 0 {
		t.Errorf("TestRestoreStore() storeBlock(t, storeAPI, 30) %d != %d", errno, 0)
	}
	existingContent.Dispose()
	storeAPI.Dispose()

	remoteStore, err = NewRemoteBlockStore(
		jobs,
		blobStore,
		"",
		runtime.NumCPU(),
		ReadWrite)
	if err != nil {
		t.Errorf("TestRestoreStore() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI = longtaillib.CreateBlockStoreAPI(remoteStore)

	chunkHashes = []uint64{uint64(0) + 1, uint64(0) + 2, uint64(10) + 1, uint64(10) + 3, uint64(30) + 1}

	existingContent, errno = getExistingContent(t, storeAPI, chunkHashes, 0)
	if !existingContent.IsValid() {
		t.Errorf("TestRestoreStore() g.err %t != %t", existingContent.IsValid(), true)
	}

	if existingContent.GetBlockCount() != 3 {
		t.Errorf("TestRestoreStore() existingContent.GetBlockCount() %d != %d", existingContent.GetBlockCount(), 3)
	}

	if existingContent.GetChunkCount() != 9 {
		t.Errorf("TestRestoreStore() existingContent.GetChunkCount() %d != %d", existingContent.GetChunkCount(), 9)
	}
	existingContent.Dispose()
	storeAPI.Dispose()
}

func createStoredBlock(chunkCount uint32, hashIdentifier uint32) (longtaillib.Longtail_StoredBlock, int) {
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

	return longtaillib.CreateStoredBlock(
		blockHash,
		hashIdentifier,
		chunkCount+uint32(10000),
		chunkHashes,
		chunkSizes,
		blockData,
		false)
}

func storeBlock(blobClient longtailstorelib.BlobClient, storedBlock longtaillib.Longtail_StoredBlock, blockHashOffset uint64, parentPath string) uint64 {
	bytes, _ := longtaillib.WriteStoredBlockToBuffer(storedBlock)
	blockIndex := storedBlock.GetBlockIndex()
	storedBlockHash := blockIndex.GetBlockHash() + blockHashOffset
	path := getBlockPath("chunks", storedBlockHash)
	if len(parentPath) > 0 {
		path = parentPath + "/" + path
	}
	blobObject, _ := blobClient.NewObject(path)
	blobObject.Write(bytes)
	return storedBlockHash
}

func generateStoredBlock(t *testing.T, seed uint8) (longtaillib.Longtail_StoredBlock, int) {
	chunkHashes := []uint64{uint64(seed) + 1, uint64(seed) + 2, uint64(seed) + 3}
	chunkSizes := []uint32{uint32(seed) + 10, uint32(seed) + 20, uint32(seed) + 30}

	blockDataLen := (int)(chunkSizes[0] + chunkSizes[1] + chunkSizes[2])
	blockData := make([]uint8, blockDataLen)
	for p := 0; p < blockDataLen; p++ {
		blockData[p] = seed
	}

	return longtaillib.CreateStoredBlock(
		uint64(seed)+21412151,
		997,
		2,
		chunkHashes,
		chunkSizes,
		blockData,
		false)
}

func generateUniqueStoredBlock(t *testing.T, seed uint8) (longtaillib.Longtail_StoredBlock, int) {
	chunkHashes := []uint64{uint64(seed)<<8 + 1, uint64(seed)<<8 + 2, uint64(seed)<<8 + 3}
	chunkSizes := []uint32{uint32(seed)<<8 + 10, uint32(seed)<<8 + 20, uint32(seed)<<8 + 30}

	blockDataLen := (int)(chunkSizes[0] + chunkSizes[1] + chunkSizes[2])
	blockData := make([]uint8, blockDataLen)
	for p := 0; p < blockDataLen; p++ {
		blockData[p] = seed
	}

	return longtaillib.CreateStoredBlock(
		uint64(seed)<<16+21412151,
		997,
		2,
		chunkHashes,
		chunkSizes,
		blockData,
		false)
}

func storeBlockFromSeed(t *testing.T, storeAPI longtaillib.Longtail_BlockStoreAPI, seed uint8) (longtaillib.Longtail_StoredBlock, int) {
	storedBlock, errno := generateStoredBlock(t, seed)
	if errno != 0 {
		return longtaillib.Longtail_StoredBlock{}, errno
	}

	p := &putStoredBlockCompletionAPI{}
	p.wg.Add(1)
	errno = storeAPI.PutStoredBlock(storedBlock, longtaillib.CreateAsyncPutStoredBlockAPI(p))
	if errno != 0 {
		p.wg.Done()
		storedBlock.Dispose()
		return longtaillib.Longtail_StoredBlock{}, errno
	}
	p.wg.Wait()

	return storedBlock, p.err
}

func fetchBlockFromStore(t *testing.T, storeAPI longtaillib.Longtail_BlockStoreAPI, blockHash uint64) (longtaillib.Longtail_StoredBlock, int) {
	g := &getStoredBlockCompletionAPI{}
	g.wg.Add(1)
	errno := storeAPI.GetStoredBlock(blockHash, longtaillib.CreateAsyncGetStoredBlockAPI(g))
	if errno != 0 {
		g.wg.Done()
		return longtaillib.Longtail_StoredBlock{}, errno
	}
	g.wg.Wait()

	return g.storedBlock, g.err
}

func validateBlockFromSeed(t *testing.T, seed uint8, storedBlock longtaillib.Longtail_StoredBlock) {
	if !storedBlock.IsValid() {
		t.Errorf("validateBlockFromSeed() g.err %t != %t", storedBlock.IsValid(), true)
	}

	storedBlockIndex := storedBlock.GetBlockIndex()

	if storedBlockIndex.GetBlockHash() != uint64(seed)+21412151 {
		t.Errorf("validateBlockFromSeed() storedBlockIndex.GetBlockHash() %d != %d", storedBlockIndex.GetBlockHash(), uint64(seed)+21412151)
	}

	if storedBlockIndex.GetChunkCount() != 3 {
		t.Errorf("validateBlockFromSeed() storedBlockIndex.GetChunkCount() %d != %d", storedBlockIndex.GetChunkCount(), 3)
	}
}

func TestBlockScanning(t *testing.T) {
	// Create stored blocks
	// Create/move stored blocks to faulty path
	// Scan and make sure we only get the blocks in the currect path
	blobStore, _ := longtailstorelib.NewMemBlobStore("", true)
	blobClient, _ := blobStore.NewClient(context.Background())

	goodBlockInCorrectPath, _ := generateStoredBlock(t, 7)
	goodBlockInCorrectPathHash := storeBlock(blobClient, goodBlockInCorrectPath, 0, "")

	badBlockInCorrectPath, _ := generateStoredBlock(t, 14)
	badBlockInCorrectPathHash := storeBlock(blobClient, badBlockInCorrectPath, 1, "")

	goodBlockInBadPath, _ := generateStoredBlock(t, 21)
	goodBlockInBadPathHash := storeBlock(blobClient, goodBlockInBadPath, 0, "chunks")

	badBlockInBatPath, _ := generateStoredBlock(t, 33)
	badBlockInBatPathHash := storeBlock(blobClient, badBlockInBatPath, 2, "chunks")

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		"",
		runtime.NumCPU(),
		Init)
	if err != nil {
		t.Errorf("TestPutGetStoredBlock() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()

	b, errno := fetchBlockFromStore(t, storeAPI, goodBlockInCorrectPathHash)
	if errno != 0 {
		t.Errorf("TestBlockScanning() fetchBlockFromStore(t, storeAPI, goodBlockInCorrectPathHash) %d != %d", errno, 0)
	}
	b.Dispose()

	_, errno = fetchBlockFromStore(t, storeAPI, badBlockInCorrectPathHash)
	if errno != longtaillib.EBADF {
		t.Errorf("TestBlockScanning() fetchBlockFromStore(t, storeAPI, badBlockInCorrectPathHash) %d != %d", errno, longtaillib.ENOENT)
	}

	_, errno = fetchBlockFromStore(t, storeAPI, goodBlockInBadPathHash)
	if errno != longtaillib.ENOENT {
		t.Errorf("TestBlockScanning() fetchBlockFromStore(t, storeAPI, goodBlockInBadPathHash) %d != %d", errno, longtaillib.ENOENT)
	}

	_, errno = fetchBlockFromStore(t, storeAPI, badBlockInBatPathHash)
	if errno != longtaillib.ENOENT {
		t.Errorf("TestBlockScanning() fetchBlockFromStore(t, storeAPI, badBlockInBatPathHash) %d != %d", errno, longtaillib.ENOENT)
	}

	goodBlockInCorrectPathIndex := goodBlockInCorrectPath.GetBlockIndex()
	chunks := goodBlockInCorrectPathIndex.GetChunkHashes()
	badBlockInCorrectPathIndex := badBlockInCorrectPath.GetBlockIndex()
	chunks = append(chunks, badBlockInCorrectPathIndex.GetChunkHashes()...)
	goodBlockInBadPathIndex := goodBlockInBadPath.GetBlockIndex()
	chunks = append(chunks, goodBlockInBadPathIndex.GetChunkHashes()...)
	badBlockInBatPathIndex := badBlockInBatPath.GetBlockIndex()
	chunks = append(chunks, badBlockInBatPathIndex.GetChunkHashes()...)

	existingContent, errno := getExistingContent(t, storeAPI, chunks, 0)
	if errno != 0 {
		t.Errorf("TestBlockScanning() getExistingContent(t, storeAPI, chunkHashes, 0) %d != %d", errno, 0)
	}
	defer existingContent.Dispose()
	if len(existingContent.GetChunkHashes()) != len(goodBlockInCorrectPathIndex.GetChunkHashes()) {
		t.Errorf("TestBlockScanning() getExistingContent(t, storeAPI, chunks, 0) %d!= %d", len(existingContent.GetChunkHashes()), len(goodBlockInCorrectPathIndex.GetChunkHashes()))
	}
}

func PruneStoreTest(syncStore bool, t *testing.T) {
	blobStore, _ := longtailstorelib.NewMemBlobStore("the_path", syncStore)
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		"",
		runtime.NumCPU(),
		ReadWrite)
	if err != nil {
		t.Errorf("TestPutGetStoredBlock() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)

	blocks := make([]longtaillib.Longtail_StoredBlock, 3)

	errno := 0
	blocks[0], errno = storeBlockFromSeed(t, storeAPI, 0)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 0) %d != %d", errno, 0)
	}
	blocks[1], errno = storeBlockFromSeed(t, storeAPI, 10)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 10) %d != %d", errno, 0)
	}
	blocks[2], errno = storeBlockFromSeed(t, storeAPI, 20)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 20) %d != %d", errno, 0)
	}

	blockIndexes := []longtaillib.Longtail_BlockIndex{
		blocks[0].GetBlockIndex(),
		blocks[1].GetBlockIndex(),
		blocks[2].GetBlockIndex()}

	blockHashes := []uint64{
		blockIndexes[0].GetBlockHash(),
		blockIndexes[1].GetBlockHash(),
		blockIndexes[2].GetBlockHash()}

	chunkHashesPerBlock := [][]uint64{
		blockIndexes[0].GetChunkHashes(),
		blockIndexes[1].GetChunkHashes(),
		blockIndexes[2].GetChunkHashes()}

	var chunkHashes []uint64
	chunkHashes = append(chunkHashes, chunkHashesPerBlock[0]...)
	chunkHashes = append(chunkHashes, chunkHashesPerBlock[1]...)
	chunkHashes = append(chunkHashes, chunkHashesPerBlock[2]...)

	defer blocks[0].Dispose()
	defer blocks[1].Dispose()
	defer blocks[1].Dispose()

	fullStoreIndex, errno := getExistingContent(t, storeAPI, chunkHashes, 0)
	if errno != 0 {
		t.Errorf("getExistingContent() errno %d != %d", 0, errno)
	}
	if !fullStoreIndex.IsValid() {
		t.Errorf("getExistingContent() errno %t != %t", true, fullStoreIndex.IsValid())
	}
	defer fullStoreIndex.Dispose()

	storeAPI.Dispose()

	remoteStore, err = NewRemoteBlockStore(
		jobs,
		blobStore,
		"",
		runtime.NumCPU(),
		ReadWrite)
	if err != nil {
		t.Errorf("TestPutGetStoredBlock() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI = longtaillib.CreateBlockStoreAPI(remoteStore)

	keepBlockHashes := make([]uint64, 2)
	keepBlockHashes[0] = blockHashes[0]
	keepBlockHashes[1] = blockHashes[2]
	pruneBlockCount, errno := pruneBlocksSync(storeAPI, keepBlockHashes)
	if pruneBlockCount != 1 {
		t.Errorf("pruneBlocksSync() pruneBlockCount %d != %d", 1, pruneBlockCount)
	}

	remoteStoreFlushComplete := &flushCompletionAPI{}
	remoteStoreFlushComplete.wg.Add(1)
	_ = remoteStore.Flush(longtaillib.CreateAsyncFlushAPI(remoteStoreFlushComplete))
	remoteStoreFlushComplete.wg.Wait()

	storeAPI.Dispose()

	remoteStore, err = NewRemoteBlockStore(
		jobs,
		blobStore,
		"",
		runtime.NumCPU(),
		ReadWrite)
	if err != nil {
		t.Errorf("TestPutGetStoredBlock() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI = longtaillib.CreateBlockStoreAPI(remoteStore)

	prunedStoreIndex, errno := getExistingContent(t, storeAPI, chunkHashes, 0)
	if errno != 0 {
		t.Errorf("getExistingContent() errno %d != %d", 0, errno)
	}
	if !prunedStoreIndex.IsValid() {
		t.Errorf("getExistingContent() errno %t != %t", true, prunedStoreIndex.IsValid())
	}
	defer prunedStoreIndex.Dispose()

	if len(prunedStoreIndex.GetBlockHashes()) != 2 {
		t.Errorf("len(prunedStoreIndex.GetBlockHashes() %d != %d", 2, len(prunedStoreIndex.GetBlockHashes()))
	}

	expectedChunkCount := len(chunkHashesPerBlock[0]) + len(chunkHashesPerBlock[2])
	if len(prunedStoreIndex.GetChunkHashes()) != expectedChunkCount {
		t.Errorf("len(prunedStoreIndex.GetChunkHashes() %d != %d", expectedChunkCount, len(prunedStoreIndex.GetChunkHashes()))
	}

	_, errno = fetchBlockFromStore(t, storeAPI, blockHashes[1])
	if errno != longtaillib.ENOENT {
		t.Errorf("fetchBlockFromStore() %d != %d", errno, longtaillib.ENOENT)
	}

	storeAPI.Dispose()
}

func TestPruneStoreWithLocking(t *testing.T) {
	PruneStoreTest(true, t)
}

func TestPruneStoreWithoutLocking(t *testing.T) {
	PruneStoreTest(false, t)
}

func testStoreIndexSync(blobStore longtailstorelib.BlobStore, t *testing.T) {

	blockGenerateCount := 4
	workerCount := 21

	generatedBlockHashes := make(chan uint64, blockGenerateCount*workerCount)

	var wg sync.WaitGroup
	for n := 0; n < workerCount; n++ {
		wg.Add(1)
		seedBase := blockGenerateCount * n
		go func(blockGenerateCount int, seedBase int) {
			client, _ := blobStore.NewClient(context.Background())
			defer client.Close()

			blocks := []longtaillib.Longtail_BlockIndex{}
			{
				for i := 0; i < blockGenerateCount-1; i++ {
					block, _ := generateUniqueStoredBlock(t, uint8(seedBase+i))
					blocks = append(blocks, block.GetBlockIndex())
				}

				blocksIndex, errno := longtaillib.CreateStoreIndexFromBlocks(blocks)
				if errno != 0 {
					wg.Done()
					t.Errorf("longtaillib.CreateStoreIndexFromBlocks() failed with %q", longtaillib.ErrnoToError(errno, longtaillib.ErrEIO))
				}
				newStoreIndex, err := addToRemoteStoreIndex(context.Background(), client, blocksIndex)
				blocksIndex.Dispose()
				if err != nil {
					wg.Done()
					t.Errorf("addToRemoteStoreIndex() failed with %q", err)
				}
				newStoreIndex.Dispose()
			}

			readStoreIndex, _, err := readStoreStoreIndexWithItems(context.Background(), client)
			if err != nil {
				wg.Done()
				t.Errorf("readStoreStoreIndexWithItems() failed with %q", err)
			}
			readStoreIndex.Dispose()

			generatedBlocksIndex := longtaillib.Longtail_StoreIndex{}
			{
				for i := blockGenerateCount - 1; i < blockGenerateCount; i++ {
					block, _ := generateUniqueStoredBlock(t, uint8(seedBase+i))
					blocks = append(blocks, block.GetBlockIndex())
				}
				errno := 0
				generatedBlocksIndex, errno = longtaillib.CreateStoreIndexFromBlocks(blocks)
				if errno != 0 {
					wg.Done()
					t.Errorf("longtaillib.CreateStoreIndexFromBlocks() failed with %q", longtaillib.ErrnoToError(errno, longtaillib.ErrEIO))
				}
				newStoreIndex, err := addToRemoteStoreIndex(context.Background(), client, generatedBlocksIndex)
				if err != nil {
					wg.Done()
					t.Errorf("addToRemoteStoreIndex() failed with %q", err)
				}
				newStoreIndex.Dispose()
			}

			storeIndex, _, err := readStoreStoreIndexWithItems(context.Background(), client)
			if err != nil {
				wg.Done()
				t.Errorf("readStoreStoreIndexWithItems() failed with %q", err)
			}
			if !storeIndex.IsValid() {
				wg.Done()
				t.Error("readStoreStoreIndexWithItems() returned invalid store index")
			}
			lookup := map[uint64]bool{}
			for _, h := range storeIndex.GetBlockHashes() {
				lookup[h] = true
			}
			storeIndex.Dispose()

			blockHashes := generatedBlocksIndex.GetBlockHashes()
			for n := 0; n < blockGenerateCount; n++ {
				h := blockHashes[n]
				generatedBlockHashes <- h
				_, exists := lookup[h]
				if !exists {
					generatedBlocksIndex.Dispose()
					wg.Done()
					t.Errorf("Missing direct block %d", h)
				}
			}
			generatedBlocksIndex.Dispose()

			wg.Done()
		}(blockGenerateCount, seedBase)
	}
	wg.Wait()
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()

	if !client.SupportsLocking() {
		// Consolidate indexes
		updateIndex, _ := longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
		newStoreIndex, _ := addToRemoteStoreIndex(context.Background(), client, updateIndex)
		updateIndex.Dispose()
		newStoreIndex.Dispose()
	}

	storeIndex, _, err := readStoreStoreIndexWithItems(context.Background(), client)
	if err != nil {
		t.Errorf("readStoreStoreIndexWithItems() failed with %q", err)
	}
	defer storeIndex.Dispose()
	if len(storeIndex.GetBlockHashes()) != blockGenerateCount*workerCount {
		t.Errorf("Unexpected number of blocks in index, expected %d, got %d", blockGenerateCount*workerCount, len(storeIndex.GetBlockHashes()))
	}
	lookup := map[uint64]bool{}
	for _, h := range storeIndex.GetBlockHashes() {
		lookup[h] = true
	}
	if len(lookup) != blockGenerateCount*workerCount {
		t.Errorf("Unexpected unique block hashes in index, expected %d, got %d", blockGenerateCount*workerCount, len(storeIndex.GetBlockHashes()))
	}

	for n := 0; n < workerCount*blockGenerateCount; n++ {
		h := <-generatedBlockHashes
		_, exists := lookup[h]
		if !exists {
			t.Errorf("Missing block %d", h)
		}
	}
}

func TestStoreIndexSyncWithLocking(t *testing.T) {
	blobStore, err := longtailstorelib.NewMemBlobStore("locking_store", true)
	if err != nil {
		t.Errorf("%v", err)
	}
	testStoreIndexSync(blobStore, t)
}

func TestStoreIndexSyncWithoutLocking(t *testing.T) {
	blobStore, err := longtailstorelib.NewMemBlobStore("locking_store", false)
	if err != nil {
		t.Errorf("%v", err)
	}
	testStoreIndexSync(blobStore, t)
}

func TestGCSStoreIndexSyncWithLocking(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-gcs-blob-store-sync")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}

	blobStore, err := longtailstorelib.NewGCSBlobStore(u, false)
	if err != nil {
		log.Fatalf("%v", err)
	}
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	object, _ := client.NewObject("store.lsi")
	object.Delete()

	testStoreIndexSync(blobStore, t)
}

func TestGCSStoreIndexSyncWithoutLocking(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-gcs-blob-store-sync")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}

	blobStore, err := longtailstorelib.NewGCSBlobStore(u, true)
	if err != nil {
		log.Fatalf("%v", err)
	}
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	object, _ := client.NewObject("store.lsi")
	object.Delete()

	testStoreIndexSync(blobStore, t)
}

func TestS3StoreIndexSync(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	t.Skip()

	u, err := url.Parse("s3://longtail-test/test-s3-blob-store-sync")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}

	blobStore, _ := longtailstorelib.NewS3BlobStore(u)
	testStoreIndexSync(blobStore, t)
}
