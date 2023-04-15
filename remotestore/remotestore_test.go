package remotestore

import (
	"context"
	"log"
	"net/url"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestCreateRemoteBlobStore(t *testing.T) {
	blobStore, _ := longtailstorelib.NewMemBlobStore("the_path", true)
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		false,
		nil,
		runtime.NumCPU(),
		ReadOnly)
	assert.Equal(t, nil, err)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()
}

type getExistingContentCompletionAPI struct {
	wg         sync.WaitGroup
	storeIndex longtaillib.Longtail_StoreIndex
	err        error
}

func (a *getExistingContentCompletionAPI) OnComplete(storeIndex longtaillib.Longtail_StoreIndex, err error) {
	a.storeIndex = storeIndex
	a.err = err
	a.wg.Done()
}

type putStoredBlockCompletionAPI struct {
	wg  sync.WaitGroup
	err error
}

func (a *putStoredBlockCompletionAPI) OnComplete(err error) {
	a.err = err
	a.wg.Done()
}

type getStoredBlockCompletionAPI struct {
	wg          sync.WaitGroup
	storedBlock longtaillib.Longtail_StoredBlock
	err         error
}

func (a *getStoredBlockCompletionAPI) OnComplete(storedBlock longtaillib.Longtail_StoredBlock, err error) {
	a.storedBlock = storedBlock
	a.err = err
	a.wg.Done()
}

func getExistingContent(storeAPI longtaillib.Longtail_BlockStoreAPI, chunkHashes []uint64, minBlockUsagePercent uint32) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "getExistingContent"
	g := &getExistingContentCompletionAPI{}
	g.wg.Add(1)
	err := storeAPI.GetExistingContent(chunkHashes, minBlockUsagePercent, longtaillib.CreateAsyncGetExistingContentAPI(g))
	if err != nil {
		g.wg.Done()
		g.wg.Wait()
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	g.wg.Wait()
	return g.storeIndex, errors.Wrap(g.err, fname)
}

type pruneBlocksCompletionAPI struct {
	wg               sync.WaitGroup
	prunedBlockCount uint32
	err              error
}

func (a *pruneBlocksCompletionAPI) OnComplete(prunedBlockCount uint32, err error) {
	a.err = err
	a.prunedBlockCount = prunedBlockCount
	a.wg.Done()
}

func pruneBlocksSync(indexStore longtaillib.Longtail_BlockStoreAPI, keepBlockHashes []uint64) (uint32, error) {
	const fname = "pruneBlocksSync"
	pruneBlocksComplete := &pruneBlocksCompletionAPI{}
	pruneBlocksComplete.wg.Add(1)
	err := indexStore.PruneBlocks(keepBlockHashes, longtaillib.CreateAsyncPruneBlocksAPI(pruneBlocksComplete))
	if err != nil {
		pruneBlocksComplete.wg.Done()
		pruneBlocksComplete.wg.Wait()
		return 0, errors.Wrap(err, fname)
	}
	pruneBlocksComplete.wg.Wait()
	return pruneBlocksComplete.prunedBlockCount, errors.Wrap(pruneBlocksComplete.err, fname)
}

func TestEmptyGetExistingContent(t *testing.T) {
	blobStore, _ := longtailstorelib.NewMemBlobStore("the_path", true)
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		false,
		nil,
		runtime.NumCPU(),
		ReadOnly)
	assert.Equal(t, nil, err)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()

	chunkHashes := []uint64{1, 2, 3, 4}

	existingContent, err := getExistingContent(storeAPI, chunkHashes, 0)
	defer existingContent.Dispose()
	assert.Equal(t, nil, err)
	assert.True(t, existingContent.IsValid())
	assert.NotEqual(t, existingContent.GetBlockCount(), 0)
}

func TestPutGetStoredBlock(t *testing.T) {
	blobStore, _ := longtailstorelib.NewMemBlobStore("the_path", true)
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		false,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, nil, err)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()

	storedBlock, err := storeBlockFromSeed(storeAPI, 0)
	assert.Equal(t, nil, err)
	blockHash := storedBlock.GetBlockHash()

	storedBlockCopy, err := fetchBlockFromStore(t, storeAPI, blockHash)
	assert.Equal(t, nil, err)
	defer storedBlockCopy.Dispose()

	assert.True(t, storedBlockCopy.IsValid())
	validateBlockFromSeed(t, 0, storedBlockCopy)
}

type flushCompletionAPI struct {
	wg  sync.WaitGroup
	err error
}

func (a *flushCompletionAPI) OnComplete(err error) {
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
		false,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, nil, err)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()

	_, err = storeBlockFromSeed(storeAPI, 0)
	assert.Equal(t, nil, err)
	_, err = storeBlockFromSeed(storeAPI, 10)
	assert.Equal(t, nil, err)
	_, err = storeBlockFromSeed(storeAPI, 20)
	assert.Equal(t, nil, err)
	_, err = storeBlockFromSeed(storeAPI, 30)
	assert.Equal(t, nil, err)
	_, err = storeBlockFromSeed(storeAPI, 40)
	assert.Equal(t, nil, err)
	_, err = storeBlockFromSeed(storeAPI, 50)
	assert.Equal(t, nil, err)

	chunkHashes := []uint64{uint64(0) + 1, uint64(0) + 2, uint64(10) + 1, uint64(10) + 3, uint64(20) + 1, uint64(20) + 2, uint64(30) + 2, uint64(30) + 3, uint64(40) + 1, uint64(40) + 3, uint64(50) + 1}

	remoteStoreFlushComplete := &flushCompletionAPI{}
	remoteStoreFlushComplete.wg.Add(1)
	_ = remoteStore.Flush(longtaillib.CreateAsyncFlushAPI(remoteStoreFlushComplete))
	remoteStoreFlushComplete.wg.Wait()

	existingContent, _ := getExistingContent(storeAPI, chunkHashes, 0)
	defer existingContent.Dispose()
	assert.True(t, existingContent.IsValid())

	assert.Equal(t, existingContent.GetBlockCount(), uint32(6))
	assert.Equal(t, existingContent.GetChunkCount(), uint32(18))
}

func TestRestoreStore(t *testing.T) {
	blobStore, _ := longtailstorelib.NewMemBlobStore("the_path", true)
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		false,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, nil, err)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)

	blocks := make([]longtaillib.Longtail_StoredBlock, 3)

	blocks[0], err = storeBlockFromSeed(storeAPI, 0)
	assert.Equal(t, nil, err)
	blocks[1], err = storeBlockFromSeed(storeAPI, 10)
	assert.Equal(t, nil, err)
	blocks[2], err = storeBlockFromSeed(storeAPI, 20)
	assert.Equal(t, nil, err)

	defer blocks[0].Dispose()
	defer blocks[1].Dispose()
	defer blocks[1].Dispose()

	storeAPI.Dispose()

	remoteStore, err = NewRemoteBlockStore(
		jobs,
		blobStore,
		false,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, nil, err)
	storeAPI = longtaillib.CreateBlockStoreAPI(remoteStore)

	chunkHashes := []uint64{uint64(0) + 1, uint64(0) + 2, uint64(10) + 1, uint64(10) + 3}

	existingContent, _ := getExistingContent(storeAPI, chunkHashes, 0)
	assert.True(t, existingContent.IsValid())

	assert.Equal(t, existingContent.GetBlockCount(), uint32(2))
	assert.Equal(t, existingContent.GetChunkCount(), uint32(6))
	existingContent.Dispose()

	chunkHashes = []uint64{uint64(0) + 1, uint64(0) + 2, uint64(10) + 1, uint64(10) + 3, uint64(30) + 1}

	existingContent, _ = getExistingContent(storeAPI, chunkHashes, 0)
	assert.True(t, existingContent.IsValid())

	assert.Equal(t, existingContent.GetBlockCount(), uint32(2))
	assert.Equal(t, existingContent.GetChunkCount(), uint32(6))

	_, err = storeBlockFromSeed(storeAPI, 30)
	assert.Equal(t, nil, err)
	existingContent.Dispose()
	storeAPI.Dispose()

	remoteStore, err = NewRemoteBlockStore(
		jobs,
		blobStore,
		false,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, nil, err)
	storeAPI = longtaillib.CreateBlockStoreAPI(remoteStore)

	chunkHashes = []uint64{uint64(0) + 1, uint64(0) + 2, uint64(10) + 1, uint64(10) + 3, uint64(30) + 1}

	existingContent, _ = getExistingContent(storeAPI, chunkHashes, 0)

	assert.True(t, existingContent.IsValid())

	assert.Equal(t, existingContent.GetBlockCount(), uint32(3))
	assert.Equal(t, existingContent.GetChunkCount(), uint32(9))
	existingContent.Dispose()

	storeAPI.Dispose()
}

func storeBlock(blobClient longtailstorelib.BlobClient, storedBlock longtaillib.Longtail_StoredBlock, blockHashOffset uint64, parentPath string) uint64 {
	bytes, _ := longtaillib.WriteStoredBlockToBuffer(storedBlock)
	defer bytes.Dispose()
	blockIndex := storedBlock.GetBlockIndex()
	storedBlockHash := blockIndex.GetBlockHash() + blockHashOffset
	path := getBlockPath("chunks", storedBlockHash)
	if len(parentPath) > 0 {
		path = parentPath + "/" + path
	}
	blobObject, _ := blobClient.NewObject(path)
	blobObject.Write(bytes.ToBuffer())
	return storedBlockHash
}

func generateStoredBlock(seed uint8) (longtaillib.Longtail_StoredBlock, error) {
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

func generateUniqueStoredBlock(seed uint8) (longtaillib.Longtail_StoredBlock, error) {
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

func storeBlockFromSeed(storeAPI longtaillib.Longtail_BlockStoreAPI, seed uint8) (longtaillib.Longtail_StoredBlock, error) {
	const fname = "storeBlockFromSeed"
	storedBlock, err := generateStoredBlock(seed)
	if err != nil {
		return longtaillib.Longtail_StoredBlock{}, errors.Wrap(err, fname)
	}

	p := &putStoredBlockCompletionAPI{}
	p.wg.Add(1)
	err = storeAPI.PutStoredBlock(storedBlock, longtaillib.CreateAsyncPutStoredBlockAPI(p))
	if err != nil {
		p.wg.Done()
		storedBlock.Dispose()
		return longtaillib.Longtail_StoredBlock{}, errors.Wrap(err, fname)
	}
	p.wg.Wait()

	if p.err != nil {
		return longtaillib.Longtail_StoredBlock{}, errors.Wrap(p.err, fname)
	}
	return storedBlock, nil
}

func fetchBlockFromStore(t *testing.T, storeAPI longtaillib.Longtail_BlockStoreAPI, blockHash uint64) (longtaillib.Longtail_StoredBlock, error) {
	const fname = "fetchBlockFromStore"
	g := &getStoredBlockCompletionAPI{}
	g.wg.Add(1)
	err := storeAPI.GetStoredBlock(blockHash, longtaillib.CreateAsyncGetStoredBlockAPI(g))
	if err != nil {
		g.wg.Done()
		return longtaillib.Longtail_StoredBlock{}, errors.Wrap(err, fname)
	}
	g.wg.Wait()

	if g.err != nil {
		return longtaillib.Longtail_StoredBlock{}, errors.Wrap(g.err, fname)
	}
	return g.storedBlock, nil
}

func validateBlockFromSeed(t *testing.T, seed uint8, storedBlock longtaillib.Longtail_StoredBlock) {
	assert.True(t, storedBlock.IsValid())

	storedBlockIndex := storedBlock.GetBlockIndex()

	assert.Equal(t, storedBlockIndex.GetBlockHash(), uint64(seed)+21412151)
	assert.Equal(t, storedBlockIndex.GetChunkCount(), uint32(3))
}

func TestBlockScanning(t *testing.T) {
	// Create stored blocks
	// Create/move stored blocks to faulty path
	// Scan and make sure we only get the blocks in the currect path
	blobStore, _ := longtailstorelib.NewMemBlobStore("", true)
	blobClient, _ := blobStore.NewClient(context.Background())

	goodBlockInCorrectPath, _ := generateStoredBlock(7)
	goodBlockInCorrectPathHash := storeBlock(blobClient, goodBlockInCorrectPath, 0, "")

	badBlockInCorrectPath, _ := generateStoredBlock(14)
	badBlockInCorrectPathHash := storeBlock(blobClient, badBlockInCorrectPath, 1, "")

	goodBlockInBadPath, _ := generateStoredBlock(21)
	goodBlockInBadPathHash := storeBlock(blobClient, goodBlockInBadPath, 0, "chunks")

	badBlockInBatPath, _ := generateStoredBlock(33)
	badBlockInBatPathHash := storeBlock(blobClient, badBlockInBatPath, 2, "chunks")

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		false,
		nil,
		runtime.NumCPU(),
		Init)
	assert.Equal(t, nil, err)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()

	b, err := fetchBlockFromStore(t, storeAPI, goodBlockInCorrectPathHash)
	assert.Equal(t, nil, err)
	b.Dispose()

	_, err = fetchBlockFromStore(t, storeAPI, badBlockInCorrectPathHash)
	assert.True(t, longtaillib.IsBadFormat(err))

	_, err = fetchBlockFromStore(t, storeAPI, goodBlockInBadPathHash)
	assert.True(t, longtaillib.IsNotExist(err))

	_, err = fetchBlockFromStore(t, storeAPI, badBlockInBatPathHash)
	assert.True(t, longtaillib.IsNotExist(err))

	goodBlockInCorrectPathIndex := goodBlockInCorrectPath.GetBlockIndex()
	chunks := goodBlockInCorrectPathIndex.GetChunkHashes()
	badBlockInCorrectPathIndex := badBlockInCorrectPath.GetBlockIndex()
	chunks = append(chunks, badBlockInCorrectPathIndex.GetChunkHashes()...)
	goodBlockInBadPathIndex := goodBlockInBadPath.GetBlockIndex()
	chunks = append(chunks, goodBlockInBadPathIndex.GetChunkHashes()...)
	badBlockInBatPathIndex := badBlockInBatPath.GetBlockIndex()
	chunks = append(chunks, badBlockInBatPathIndex.GetChunkHashes()...)

	existingContent, err := getExistingContent(storeAPI, chunks, 0)
	assert.Equal(t, nil, err)
	defer existingContent.Dispose()
	assert.Equal(t, len(goodBlockInCorrectPathIndex.GetChunkHashes()), len(existingContent.GetChunkHashes()))
}

func PruneStoreTest(syncStore bool, t *testing.T) {
	blobStore, _ := longtailstorelib.NewMemBlobStore("the_path", syncStore)
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		false,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, nil, err)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)

	blocks := make([]longtaillib.Longtail_StoredBlock, 3)

	blocks[0], err = storeBlockFromSeed(storeAPI, 0)
	assert.Equal(t, nil, err)
	blocks[1], err = storeBlockFromSeed(storeAPI, 10)
	assert.Equal(t, nil, err)
	blocks[2], err = storeBlockFromSeed(storeAPI, 20)
	assert.Equal(t, nil, err)

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

	fullStoreIndex, err := getExistingContent(storeAPI, chunkHashes, 0)
	assert.Equal(t, nil, err)
	assert.True(t, fullStoreIndex.IsValid())
	defer fullStoreIndex.Dispose()

	storeAPI.Dispose()

	remoteStore, err = NewRemoteBlockStore(
		jobs,
		blobStore,
		false,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, nil, err)
	storeAPI = longtaillib.CreateBlockStoreAPI(remoteStore)

	keepBlockHashes := make([]uint64, 2)
	keepBlockHashes[0] = blockHashes[0]
	keepBlockHashes[1] = blockHashes[2]
	pruneBlockCount, _ := pruneBlocksSync(storeAPI, keepBlockHashes)
	assert.Equal(t, pruneBlockCount, uint32(1))

	remoteStoreFlushComplete := &flushCompletionAPI{}
	remoteStoreFlushComplete.wg.Add(1)
	_ = remoteStore.Flush(longtaillib.CreateAsyncFlushAPI(remoteStoreFlushComplete))
	remoteStoreFlushComplete.wg.Wait()

	storeAPI.Dispose()

	remoteStore, err = NewRemoteBlockStore(
		jobs,
		blobStore,
		false,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, nil, err)
	storeAPI = longtaillib.CreateBlockStoreAPI(remoteStore)

	prunedStoreIndex, err := getExistingContent(storeAPI, chunkHashes, 0)
	assert.Equal(t, nil, err)
	assert.True(t, prunedStoreIndex.IsValid())
	defer prunedStoreIndex.Dispose()

	assert.Equal(t, 2, len(prunedStoreIndex.GetBlockHashes()))

	expectedChunkCount := len(chunkHashesPerBlock[0]) + len(chunkHashesPerBlock[2])
	assert.Equal(t, expectedChunkCount, len(prunedStoreIndex.GetChunkHashes()))

	_, err = fetchBlockFromStore(t, storeAPI, blockHashes[1])
	assert.True(t, longtaillib.IsNotExist(err))

	storeAPI.Dispose()
}

func TestPruneStoreWithLocking(t *testing.T) {
	PruneStoreTest(true, t)
}

func TestPruneStoreWithoutLocking(t *testing.T) {
	PruneStoreTest(false, t)
}

func validateThatAllBlocksAreInIndex(generatedBlocksIndex longtaillib.Longtail_StoreIndex, storeIndex longtaillib.Longtail_StoreIndex) bool {
	if !storeIndex.IsValid() {
		log.Printf("PutStoreLSI() returned invalid store index")
		return false
	}

	lookup := map[uint64]bool{}
	for _, h := range storeIndex.GetBlockHashes() {
		lookup[h] = true
	}

	blockHashes := generatedBlocksIndex.GetBlockHashes()
	for _, h := range blockHashes {
		_, exists := lookup[h]
		if !exists {
			return false
		}
	}
	return true
}

func validateThatBlocksArePresent(generatedBlocksIndex longtaillib.Longtail_StoreIndex, blobStore longtailstorelib.BlobStore) bool {
	storeIndex, err := GetStoreLSI(context.Background(), blobStore, nil, 8)
	if err != nil {
		log.Printf("GetStoreLSI() failed with %s", err)
		return false
	}
	defer storeIndex.Dispose()
	return validateThatAllBlocksAreInIndex(generatedBlocksIndex, storeIndex)
}

func testStoreIndexSync(blobStore longtailstorelib.BlobStore, t *testing.T, maxStoreIndexSize int64) {

	blockGenerateCount := 4
	workerCount := 51
	assert.True(t, blockGenerateCount*(workerCount+1) < 255)

	generatedBlockHashes := make(chan uint64, blockGenerateCount*workerCount)

	var wg sync.WaitGroup
	wg.Add(workerCount)
	for n := 0; n < workerCount; n++ {
		seedBase := blockGenerateCount * (n + 1)
		go func(blockGenerateCount int, seedBase int) {
			defer wg.Done()
			client, _ := blobStore.NewClient(context.Background())
			defer client.Close()
			blocks := []longtaillib.Longtail_StoredBlock{}
			defer func() {
				for _, block := range blocks {
					block.Dispose()
				}
			}()

			blockIndexes := []longtaillib.Longtail_BlockIndex{}
			{
				for i := 0; i < blockGenerateCount-1; i++ {
					block, err := generateUniqueStoredBlock(uint8(seedBase + i))
					assert.Equal(t, nil, err)
					blocks = append(blocks, block)
					blockIndexes = append(blockIndexes, block.GetBlockIndex())
				}

				blocksIndex, err := longtaillib.CreateStoreIndexFromBlocks(blockIndexes)
				assert.Equal(t, nil, err)
				for {
					var newStoreIndex longtaillib.Longtail_StoreIndex
					newStoreIndex, err = PutStoreLSI(context.Background(), blobStore, nil, blocksIndex, maxStoreIndexSize, 8)
					if err == nil {
						assert.True(t, validateThatAllBlocksAreInIndex(blocksIndex, newStoreIndex))
						newStoreIndex.Dispose()
						break
					}
				}
				assert.Equal(t, nil, err)
				for !validateThatBlocksArePresent(blocksIndex, blobStore) {
					time.Sleep(2 * time.Millisecond)
				}
				blocksIndex.Dispose()
			}

			generatedBlocksIndex := longtaillib.Longtail_StoreIndex{}
			{
				for i := blockGenerateCount - 1; i < blockGenerateCount; i++ {
					block, err := generateUniqueStoredBlock(uint8(seedBase + i))
					assert.Equal(t, nil, err)
					blocks = append(blocks, block)
					blockIndexes = append(blockIndexes, block.GetBlockIndex())
				}
				var err error
				generatedBlocksIndex, err = longtaillib.CreateStoreIndexFromBlocks(blockIndexes)
				assert.Equal(t, nil, err)
				for {
					var newStoreIndex longtaillib.Longtail_StoreIndex
					newStoreIndex, err = PutStoreLSI(context.Background(), blobStore, nil, generatedBlocksIndex, maxStoreIndexSize, 8)
					if err == nil {
						assert.True(t, validateThatAllBlocksAreInIndex(generatedBlocksIndex, newStoreIndex))
						newStoreIndex.Dispose()
						break
					}
				}
				assert.Equal(t, nil, err)
				for !validateThatBlocksArePresent(generatedBlocksIndex, blobStore) {
					time.Sleep(2 * time.Millisecond)
				}
			}

			blockHashes := generatedBlocksIndex.GetBlockHashes()
			for n := 0; n < blockGenerateCount; n++ {
				h := blockHashes[n]
				generatedBlockHashes <- h
			}
			generatedBlocksIndex.Dispose()
		}(blockGenerateCount, seedBase)
	}
	wg.Wait()

	generatedBlocks := map[uint64]bool{}
	for n := 0; n < workerCount*blockGenerateCount; n++ {
		h := <-generatedBlockHashes
		generatedBlocks[h] = true
	}
	assert.Equal(t, blockGenerateCount*workerCount, len(generatedBlocks))

	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()

	storeIndex, err := GetStoreLSI(context.Background(), blobStore, nil, 8)
	assert.Equal(t, nil, err)
	defer storeIndex.Dispose()
	assert.Equal(t, blockGenerateCount*workerCount, len(storeIndex.GetBlockHashes()))
	lookup := map[uint64]bool{}
	for _, h := range storeIndex.GetBlockHashes() {
		_, exists := lookup[h]
		assert.False(t, exists)
		_, generated := generatedBlocks[h]
		assert.True(t, generated)
		lookup[h] = true
	}
	assert.Equal(t, blockGenerateCount*workerCount, len(lookup))
}

func TestStoreIndexSyncNeverMerge(t *testing.T) {
	blobStore, err := longtailstorelib.NewMemBlobStore("store", false)
	assert.Equal(t, nil, err)
	testStoreIndexSync(blobStore, t, 0)
}

func TestStoreIndexSyncAlwaysMerge(t *testing.T) {
	blobStore, err := longtailstorelib.NewMemBlobStore("store", false)
	assert.Equal(t, nil, err)
	testStoreIndexSync(blobStore, t, 0x7fffffffffffffff)
}

func TestStoreIndexSyncSometimesMerge(t *testing.T) {
	blobStore, err := longtailstorelib.NewMemBlobStore("store", false)
	assert.Equal(t, nil, err)
	testStoreIndexSync(blobStore, t, 420)
}

func TestGCSStoreIndexSync(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-gcs-blob-store-sync")
	assert.Equal(t, nil, err)

	blobStore, err := longtailstorelib.NewGCSBlobStore(u, false)
	assert.Equal(t, nil, err)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	object, _ := client.NewObject("store.lsi")
	object.Delete()

	testStoreIndexSync(blobStore, t, 1024)
}

func TestS3StoreIndexSync(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	t.Skip()

	u, err := url.Parse("s3://longtail-test/test-s3-blob-store-sync")
	assert.Equal(t, nil, err)

	blobStore, _ := longtailstorelib.NewS3BlobStore(u)
	testStoreIndexSync(blobStore, t, 1024)
}

func TestFSStoreIndexSync(t *testing.T) {
	storePath, err := os.MkdirTemp("", "TestFSStoreIndexSync")
	assert.Equal(t, nil, err)
	blobStore, err := longtailstorelib.NewFSBlobStore(storePath, false)
	assert.Equal(t, nil, err)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	object, _ := client.NewObject("store.lsi")
	object.Delete()

	testStoreIndexSync(blobStore, t, 1024)
}
