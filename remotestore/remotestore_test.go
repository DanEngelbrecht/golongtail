package remotestore

import (
	"context"
	"io/ioutil"
	"log"
	"net/url"
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
		true,
		nil,
		runtime.NumCPU(),
		ReadOnly)
	assert.Equal(t, err, nil)
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
		true,
		nil,
		runtime.NumCPU(),
		ReadOnly)
	assert.Equal(t, err, nil)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()

	chunkHashes := []uint64{1, 2, 3, 4}

	existingContent, err := getExistingContent(storeAPI, chunkHashes, 0)
	defer existingContent.Dispose()
	assert.Equal(t, err, nil)
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
		true,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, err, nil)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()

	storedBlock, err := storeBlockFromSeed(storeAPI, 0)
	assert.Equal(t, err, nil)
	blockHash := storedBlock.GetBlockHash()

	storedBlockCopy, err := fetchBlockFromStore(t, storeAPI, blockHash)
	assert.Equal(t, err, nil)
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
		true,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, err, nil)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()

	_, err = storeBlockFromSeed(storeAPI, 0)
	assert.Equal(t, err, nil)
	_, err = storeBlockFromSeed(storeAPI, 10)
	assert.Equal(t, err, nil)
	_, err = storeBlockFromSeed(storeAPI, 20)
	assert.Equal(t, err, nil)
	_, err = storeBlockFromSeed(storeAPI, 30)
	assert.Equal(t, err, nil)
	_, err = storeBlockFromSeed(storeAPI, 40)
	assert.Equal(t, err, nil)
	_, err = storeBlockFromSeed(storeAPI, 50)
	assert.Equal(t, err, nil)

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
		true,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, err, nil)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)

	blocks := make([]longtaillib.Longtail_StoredBlock, 3)

	blocks[0], err = storeBlockFromSeed(storeAPI, 0)
	assert.Equal(t, err, nil)
	blocks[1], err = storeBlockFromSeed(storeAPI, 10)
	assert.Equal(t, err, nil)
	blocks[2], err = storeBlockFromSeed(storeAPI, 20)
	assert.Equal(t, err, nil)

	defer blocks[0].Dispose()
	defer blocks[1].Dispose()
	defer blocks[1].Dispose()

	storeAPI.Dispose()

	remoteStore, err = NewRemoteBlockStore(
		jobs,
		blobStore,
		true,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, err, nil)
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
	assert.Equal(t, err, nil)
	existingContent.Dispose()
	storeAPI.Dispose()

	remoteStore, err = NewRemoteBlockStore(
		jobs,
		blobStore,
		true,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, err, nil)
	storeAPI = longtaillib.CreateBlockStoreAPI(remoteStore)

	chunkHashes = []uint64{uint64(0) + 1, uint64(0) + 2, uint64(10) + 1, uint64(10) + 3, uint64(30) + 1}

	existingContent, _ = getExistingContent(storeAPI, chunkHashes, 0)

	assert.True(t, existingContent.IsValid())

	assert.Equal(t, existingContent.GetBlockCount(), uint32(3))
	assert.Equal(t, existingContent.GetChunkCount(), uint32(9))
	existingContent.Dispose()

	storeAPI.Dispose()
}

//func createStoredBlock(chunkCount uint32, hashIdentifier uint32) (longtaillib.Longtail_StoredBlock, error) {
//	blockHash := uint64(0xdeadbeef500177aa) + uint64(chunkCount)
//	chunkHashes := make([]uint64, chunkCount)
//	chunkSizes := make([]uint32, chunkCount)
//	blockOffset := uint32(0)
//	for index := range chunkHashes {
//		chunkHashes[index] = uint64(index+1) * 4711
//		chunkSizes[index] = uint32(index+1) * 10
//		blockOffset += uint32(chunkSizes[index])
//	}
//	blockData := make([]uint8, blockOffset)
//	blockOffset = 0
//	for chunkIndex := range chunkHashes {
//		for index := uint32(0); index < uint32(chunkSizes[chunkIndex]); index++ {
//			blockData[blockOffset+index] = uint8(chunkIndex + 1)
//		}
//		blockOffset += uint32(chunkSizes[chunkIndex])
//	}
//
//	return longtaillib.CreateStoredBlock(
//		blockHash,
//		hashIdentifier,
//		chunkCount+uint32(10000),
//		chunkHashes,
//		chunkSizes,
//		blockData,
//		false)
//}

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
		true,
		nil,
		runtime.NumCPU(),
		Init)
	assert.Equal(t, err, nil)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()

	b, err := fetchBlockFromStore(t, storeAPI, goodBlockInCorrectPathHash)
	assert.Equal(t, err, nil)
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
	assert.Equal(t, err, nil)
	defer existingContent.Dispose()
	assert.Equal(t, len(existingContent.GetChunkHashes()), len(goodBlockInCorrectPathIndex.GetChunkHashes()))
}

func PruneStoreTest(syncStore bool, t *testing.T) {
	blobStore, _ := longtailstorelib.NewMemBlobStore("the_path", syncStore)
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		true,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, err, nil)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)

	blocks := make([]longtaillib.Longtail_StoredBlock, 3)

	blocks[0], err = storeBlockFromSeed(storeAPI, 0)
	assert.Equal(t, err, nil)
	blocks[1], err = storeBlockFromSeed(storeAPI, 10)
	assert.Equal(t, err, nil)
	blocks[2], err = storeBlockFromSeed(storeAPI, 20)
	assert.Equal(t, err, nil)

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
	assert.Equal(t, err, nil)
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
	assert.Equal(t, err, nil)
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
		true,
		nil,
		runtime.NumCPU(),
		ReadWrite)
	assert.Equal(t, err, nil)
	storeAPI = longtaillib.CreateBlockStoreAPI(remoteStore)

	prunedStoreIndex, err := getExistingContent(storeAPI, chunkHashes, 0)
	assert.Equal(t, err, nil)
	assert.True(t, prunedStoreIndex.IsValid())
	defer prunedStoreIndex.Dispose()

	assert.Equal(t, len(prunedStoreIndex.GetBlockHashes()), 2)

	expectedChunkCount := len(chunkHashesPerBlock[0]) + len(chunkHashesPerBlock[2])
	assert.Equal(t, len(prunedStoreIndex.GetChunkHashes()), expectedChunkCount)

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

func validateThatBlocksArePresent(generatedBlocksIndex longtaillib.Longtail_StoreIndex, client longtailstorelib.BlobClient) bool {
	storeIndex, _, err := readStoreStoreIndexWithItemsLegacy(context.Background(), client)
	defer storeIndex.Dispose()
	if err != nil {
		log.Printf("readStoreStoreIndexWithItemsLegacy() failed with %s", err)
		return false
	}
	if !storeIndex.IsValid() {
		log.Printf("readStoreStoreIndexWithItemsLegacy() returned invalid store index")
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
			log.Printf("Missing direct block %d", h)
			return false
		}
	}
	return true
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
					block, _ := generateUniqueStoredBlock(uint8(seedBase + i))
					blocks = append(blocks, block.GetBlockIndex())
				}

				blocksIndex, err := longtaillib.CreateStoreIndexFromBlocks(blocks)
				assert.Equal(t, err, nil)
				newStoreIndex, err := addToRemoteStoreIndexLegacy(context.Background(), client, blocksIndex)
				blocksIndex.Dispose()
				assert.Equal(t, err, nil)
				newStoreIndex.Dispose()
			}

			readStoreIndex, _, err := readStoreStoreIndexWithItemsLegacy(context.Background(), client)
			assert.Equal(t, err, nil)
			readStoreIndex.Dispose()

			generatedBlocksIndex := longtaillib.Longtail_StoreIndex{}
			{
				for i := blockGenerateCount - 1; i < blockGenerateCount; i++ {
					block, _ := generateUniqueStoredBlock(uint8(seedBase + i))
					blocks = append(blocks, block.GetBlockIndex())
				}
				generatedBlocksIndex, err = longtaillib.CreateStoreIndexFromBlocks(blocks)
				assert.Equal(t, err, nil)
				newStoreIndex, err := addToRemoteStoreIndexLegacy(context.Background(), client, generatedBlocksIndex)
				assert.Equal(t, err, nil)
				newStoreIndex.Dispose()
			}

			for i := 0; i < 5; i++ {
				if validateThatBlocksArePresent(generatedBlocksIndex, client) {
					break
				}
				log.Printf("Could not find generated blocks in store index, retrying...\n")
				time.Sleep(1 * time.Second)
			}

			blockHashes := generatedBlocksIndex.GetBlockHashes()
			for n := 0; n < blockGenerateCount; n++ {
				h := blockHashes[n]
				generatedBlockHashes <- h
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
		newStoreIndex, _ := addToRemoteStoreIndexLegacy(context.Background(), client, updateIndex)
		updateIndex.Dispose()
		newStoreIndex.Dispose()
	}

	storeIndex, _, err := readStoreStoreIndexWithItemsLegacy(context.Background(), client)
	assert.Equal(t, err, nil)
	defer storeIndex.Dispose()
	assert.Equal(t, len(storeIndex.GetBlockHashes()), blockGenerateCount*workerCount)
	lookup := map[uint64]bool{}
	for _, h := range storeIndex.GetBlockHashes() {
		lookup[h] = true
	}
	assert.Equal(t, len(lookup), blockGenerateCount*workerCount)

	for n := 0; n < workerCount*blockGenerateCount; n++ {
		h := <-generatedBlockHashes
		_, exists := lookup[h]
		assert.True(t, exists)
	}
}

func TestStoreIndexSyncWithLocking(t *testing.T) {
	blobStore, err := longtailstorelib.NewMemBlobStore("locking_store", true)
	assert.Equal(t, err, nil)
	testStoreIndexSync(blobStore, t)
}

func TestStoreIndexSyncWithoutLocking(t *testing.T) {
	blobStore, err := longtailstorelib.NewMemBlobStore("locking_store", false)
	assert.Equal(t, err, nil)
	testStoreIndexSync(blobStore, t)
}

func TestGCSStoreIndexSyncWithLocking(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-gcs-blob-store-sync")
	assert.Equal(t, err, nil)

	blobStore, err := longtailstorelib.NewGCSBlobStore(u, false)
	assert.Equal(t, err, nil)
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
	assert.Equal(t, err, nil)

	blobStore, err := longtailstorelib.NewGCSBlobStore(u, true)
	assert.Equal(t, err, nil)
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
	assert.Equal(t, err, nil)

	blobStore, _ := longtailstorelib.NewS3BlobStore(u)
	testStoreIndexSync(blobStore, t)
}

func TestFSStoreIndexSyncWithLocking(t *testing.T) {
	storePath, err := ioutil.TempDir("", "longtail-test")
	assert.Equal(t, err, nil)
	blobStore, err := longtailstorelib.NewFSBlobStore(storePath, true)
	assert.Equal(t, err, nil)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	object, _ := client.NewObject("store.lsi")
	object.Delete()

	testStoreIndexSync(blobStore, t)
}

func TestFSStoreIndexSyncWithoutLocking(t *testing.T) {
	storePath, err := ioutil.TempDir("", "test")
	assert.Equal(t, err, nil)
	blobStore, err := longtailstorelib.NewFSBlobStore(storePath, false)
	assert.Equal(t, err, nil)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	object, _ := client.NewObject("store.lsi")
	object.Delete()

	testStoreIndexSync(blobStore, t)
}
