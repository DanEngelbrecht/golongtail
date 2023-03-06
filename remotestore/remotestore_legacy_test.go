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
	"github.com/stretchr/testify/assert"
)

func TestCreateRemoteBlobStoreLegacy(t *testing.T) {
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
	assert.Equal(t, nil, err)
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()
}

func TestEmptyGetExistingContentLegacy(t *testing.T) {
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

func TestPutGetStoredBlockLegacy(t *testing.T) {
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

func TestGetExistingContentLegacy(t *testing.T) {
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

func TestRestoreStoreLegacy(t *testing.T) {
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
		true,
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
		true,
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

func TestBlockScanningLegacy(t *testing.T) {
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

func PruneStoreTestLegacy(syncStore bool, t *testing.T) {
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
		true,
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
		true,
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

func validateThatBlocksArePresentLegacy(generatedBlocksIndex longtaillib.Longtail_StoreIndex, client longtailstorelib.BlobClient) bool {
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

func testStoreIndexSyncLegacy(blobStore longtailstorelib.BlobStore, t *testing.T) {

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
				assert.Equal(t, nil, err)
				newStoreIndex, err := addToRemoteStoreIndexLegacy(context.Background(), client, blocksIndex)
				blocksIndex.Dispose()
				assert.Equal(t, nil, err)
				newStoreIndex.Dispose()
			}

			readStoreIndex, _, err := readStoreStoreIndexWithItemsLegacy(context.Background(), client)
			assert.Equal(t, nil, err)
			readStoreIndex.Dispose()

			generatedBlocksIndex := longtaillib.Longtail_StoreIndex{}
			{
				for i := blockGenerateCount - 1; i < blockGenerateCount; i++ {
					block, _ := generateUniqueStoredBlock(uint8(seedBase + i))
					blocks = append(blocks, block.GetBlockIndex())
				}
				generatedBlocksIndex, err = longtaillib.CreateStoreIndexFromBlocks(blocks)
				assert.Equal(t, nil, err)
				newStoreIndex, err := addToRemoteStoreIndexLegacy(context.Background(), client, generatedBlocksIndex)
				assert.Equal(t, nil, err)
				newStoreIndex.Dispose()
			}

			for i := 0; i < 5; i++ {
				if validateThatBlocksArePresentLegacy(generatedBlocksIndex, client) {
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
	assert.Equal(t, nil, err)
	defer storeIndex.Dispose()
	assert.Equal(t, blockGenerateCount*workerCount, len(storeIndex.GetBlockHashes()))
	lookup := map[uint64]bool{}
	for _, h := range storeIndex.GetBlockHashes() {
		lookup[h] = true
	}
	assert.Equal(t, blockGenerateCount*workerCount, len(lookup))

	for n := 0; n < workerCount*blockGenerateCount; n++ {
		h := <-generatedBlockHashes
		_, exists := lookup[h]
		assert.True(t, exists)
	}
}

func TestStoreIndexSyncWithLockingLegacy(t *testing.T) {
	blobStore, err := longtailstorelib.NewMemBlobStore("locking_store", true)
	assert.Equal(t, nil, err)
	testStoreIndexSyncLegacy(blobStore, t)
}

func TestStoreIndexSyncWithoutLockingLegacy(t *testing.T) {
	blobStore, err := longtailstorelib.NewMemBlobStore("locking_store", false)
	assert.Equal(t, nil, err)
	testStoreIndexSyncLegacy(blobStore, t)
}

func TestGCSStoreIndexSyncWithLockingLegacy(t *testing.T) {
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

	testStoreIndexSyncLegacy(blobStore, t)
}

func TestGCSStoreIndexSyncWithoutLockingLegacy(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-gcs-blob-store-sync")
	assert.Equal(t, nil, err)

	blobStore, err := longtailstorelib.NewGCSBlobStore(u, true)
	assert.Equal(t, nil, err)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	object, _ := client.NewObject("store.lsi")
	object.Delete()

	testStoreIndexSyncLegacy(blobStore, t)
}

func TestS3StoreIndexSyncLegacy(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	t.Skip()

	u, err := url.Parse("s3://longtail-test/test-s3-blob-store-sync")
	assert.Equal(t, nil, err)

	blobStore, _ := longtailstorelib.NewS3BlobStore(u)
	testStoreIndexSyncLegacy(blobStore, t)
}

func TestFSStoreIndexSyncWithLockingLegacy(t *testing.T) {
	storePath, err := ioutil.TempDir("", "longtail-test")
	assert.Equal(t, nil, err)
	blobStore, err := longtailstorelib.NewFSBlobStore(storePath, true)
	assert.Equal(t, nil, err)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	object, _ := client.NewObject("store.lsi")
	object.Delete()

	testStoreIndexSyncLegacy(blobStore, t)
}

func TestFSStoreIndexSyncWithoutLockingLegacy(t *testing.T) {
	storePath, err := ioutil.TempDir("", "test")
	assert.Equal(t, nil, err)
	blobStore, err := longtailstorelib.NewFSBlobStore(storePath, false)
	assert.Equal(t, nil, err)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	object, _ := client.NewObject("store.lsi")
	object.Delete()

	testStoreIndexSyncLegacy(blobStore, t)
}
