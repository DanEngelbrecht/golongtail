package longtailstorelib

import (
	"runtime"
	"sync"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
)

func TestCreateRemoteBlobStore(t *testing.T) {
	blobStore, _ := NewTestBlobStore("the_path")
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		8192,
		128)
	if err != nil {
		t.Errorf("TestCreateRemoveBlobStore() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)
	defer storeAPI.Dispose()
}

type getExistingContentCompletionAPI struct {
	wg            sync.WaitGroup
	content_index longtaillib.Longtail_ContentIndex
	err           int
}

func (a *getExistingContentCompletionAPI) OnComplete(content_index longtaillib.Longtail_ContentIndex, errno int) {
	a.content_index = content_index
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

func getExistingContent(t *testing.T, storeAPI longtaillib.Longtail_BlockStoreAPI, chunkHashes []uint64) (longtaillib.Longtail_ContentIndex, int) {
	g := &getExistingContentCompletionAPI{}
	g.wg.Add(1)
	errno := storeAPI.GetExistingContent(chunkHashes, longtaillib.CreateAsyncGetExistingContentAPI(g))
	if errno != 0 {
		g.wg.Done()
		return longtaillib.Longtail_ContentIndex{}, errno
	}
	g.wg.Wait()
	return g.content_index, g.err
}

func TestEmptyGetExistingContent(t *testing.T) {
	blobStore, _ := NewTestBlobStore("the_path")
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		8192,
		128)
	if err != nil {
		t.Errorf("TestCreateRemoveBlobStore() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)

	chunkHashes := []uint64{1, 2, 3, 4}

	existingContent, errno := getExistingContent(t, storeAPI, chunkHashes)
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

func storeBlockFromSeed(t *testing.T, storeAPI longtaillib.Longtail_BlockStoreAPI, seed uint8) (uint64, int) {
	chunkHashes := []uint64{uint64(seed) + 1, uint64(seed) + 2, uint64(seed) + 3}
	chunkSizes := []uint32{uint32(seed) + 10, uint32(seed) + 20, uint32(seed) + 30}

	blockDataLen := (int)(chunkSizes[0] + chunkSizes[1] + chunkSizes[2])
	blockData := make([]uint8, blockDataLen)
	for p := 0; p < blockDataLen; p++ {
		blockData[p] = seed
	}

	storedBlock, errno := longtaillib.CreateStoredBlock(
		uint64(seed)+21412151,
		997,
		2,
		chunkHashes,
		chunkSizes,
		blockData,
		false)
	if errno != 0 {
		return 0, errno
	}

	p := &putStoredBlockCompletionAPI{}
	p.wg.Add(1)
	errno = storeAPI.PutStoredBlock(storedBlock, longtaillib.CreateAsyncPutStoredBlockAPI(p))
	if errno != 0 {
		p.wg.Done()
		storedBlock.Dispose()
		return 0, errno
	}
	p.wg.Wait()

	return uint64(seed) + 21412151, p.err
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

func TestPutGetStoredBlock(t *testing.T) {
	blobStore, _ := NewTestBlobStore("the_path")
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		8192,
		128)
	if err != nil {
		t.Errorf("TestPutGetStoredBlock() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)

	blockHash, errno := storeBlockFromSeed(t, storeAPI, 0)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 0) %d != %d", errno, 0)
	}

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

func TestGetExistingContent(t *testing.T) {
	blobStore, _ := NewTestBlobStore("the_path")
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		8192,
		128)
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

	chunkHashes := []uint64{uint64(0) + 1, uint64(0) + 2, uint64(10) + 1, uint64(10) + 3}

	existingContent, errno := getExistingContent(t, storeAPI, chunkHashes)
	defer existingContent.Dispose()
	if !existingContent.IsValid() {
		t.Errorf("TestEmptyGetExistingContent() g.err %t != %t", existingContent.IsValid(), true)
	}

	if existingContent.GetBlockCount() != 2 {
		t.Errorf("TestEmptyGetExistingContent() existingContent.GetBlockCount() %d != %d", existingContent.GetBlockCount(), 2)
	}

	if existingContent.GetChunkCount() != 4 {
		t.Errorf("TestEmptyGetExistingContent() existingContent.GetChunkCount() %d != %d", existingContent.GetChunkCount(), 4)
	}
}

func TestRestoreStore(t *testing.T) {
	blobStore, _ := NewTestBlobStore("the_path")
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	remoteStore, err := NewRemoteBlockStore(
		jobs,
		blobStore,
		8192,
		128)
	if err != nil {
		t.Errorf("TestPutGetStoredBlock() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI := longtaillib.CreateBlockStoreAPI(remoteStore)

	_, errno := storeBlockFromSeed(t, storeAPI, 0)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 0) %d != %d", errno, 0)
	}
	_, errno = storeBlockFromSeed(t, storeAPI, 10)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 10) %d != %d", errno, 0)
	}
	_, errno = storeBlockFromSeed(t, storeAPI, 20)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 20) %d != %d", errno, 0)
	}

	storeAPI.Dispose()

	remoteStore, err = NewRemoteBlockStore(
		jobs,
		blobStore,
		8192,
		128)
	if err != nil {
		t.Errorf("TestPutGetStoredBlock() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI = longtaillib.CreateBlockStoreAPI(remoteStore)

	chunkHashes := []uint64{uint64(0) + 1, uint64(0) + 2, uint64(10) + 1, uint64(10) + 3}

	existingContent, errno := getExistingContent(t, storeAPI, chunkHashes)
	if !existingContent.IsValid() {
		t.Errorf("TestEmptyGetExistingContent() g.err %t != %t", existingContent.IsValid(), true)
	}

	if existingContent.GetBlockCount() != 2 {
		t.Errorf("TestEmptyGetExistingContent() existingContent.GetBlockCount() %d != %d", existingContent.GetBlockCount(), 2)
	}

	if existingContent.GetChunkCount() != 4 {
		t.Errorf("TestEmptyGetExistingContent() existingContent.GetChunkCount() %d != %d", existingContent.GetChunkCount(), 4)
	}

	chunkHashes = []uint64{uint64(0) + 1, uint64(0) + 2, uint64(10) + 1, uint64(10) + 3, uint64(30) + 1}

	existingContent, errno = getExistingContent(t, storeAPI, chunkHashes)
	if !existingContent.IsValid() {
		t.Errorf("TestEmptyGetExistingContent() g.err %t != %t", existingContent.IsValid(), true)
	}

	if existingContent.GetBlockCount() != 2 {
		t.Errorf("TestEmptyGetExistingContent() existingContent.GetBlockCount() %d != %d", existingContent.GetBlockCount(), 2)
	}

	if existingContent.GetChunkCount() != 4 {
		t.Errorf("TestEmptyGetExistingContent() existingContent.GetChunkCount() %d != %d", existingContent.GetChunkCount(), 4)
	}

	_, errno = storeBlockFromSeed(t, storeAPI, 30)
	if errno != 0 {
		t.Errorf("TestPutGetStoredBlock() storeBlock(t, storeAPI, 30) %d != %d", errno, 0)
	}
	existingContent.Dispose()
	storeAPI.Dispose()

	remoteStore, err = NewRemoteBlockStore(
		jobs,
		blobStore,
		8192,
		128)
	if err != nil {
		t.Errorf("TestPutGetStoredBlock() NewRemoteBlockStore()) %v != %v", err, nil)
	}
	storeAPI = longtaillib.CreateBlockStoreAPI(remoteStore)

	chunkHashes = []uint64{uint64(0) + 1, uint64(0) + 2, uint64(10) + 1, uint64(10) + 3, uint64(30) + 1}

	existingContent, errno = getExistingContent(t, storeAPI, chunkHashes)
	if !existingContent.IsValid() {
		t.Errorf("TestEmptyGetExistingContent() g.err %t != %t", existingContent.IsValid(), true)
	}

	if existingContent.GetBlockCount() != 3 {
		t.Errorf("TestEmptyGetExistingContent() existingContent.GetBlockCount() %d != %d", existingContent.GetBlockCount(), 3)
	}

	if existingContent.GetChunkCount() != 5 {
		t.Errorf("TestEmptyGetExistingContent() existingContent.GetChunkCount() %d != %d", existingContent.GetChunkCount(), 5)
	}
	existingContent.Dispose()
	storeAPI.Dispose()
}
