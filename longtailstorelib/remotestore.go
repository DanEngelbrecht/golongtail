package longtailstorelib

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/pkg/errors"
)

type putBlockMessage struct {
	storedBlock      longtaillib.Longtail_StoredBlock
	asyncCompleteAPI longtaillib.Longtail_AsyncPutStoredBlockAPI
}

type getBlockMessage struct {
	blockHash        uint64
	asyncCompleteAPI longtaillib.Longtail_AsyncGetStoredBlockAPI
}

type prefetchBlockMessage struct {
	blockHash uint64
}

type preflightGetMessage struct {
	chunkHashes []uint64
}

type blockIndexMessage struct {
	blockIndex longtaillib.Longtail_BlockIndex
}

type getExistingContentMessage struct {
	chunkHashes          []uint64
	minBlockUsagePercent uint32
	asyncCompleteAPI     longtaillib.Longtail_AsyncGetExistingContentAPI
}

type stopMessage struct {
}

type pendingPrefetchedBlock struct {
	storedBlock       longtaillib.Longtail_StoredBlock
	completeCallbacks []longtaillib.Longtail_AsyncGetStoredBlockAPI
}

type remoteStore struct {
	jobAPI            longtaillib.Longtail_JobAPI
	maxBlockSize      uint32
	maxChunksPerBlock uint32
	blobStore         BlobStore
	defaultClient     BlobClient

	workerCount int

	putBlockChan           chan putBlockMessage
	getBlockChan           chan getBlockMessage
	preflightGetChan       chan preflightGetMessage
	prefetchBlockChan      chan prefetchBlockMessage
	blockIndexChan         chan blockIndexMessage
	getExistingContentChan chan getExistingContentMessage
	workerFlushChan        chan int
	workerFlushReplyChan   chan int
	workerStopChan         chan stopMessage
	indexFlushChan         chan int
	indexFlushReplyChan    chan int
	indexStopChan          chan stopMessage
	workerErrorChan        chan error
	prefetchMemory         int64
	maxPrefetchMemory      int64

	fetchedBlocksSync sync.Mutex
	fetchedBlocks     map[uint64]bool
	prefetchBlocks    map[uint64]*pendingPrefetchedBlock

	stats longtaillib.BlockStoreStats
}

// String() ...
func (s *remoteStore) String() string {
	return s.defaultClient.String()
}

func readBlobWithRetry(
	ctx context.Context,
	s *remoteStore,
	client BlobClient,
	key string) ([]byte, int, error) {
	retryCount := 0
	objHandle, err := client.NewObject(key)
	if err != nil {
		return nil, retryCount, err
	}
	exists, err := objHandle.Exists()
	if err != nil {
		return nil, retryCount, err
	}
	if !exists {
		return nil, retryCount, longtaillib.ErrENOENT
	}
	blobData, err := objHandle.Read()
	if err != nil {
		log.Printf("Retrying getBlob %s in store %s\n", key, s.String())
		retryCount++
		blobData, err = objHandle.Read()
	}
	if err != nil {
		log.Printf("Retrying 500 ms delayed getBlob %s in store %s\n", key, s.String())
		time.Sleep(500 * time.Millisecond)
		retryCount++
		blobData, err = objHandle.Read()
	}
	if err != nil {
		log.Printf("Retrying 2 s delayed getBlob %s in store %s\n", key, s.String())
		time.Sleep(2 * time.Second)
		retryCount++
		blobData, err = objHandle.Read()
	}

	if err != nil {
		return nil, retryCount, err
	}

	return blobData, retryCount, nil
}

func putStoredBlock(
	ctx context.Context,
	s *remoteStore,
	blobClient BlobClient,
	blockIndexMessages chan<- blockIndexMessage,
	storedBlock longtaillib.Longtail_StoredBlock) error {

	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count], 1)

	blockIndex := storedBlock.GetBlockIndex()
	blockHash := blockIndex.GetBlockHash()
	key := getBlockPath("chunks", blockHash)
	objHandle, err := blobClient.NewObject(key)
	if err != nil {
		return err
	}
	if exists, err := objHandle.Exists(); err == nil && !exists {
		blob, errno := longtaillib.WriteStoredBlockToBuffer(storedBlock)
		if errno != 0 {
			return longtaillib.ErrnoToError(errno, longtaillib.ErrEIO)
		}

		ok, err := objHandle.Write(blob)
		if err != nil || !ok {
			log.Printf("Retrying putBlob %s in store %s\n", key, s.String())
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount], 1)
			ok, err = objHandle.Write(blob)
		}
		if err != nil || !ok {
			log.Printf("Retrying 500 ms delayed putBlob %s in store %s\n", key, s.String())
			time.Sleep(500 * time.Millisecond)
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount], 1)
			ok, err = objHandle.Write(blob)
		}
		if err != nil || !ok {
			log.Printf("Retrying 2 s delayed putBlob %s in store %s\n", key, s.String())
			time.Sleep(2 * time.Second)
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount], 1)
			ok, err = objHandle.Write(blob)
		}

		if err != nil || !ok {
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1)
			return longtaillib.ErrnoToError(errno, longtaillib.ErrEIO)
		}

		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count], (uint64)(len(blob)))
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count], (uint64)(blockIndex.GetChunkCount()))
	}

	// We need to make a copy of the block index - as soon as we call the OnComplete function the data for the block might be deallocated
	storedBlockIndex, errno := longtaillib.WriteBlockIndexToBuffer(blockIndex)
	if errno != 0 {
		return longtaillib.ErrnoToError(errno, longtaillib.ErrEIO)
	}
	blockIndexCopy, errno := longtaillib.ReadBlockIndexFromBuffer(storedBlockIndex)
	if errno != 0 {
		return longtaillib.ErrnoToError(errno, longtaillib.ErrEIO)
	}
	blockIndexMessages <- blockIndexMessage{blockIndex: blockIndexCopy}
	return nil
}

func getStoredBlock(
	ctx context.Context,
	s *remoteStore,
	blobClient BlobClient,
	blockHash uint64) (longtaillib.Longtail_StoredBlock, error) {

	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count], 1)

	key := getBlockPath("chunks", blockHash)

	storedBlockData, retryCount, err := readBlobWithRetry(ctx, s, blobClient, key)
	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_RetryCount], uint64(retryCount))

	if err != nil || storedBlockData == nil {
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1)
		return longtaillib.Longtail_StoredBlock{}, err
	}

	storedBlock, errno := longtaillib.ReadStoredBlockFromBuffer(storedBlockData)
	if errno != 0 {
		return longtaillib.Longtail_StoredBlock{}, longtaillib.ErrnoToError(errno, longtaillib.ErrEIO)
	}

	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count], (uint64)(len(storedBlockData)))
	blockIndex := storedBlock.GetBlockIndex()
	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count], (uint64)(blockIndex.GetChunkCount()))
	return storedBlock, nil
}

func fetchBlock(
	ctx context.Context,
	s *remoteStore,
	client BlobClient,
	getMsg getBlockMessage) {
	s.fetchedBlocksSync.Lock()
	prefetchedBlock, exists := s.prefetchBlocks[getMsg.blockHash]
	if exists {
		storedBlock := prefetchedBlock.storedBlock
		if storedBlock.IsValid() {
			delete(s.prefetchBlocks, getMsg.blockHash)
			blockSize := -int64(storedBlock.GetBlockSize())
			atomic.AddInt64(&s.prefetchMemory, blockSize)
			s.fetchedBlocksSync.Unlock()
			getMsg.asyncCompleteAPI.OnComplete(storedBlock, 0)
			return
		}
		prefetchedBlock.completeCallbacks = append(prefetchedBlock.completeCallbacks, getMsg.asyncCompleteAPI)
		s.fetchedBlocksSync.Unlock()
		return
	}
	prefetchedBlock = &pendingPrefetchedBlock{storedBlock: longtaillib.Longtail_StoredBlock{}}
	s.prefetchBlocks[getMsg.blockHash] = prefetchedBlock
	s.fetchedBlocks[getMsg.blockHash] = true
	s.fetchedBlocksSync.Unlock()
	storedBlock, getStoredBlockErr := getStoredBlock(ctx, s, client, getMsg.blockHash)
	s.fetchedBlocksSync.Lock()
	completeCallbacks := prefetchedBlock.completeCallbacks
	delete(s.prefetchBlocks, getMsg.blockHash)
	s.fetchedBlocksSync.Unlock()
	for _, c := range completeCallbacks {
		if getStoredBlockErr != nil {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, longtaillib.ErrorToErrno(getStoredBlockErr, longtaillib.EIO))
			continue
		}
		buf, errno := longtaillib.WriteStoredBlockToBuffer(storedBlock)
		if errno != 0 {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, errno)
			continue
		}
		blockCopy, errno := longtaillib.ReadStoredBlockFromBuffer(buf)
		if errno != 0 {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, errno)
			continue
		}
		c.OnComplete(blockCopy, 0)
	}
	getMsg.asyncCompleteAPI.OnComplete(storedBlock, longtaillib.ErrorToErrno(getStoredBlockErr, longtaillib.EIO))
}

func prefetchBlock(
	ctx context.Context,
	s *remoteStore,
	client BlobClient,
	prefetchMsg prefetchBlockMessage) {
	s.fetchedBlocksSync.Lock()
	_, exists := s.fetchedBlocks[prefetchMsg.blockHash]
	if exists {
		// Already fetched
		s.fetchedBlocksSync.Unlock()
		return
	}
	s.fetchedBlocks[prefetchMsg.blockHash] = true
	prefetchedBlock := &pendingPrefetchedBlock{storedBlock: longtaillib.Longtail_StoredBlock{}}
	s.prefetchBlocks[prefetchMsg.blockHash] = prefetchedBlock
	s.fetchedBlocksSync.Unlock()

	storedBlock, getErr := getStoredBlock(ctx, s, client, prefetchMsg.blockHash)
	if getErr != nil {
		return
	}

	s.fetchedBlocksSync.Lock()
	prefetchedBlock = s.prefetchBlocks[prefetchMsg.blockHash]
	completeCallbacks := prefetchedBlock.completeCallbacks
	if len(completeCallbacks) == 0 {
		// Nobody is actively waiting for the block
		blockSize := int64(storedBlock.GetBlockSize())
		prefetchedBlock.storedBlock = storedBlock
		atomic.AddInt64(&s.prefetchMemory, blockSize)
		s.fetchedBlocksSync.Unlock()
		return
	}
	delete(s.prefetchBlocks, prefetchMsg.blockHash)
	s.fetchedBlocksSync.Unlock()
	for i := 1; i < len(completeCallbacks)-1; i++ {
		c := completeCallbacks[i]
		if getErr != nil {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, longtaillib.ErrorToErrno(getErr, longtaillib.EIO))
			continue
		}
		buf, errno := longtaillib.WriteStoredBlockToBuffer(storedBlock)
		if errno != 0 {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, errno)
			continue
		}
		blockCopy, errno := longtaillib.ReadStoredBlockFromBuffer(buf)
		if errno != 0 {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, errno)
			continue
		}
		c.OnComplete(blockCopy, 0)
	}
	completeCallbacks[0].OnComplete(storedBlock, longtaillib.ErrorToErrno(getErr, longtaillib.EIO))
}

func remoteWorker(
	ctx context.Context,
	s *remoteStore,
	putBlockMessages <-chan putBlockMessage,
	getBlockMessages <-chan getBlockMessage,
	prefetchBlockChan <-chan prefetchBlockMessage,
	blockIndexMessages chan<- blockIndexMessage,
	flushMessages <-chan int,
	flushReplyMessages chan<- int,
	stopMessages <-chan stopMessage) error {
	client, err := s.blobStore.NewClient(ctx)
	if err != nil {
		return errors.Wrap(err, s.blobStore.String())
	}
	defer client.Close()
	run := true
	for run {
		received := 0
		select {
		case putMsg := <-putBlockMessages:
			received += 1
			err := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
			putMsg.asyncCompleteAPI.OnComplete(longtaillib.ErrorToErrno(err, longtaillib.EIO))
		case getMsg := <-getBlockMessages:
			received += 1
			fetchBlock(ctx, s, client, getMsg)
		default:
		}
		if received == 0 {
			if s.prefetchMemory < s.maxPrefetchMemory {
				select {
				case _ = <-flushMessages:
					flushReplyMessages <- 0
				case putMsg := <-putBlockMessages:
					err := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
					putMsg.asyncCompleteAPI.OnComplete(longtaillib.ErrorToErrno(err, longtaillib.EIO))
				case getMsg := <-getBlockMessages:
					fetchBlock(ctx, s, client, getMsg)
				case prefetchMsg := <-prefetchBlockChan:
					prefetchBlock(ctx, s, client, prefetchMsg)
				case _ = <-stopMessages:
					run = false
				}
			} else {
				select {
				case _ = <-flushMessages:
					flushReplyMessages <- 0
				case putMsg := <-putBlockMessages:
					err := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
					putMsg.asyncCompleteAPI.OnComplete(longtaillib.ErrorToErrno(err, longtaillib.EIO))
				case getMsg := <-getBlockMessages:
					fetchBlock(ctx, s, client, getMsg)
				case _ = <-stopMessages:
					run = false
				}
			}
		}
	}

	for len(putBlockMessages) > 0 {
		select {
		case putMsg := <-putBlockMessages:
			err := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
			putMsg.asyncCompleteAPI.OnComplete(longtaillib.ErrorToErrno(err, longtaillib.EIO))
		default:
		}
	}

	return nil
}

func updateRemoteStoreIndex(
	ctx context.Context,
	blobClient BlobClient,
	//	prefix string,
	jobAPI longtaillib.Longtail_JobAPI,
	updatedStoreIndex longtaillib.Longtail_StoreIndex) error {

	key := "store.lsi"
	objHandle, err := blobClient.NewObject(key)
	if err != nil {
		return errors.Wrapf(err, "updateRemoteStoreIndex: blobClient.NewObject(%s) failed", key)
	}
	for {
		exists, err := objHandle.LockWriteVersion()
		if err != nil {
			return errors.Wrap(err, key)
		}
		if exists {
			blob, err := objHandle.Read()
			if err != nil {
				return errors.Wrapf(err, "updateRemoteStoreIndex: objHandle.Read() key %s failed", key)
			}

			remoteStoreIndex, errno := longtaillib.ReadStoreIndexFromBuffer(blob)
			if errno != 0 {
				return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "updateRemoteStoreIndex: longtaillib.ReadStoreIndexFromBuffer() key %s failed", key)
			}
			defer remoteStoreIndex.Dispose()

			newStoreIndex, errno := longtaillib.MergeStoreIndex(updatedStoreIndex, remoteStoreIndex)
			if errno != 0 {
				return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "updateRemoteStoreIndex: longtaillib.MergeStoreIndex() key %s failed", key)
			}
			defer newStoreIndex.Dispose()

			storeBlob, errno := longtaillib.WriteStoreIndexToBuffer(newStoreIndex)
			if errno != 0 {
				return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "updateRemoteStoreIndex: longtaillib.WriteStoreIndexToBuffer() key %s failed", key)
			}

			ok, err := objHandle.Write(storeBlob)
			if err != nil {
				return errors.Wrapf(err, "updateRemoteStoreIndex: objHandle.Write() key %s failed", key)
			}
			if ok {
				break
			}
		} else {
			storeBlob, errno := longtaillib.WriteStoreIndexToBuffer(updatedStoreIndex)
			if errno != 0 {
				return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "updateRemoteStoreIndex: WriteStoreIndexToBuffer() %s failed", key)
			}

			ok, err := objHandle.Write(storeBlob)
			if err != nil {
				return errors.Wrapf(err, "updateRemoteStoreIndex: objHandle.Write() %s failed", key)
			}
			if ok {
				break
			}
		}
	}
	return nil
}

func updateRemoteContentIndex(
	ctx context.Context,
	blobClient BlobClient,
	//	prefix string,
	jobAPI longtaillib.Longtail_JobAPI,
	updatedContentIndex longtaillib.Longtail_ContentIndex) error {

	key := "store.lci"
	objHandle, err := blobClient.NewObject(key)
	if err != nil {
		return errors.Wrapf(err, "updateRemoteContentIndex: blobClient.NewObject(%s) failed", key)
	}
	for {
		exists, err := objHandle.LockWriteVersion()
		if err != nil {
			return errors.Wrap(err, key)
		}
		if exists {
			blob, err := objHandle.Read()
			if err != nil {
				return errors.Wrapf(err, "updateRemoteContentIndex: objHandle.Read() key %s failed", key)
			}

			remoteContentIndex, errno := longtaillib.ReadContentIndexFromBuffer(blob)
			if errno != 0 {
				return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "updateRemoteContentIndex: longtaillib.ReadContentIndexFromBuffer() key %s failed", key)
			}
			defer remoteContentIndex.Dispose()

			newContentIndex, errno := longtaillib.MergeContentIndex(jobAPI, remoteContentIndex, updatedContentIndex)
			if errno != 0 {
				return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "updateRemoteContentIndex: longtaillib.MergeContentIndex() key %s failed", key)
			}
			defer newContentIndex.Dispose()

			storeBlob, errno := longtaillib.WriteContentIndexToBuffer(newContentIndex)
			if errno != 0 {
				return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "updateRemoteContentIndex: longtaillib.WriteContentIndexToBuffer() key %s failed", key)
			}

			ok, err := objHandle.Write(storeBlob)
			if err != nil {
				return errors.Wrapf(err, "updateRemoteContentIndex: objHandle.Write() key %s failed", key)
			}
			if ok {
				break
			}
		} else {
			storeBlob, errno := longtaillib.WriteContentIndexToBuffer(updatedContentIndex)
			if errno != 0 {
				return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "updateRemoteContentIndex: WriteContentIndexToBuffer() %s failed", key)
			}

			ok, err := objHandle.Write(storeBlob)
			if err != nil {
				return errors.Wrapf(err, "updateRemoteContentIndex: objHandle.Write() %s failed", key)
			}
			if ok {
				break
			}
		}
	}
	return nil
}

func getStoreIndexFromBlocks(
	ctx context.Context,
	s *remoteStore,
	blobClient BlobClient,
	blockKeys []string) (longtaillib.Longtail_StoreIndex, error) {

	storeIndex, errno := longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
	if errno != 0 {
		return longtaillib.Longtail_StoreIndex{}, longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
	}

	batchCount := runtime.NumCPU()
	batchStart := 0

	if batchCount > len(blockKeys) {
		batchCount = len(blockKeys)
	}
	clients := make([]BlobClient, batchCount)
	for c := 0; c < batchCount; c++ {
		client, err := s.blobStore.NewClient(ctx)
		if err != nil {
			storeIndex.Dispose()
			return longtaillib.Longtail_StoreIndex{}, err
		}
		clients[c] = client
	}

	var wg sync.WaitGroup

	for batchStart < len(blockKeys) {
		batchLength := batchCount
		if batchStart+batchLength > len(blockKeys) {
			batchLength = len(blockKeys) - batchStart
		}
		batchBlockIndexes := make([]longtaillib.Longtail_BlockIndex, batchLength)
		wg.Add(batchLength)
		for batchPos := 0; batchPos < batchLength; batchPos++ {
			i := batchStart + batchPos
			blockKey := blockKeys[i]
			go func(client BlobClient, batchPos int, blockKey string) {
				storedBlockData, _, err := readBlobWithRetry(
					ctx,
					s,
					client,
					blockKey)

				if err != nil {
					wg.Done()
					return
				}

				blockIndex, errno := longtaillib.ReadBlockIndexFromBuffer(storedBlockData)
				if errno != 0 {
					wg.Done()
					return
				}

				blockPath := getBlockPath("chunks", blockIndex.GetBlockHash())
				if blockPath == blockKey {
					batchBlockIndexes[batchPos] = blockIndex
				} else {
					log.Printf("Block %s name does not match content hash, expected name %s\n", blockKey, blockPath)
				}

				wg.Done()
			}(clients[batchPos], batchPos, blockKey)
		}
		wg.Wait()
		writeIndex := 0
		for i, blockIndex := range batchBlockIndexes {
			if !blockIndex.IsValid() {
				continue
			}
			if i > writeIndex {
				batchBlockIndexes[writeIndex] = blockIndex
			}
			writeIndex++
		}
		batchBlockIndexes = batchBlockIndexes[:writeIndex]
		batchStoreIndex, errno := longtaillib.CreateStoreIndexFromBlocks(batchBlockIndexes)
		for _, blockIndex := range batchBlockIndexes {
			blockIndex.Dispose()
		}
		if errno != 0 {
			storeIndex.Dispose()
			return longtaillib.Longtail_StoreIndex{}, longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
		}
		newStoreIndex, errno := longtaillib.MergeStoreIndex(storeIndex, batchStoreIndex)
		batchStoreIndex.Dispose()
		storeIndex.Dispose()
		storeIndex = newStoreIndex
		//		blockIndexes = append(blockIndexes, batchBlockIndexes[:writeIndex]...)
		batchStart += batchLength
		log.Printf("Scanned %d/%d blocks in %s\n", batchStart, len(blockKeys), blobClient.String())
	}

	for c := 0; c < batchCount; c++ {
		clients[c].Close()
	}

	return storeIndex, nil
}

func buildStoreIndexFromStoreBlocks(
	ctx context.Context,
	s *remoteStore,
	blobClient BlobClient) (longtaillib.Longtail_StoreIndex, error) {

	var items []string
	blobs, err := blobClient.GetObjects()
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, err
	}

	for _, blob := range blobs {
		if blob.Size == 0 {
			continue
		}
		if strings.HasSuffix(blob.Name, ".lsb") {
			items = append(items, blob.Name)
		}
	}

	return getStoreIndexFromBlocks(ctx, s, blobClient, items)
}

func storeIndexWorkerReplyErrorState(
	blockIndexMessages <-chan blockIndexMessage,
	getExistingContentMessages <-chan getExistingContentMessage,
	flushMessages <-chan int,
	flushReplyMessages chan<- int,
	stopMessages <-chan stopMessage) {
	run := true
	for run {
		select {
		case _ = <-flushMessages:
			flushReplyMessages <- 0
		case _ = <-blockIndexMessages:
		case getExistingContentMessage := <-getExistingContentMessages:
			getExistingContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, longtaillib.EINVAL)
		case _ = <-stopMessages:
			run = false
		}
	}
}

func readStoreContentIndex(
	ctx context.Context,
	s *remoteStore,
	client BlobClient) (longtaillib.Longtail_ContentIndex, error) {

	key := "store.lci"
	blobData, _, err := readBlobWithRetry(ctx, s, client, key)
	if err != nil {
		return longtaillib.Longtail_ContentIndex{}, err
	}
	if blobData == nil {
		return longtaillib.Longtail_ContentIndex{}, nil
	}
	contentIndex, errno := longtaillib.ReadContentIndexFromBuffer(blobData)
	if errno != 0 {
		return longtaillib.Longtail_ContentIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "contentIndexWorker: longtaillib.ReadContentIndexFromBuffer() %s failed", key)
	}
	return contentIndex, nil
}

func readStoreStoreIndex(
	ctx context.Context,
	s *remoteStore,
	client BlobClient) (longtaillib.Longtail_StoreIndex, error) {

	key := "store.lsi"
	blobData, _, err := readBlobWithRetry(ctx, s, client, key)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, err
	}
	if blobData == nil {
		return longtaillib.Longtail_StoreIndex{}, nil
	}
	storeIndex, errno := longtaillib.ReadStoreIndexFromBuffer(blobData)
	if errno != 0 {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "contentIndexWorker: longtaillib.ReadStoreIndexFromBuffer() for %s", key)
	}
	return storeIndex, nil
}

func onPreflighMessage(
	s *remoteStore,
	storeIndex longtaillib.Longtail_StoreIndex,
	message preflightGetMessage,
	prefetchBlockMessages chan<- prefetchBlockMessage) {
	existingContentIndex, errno := longtaillib.GetExistingContentIndex(storeIndex, message.chunkHashes, 0, s.maxBlockSize, s.maxChunksPerBlock)
	if errno != 0 {
		log.Printf("WARNING: onPreflighMessage longtaillib.GetExistingContentIndex() failed with %v", longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM))
		return
	}
	blockHashes := existingContentIndex.GetBlockHashes()
	for _, blockHash := range blockHashes {
		prefetchBlockMessages <- prefetchBlockMessage{blockHash: blockHash}
	}
}

func onGetExistingContentMessage(
	s *remoteStore,
	storeIndex longtaillib.Longtail_StoreIndex,
	message getExistingContentMessage) {
	existingContentIndex, errno := longtaillib.GetExistingContentIndex(storeIndex, message.chunkHashes, message.minBlockUsagePercent, s.maxBlockSize, s.maxChunksPerBlock)
	if errno != 0 {
		message.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
		return
	}
	message.asyncCompleteAPI.OnComplete(existingContentIndex, 0)
}

func updateStoreIndex(
	storeIndex longtaillib.Longtail_StoreIndex,
	addedBlockIndexes []longtaillib.Longtail_BlockIndex) (longtaillib.Longtail_StoreIndex, error) {
	addedStoreIndex, errno := longtaillib.CreateStoreIndexFromBlocks(addedBlockIndexes)
	if errno != 0 {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "contentIndexWorker: longtaillib.CreateStoreIndexFromBlocks() failed")
	}

	updatedStoreIndex, errno := longtaillib.MergeStoreIndex(addedStoreIndex, storeIndex)
	addedStoreIndex.Dispose()
	if errno != 0 {
		updatedStoreIndex.Dispose()
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "contentIndexWorker: longtaillib.MergeStoreIndex() failed")
	}
	return updatedStoreIndex, nil
}

func contentIndexWorker(
	ctx context.Context,
	s *remoteStore,
	preflightGetMessages <-chan preflightGetMessage,
	prefetchBlockMessages chan<- prefetchBlockMessage,
	blockIndexMessages <-chan blockIndexMessage,
	getExistingContentMessages <-chan getExistingContentMessage,
	flushMessages <-chan int,
	flushReplyMessages chan<- int,
	stopMessages <-chan stopMessage,
	rebuildIndex bool) error {

	client, err := s.blobStore.NewClient(ctx)
	if err != nil {
		storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, flushMessages, flushReplyMessages, stopMessages)
		return errors.Wrap(err, s.blobStore.String())
	}
	defer client.Close()

	var errno int

	saveStoreIndex := false
	saveStoreContentIndex := false

	var storeIndex longtaillib.Longtail_StoreIndex
	if rebuildIndex {
		saveStoreIndex = true
		saveStoreContentIndex = true
	} else {
		storeIndex, err = readStoreStoreIndex(ctx, s, client)
		if err != nil {
			log.Printf("contentIndexWorker: readStoreStoreIndex() failed with %v", err)
		}
	}
	defer storeIndex.Dispose()

	if !storeIndex.IsValid() {
		storeIndex, errno = longtaillib.CreateStoreIndexFromBlocks(
			[]longtaillib.Longtail_BlockIndex{})
		if errno != 0 {
			storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, flushMessages, flushReplyMessages, stopMessages)
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "contentIndexWorker: longtaillib.CreateStoreIndexFromBlocks() failed")
		}

		storeIndex, err = buildStoreIndexFromStoreBlocks(
			ctx,
			s,
			client)

		if err != nil {
			storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, flushMessages, flushReplyMessages, stopMessages)
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "contentIndexWorker: buildStoreIndexFromStoreBlocks() failed")
		}
		log.Printf("Rebuilt remote index with %d blocks\n", len(storeIndex.GetBlockHashes()))
		err := updateRemoteStoreIndex(ctx, client, s.jobAPI, storeIndex)
		if err != nil {
			log.Printf("Failed to update store index in store %s\n", s.String())
			saveStoreIndex = true
		}
	}

	storeContentIndex, err := readStoreContentIndex(ctx, s, client)
	defer storeContentIndex.Dispose()
	if storeContentIndex.IsValid() {
		missingBlockPaths := []string{}
		storeIndexBlocks := make(map[uint64]bool)
		for _, b := range storeIndex.GetBlockHashes() {
			storeIndexBlocks[b] = true
		}
		contentIndexBlocks := make(map[uint64]bool)
		for _, b := range storeContentIndex.GetBlockHashes() {
			if _, exists := storeIndexBlocks[b]; !exists {
				missingBlockPaths = append(missingBlockPaths, getBlockPath("chunks", b))
			}
			contentIndexBlocks[b] = true
		}
		for _, b := range storeIndex.GetBlockHashes() {
			if _, exists := contentIndexBlocks[b]; !exists {
				saveStoreContentIndex = true
				break
			}
		}

		if len(missingBlockPaths) > 0 {
			log.Printf("Updating store index from legacy store content index %s\n", s.String())
			missingStoreIndex, err := getStoreIndexFromBlocks(ctx, s, client, missingBlockPaths)
			defer missingStoreIndex.Dispose()
			if err == nil && len(missingStoreIndex.GetBlockHashes()) > 0 {
				updatedStoreIndex, errno := longtaillib.MergeStoreIndex(missingStoreIndex, storeIndex)
				if errno == 0 {
					storeIndex.Dispose()
					storeIndex = updatedStoreIndex
					saveStoreIndex = true
				}
			}
		}
	}

	var addedBlockIndexes []longtaillib.Longtail_BlockIndex
	defer func(addedBlockIndexes []longtaillib.Longtail_BlockIndex) {
		for _, blockIndex := range addedBlockIndexes {
			blockIndex.Dispose()
		}
	}(addedBlockIndexes)

	run := true
	for run {
		received := 0
		select {
		case preflightGetMsg := <-preflightGetMessages:
			received += 1
			if len(addedBlockIndexes) > 0 {
				updatedStoreIndex, err := updateStoreIndex(storeIndex, addedBlockIndexes)
				if err != nil {
					log.Printf("WARNING: Failed to update store index with added blocks %v", err)
					continue
				}
				storeIndex.Dispose()
				storeIndex = updatedStoreIndex
				saveStoreIndex = true
				saveStoreContentIndex = storeContentIndex.IsValid()
				addedBlockIndexes = nil
			}
			onPreflighMessage(s, storeIndex, preflightGetMsg, prefetchBlockMessages)
		case blockIndexMsg := <-blockIndexMessages:
			received += 1
			addedBlockIndexes = append(addedBlockIndexes, blockIndexMsg.blockIndex)
		case getExistingContentMessage := <-getExistingContentMessages:
			received += 1
			if len(addedBlockIndexes) > 0 {
				updatedStoreIndex, err := updateStoreIndex(storeIndex, addedBlockIndexes)
				if err != nil {
					log.Printf("WARNING: Failed to update store index with added blocks %v", err)
					getExistingContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, longtaillib.ErrorToErrno(err, longtaillib.EIO))
					continue
				}
				storeIndex.Dispose()
				storeIndex = updatedStoreIndex
				saveStoreIndex = true
				saveStoreContentIndex = storeContentIndex.IsValid()
				addedBlockIndexes = nil
			}
			onGetExistingContentMessage(s, storeIndex, getExistingContentMessage)
		default:
		}

		if received == 0 {
			select {
			case _ = <-flushMessages:
				flushReplyMessages <- 0
			case preflightGetMsg := <-preflightGetMessages:
				if len(addedBlockIndexes) > 0 {
					updatedStoreIndex, err := updateStoreIndex(storeIndex, addedBlockIndexes)
					if err != nil {
						log.Printf("WARNING: Failed to update store index with added blocks %v", err)
						continue
					}
					storeIndex.Dispose()
					storeIndex = updatedStoreIndex
					saveStoreIndex = true
					saveStoreContentIndex = storeContentIndex.IsValid()
					addedBlockIndexes = nil
				}
				onPreflighMessage(s, storeIndex, preflightGetMsg, prefetchBlockMessages)
			case blockIndexMsg := <-blockIndexMessages:
				addedBlockIndexes = append(addedBlockIndexes, blockIndexMsg.blockIndex)
			case getExistingContentMessage := <-getExistingContentMessages:
				if len(addedBlockIndexes) > 0 {
					updatedStoreIndex, err := updateStoreIndex(storeIndex, addedBlockIndexes)
					if err != nil {
						log.Printf("WARNING: Failed to update store index with added blocks %v", err)
						getExistingContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, longtaillib.ErrorToErrno(err, longtaillib.EIO))
						continue
					}
					storeIndex.Dispose()
					storeIndex = updatedStoreIndex
					saveStoreIndex = true
					saveStoreContentIndex = storeContentIndex.IsValid()
					addedBlockIndexes = nil
				}
				onGetExistingContentMessage(s, storeIndex, getExistingContentMessage)
			case _ = <-stopMessages:
				run = false
			}
		}
	}

	for len(blockIndexMessages) > 0 {
		select {
		case blockIndexMsg := <-blockIndexMessages:
			addedBlockIndexes = append(addedBlockIndexes, blockIndexMsg.blockIndex)
		default:
		}
	}

	if len(addedBlockIndexes) > 0 {
		updatedStoreIndex, err := updateStoreIndex(storeIndex, addedBlockIndexes)
		if err != nil {
			return errors.Wrapf(err, "WARNING: Failed to update store index with added blocks")
		}
		storeIndex.Dispose()
		storeIndex = updatedStoreIndex
		saveStoreIndex = true
		saveStoreContentIndex = storeContentIndex.IsValid()
		addedBlockIndexes = nil
	}

	if saveStoreIndex {
		err := updateRemoteStoreIndex(ctx, client, s.jobAPI, storeIndex)
		if err != nil {
			log.Printf("Retrying store index in store %s\n", s.String())
			err = updateRemoteStoreIndex(ctx, client, s.jobAPI, storeIndex)
		}
		if err != nil {
			log.Printf("Retrying 500 ms delayed store index in store %s\n", s.String())
			time.Sleep(500 * time.Millisecond)
			err = updateRemoteStoreIndex(ctx, client, s.jobAPI, storeIndex)
		}
		if err != nil {
			log.Printf("Retrying 2 s delayed store index in store %s\n", s.String())
			time.Sleep(2 * time.Second)
			err = updateRemoteStoreIndex(ctx, client, s.jobAPI, storeIndex)
		}

		if err != nil {
			storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, flushMessages, flushReplyMessages, stopMessages)
		}
	}
	if saveStoreContentIndex {
		updatedContentIndex, errno := longtaillib.CreateContentIndexFromStoreIndex(storeIndex, s.maxBlockSize, s.maxChunksPerBlock)
		if errno != 0 {
			storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, flushMessages, flushReplyMessages, stopMessages)
		}
		defer updatedContentIndex.Dispose()

		err := updateRemoteContentIndex(ctx, client, s.jobAPI, updatedContentIndex)
		if err != nil {
			log.Printf("Retrying store content index in store %s\n", s.String())
			err = updateRemoteContentIndex(ctx, client, s.jobAPI, updatedContentIndex)
		}
		if err != nil {
			log.Printf("Retrying 500 ms delayed store content index in store %s\n", s.String())
			time.Sleep(500 * time.Millisecond)
			err = updateRemoteContentIndex(ctx, client, s.jobAPI, updatedContentIndex)
		}
		if err != nil {
			log.Printf("Retrying 2 s delayed store content index in store %s\n", s.String())
			time.Sleep(2 * time.Second)
			err = updateRemoteContentIndex(ctx, client, s.jobAPI, updatedContentIndex)
		}

		if err != nil {
			return errors.Wrapf(err, "WARNING: Failed to write store content index failed")
		}
	}
	return nil
}

// NewRemoteBlockStore ...
func NewRemoteBlockStore(
	jobAPI longtaillib.Longtail_JobAPI,
	blobStore BlobStore,
	maxBlockSize uint32,
	maxChunksPerBlock uint32,
	rebuildIndex bool) (longtaillib.BlockStoreAPI, error) {
	ctx := context.Background()
	defaultClient, err := blobStore.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, blobStore.String())
	}

	s := &remoteStore{
		jobAPI:            jobAPI,
		maxBlockSize:      maxBlockSize,
		maxChunksPerBlock: maxChunksPerBlock,
		blobStore:         blobStore,
		defaultClient:     defaultClient}

	s.workerCount = runtime.NumCPU()
	s.putBlockChan = make(chan putBlockMessage, s.workerCount*8)
	s.getBlockChan = make(chan getBlockMessage, s.workerCount*2048)
	s.prefetchBlockChan = make(chan prefetchBlockMessage, s.workerCount*2048)
	s.preflightGetChan = make(chan preflightGetMessage, 16)
	s.blockIndexChan = make(chan blockIndexMessage, s.workerCount*2048)
	s.getExistingContentChan = make(chan getExistingContentMessage, 16)
	s.workerFlushChan = make(chan int, s.workerCount)
	s.workerFlushReplyChan = make(chan int, s.workerCount)
	s.workerStopChan = make(chan stopMessage, s.workerCount)
	s.indexFlushChan = make(chan int, 1)
	s.indexFlushReplyChan = make(chan int, 1)
	s.indexStopChan = make(chan stopMessage, 1)
	s.workerErrorChan = make(chan error, 1+s.workerCount)

	s.prefetchMemory = 0
	s.maxPrefetchMemory = 512 * 1024 * 1024

	s.fetchedBlocks = map[uint64]bool{}
	s.prefetchBlocks = map[uint64]*pendingPrefetchedBlock{}

	go func() {
		err := contentIndexWorker(ctx, s, s.preflightGetChan, s.prefetchBlockChan, s.blockIndexChan, s.getExistingContentChan, s.indexFlushChan, s.indexFlushReplyChan, s.indexStopChan, rebuildIndex)
		s.workerErrorChan <- err
	}()

	for i := 0; i < s.workerCount; i++ {
		go func() {
			err := remoteWorker(ctx, s, s.putBlockChan, s.getBlockChan, s.prefetchBlockChan, s.blockIndexChan, s.workerFlushChan, s.workerFlushReplyChan, s.workerStopChan)
			s.workerErrorChan <- err
		}()
	}

	return s, nil
}

func getBlockPath(basePath string, blockHash uint64) string {
	fileName := fmt.Sprintf("0x%016x.lsb", blockHash)
	dir := filepath.Join(basePath, fileName[2:6])
	name := filepath.Join(dir, fileName)
	name = strings.Replace(name, "\\", "/", -1)
	return name
}

// PutStoredBlock ...
func (s *remoteStore) PutStoredBlock(storedBlock longtaillib.Longtail_StoredBlock, asyncCompleteAPI longtaillib.Longtail_AsyncPutStoredBlockAPI) int {
	s.putBlockChan <- putBlockMessage{storedBlock: storedBlock, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// PreflightGet ...
func (s *remoteStore) PreflightGet(chunkHashes []uint64) int {
	s.preflightGetChan <- preflightGetMessage{chunkHashes: chunkHashes}
	return 0
}

// GetStoredBlock ...
func (s *remoteStore) GetStoredBlock(blockHash uint64, asyncCompleteAPI longtaillib.Longtail_AsyncGetStoredBlockAPI) int {
	s.getBlockChan <- getBlockMessage{blockHash: blockHash, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// GetExistingContent ...
func (s *remoteStore) GetExistingContent(
	chunkHashes []uint64,
	minBlockUsagePercent uint32,
	asyncCompleteAPI longtaillib.Longtail_AsyncGetExistingContentAPI) int {
	s.getExistingContentChan <- getExistingContentMessage{chunkHashes: chunkHashes, minBlockUsagePercent: minBlockUsagePercent, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// GetStats ...
func (s *remoteStore) GetStats() (longtaillib.BlockStoreStats, int) {
	return s.stats, 0
}

// Flush ...
func (s *remoteStore) Flush(asyncCompleteAPI longtaillib.Longtail_AsyncFlushAPI) int {
	go func() {
		any_errno := 0
		for i := 0; i < s.workerCount; i++ {
			s.workerFlushChan <- 1
		}
		for i := 0; i < s.workerCount; i++ {
			select {
			case errno := <-s.workerFlushReplyChan:
				if errno != 0 && any_errno == 0 {
					any_errno = errno
				}
			}
		}
		s.indexFlushChan <- 1
		select {
		case errno := <-s.indexFlushReplyChan:
			if errno != 0 && any_errno == 0 {
				any_errno = errno
			}
		}
		asyncCompleteAPI.OnComplete(any_errno)
	}()
	return 0
}

// Close ...
func (s *remoteStore) Close() {
	for i := 0; i < s.workerCount; i++ {
		s.workerStopChan <- stopMessage{}
	}
	for i := 0; i < s.workerCount; i++ {
		select {
		case err := <-s.workerErrorChan:
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	s.indexStopChan <- stopMessage{}
	select {
	case err := <-s.workerErrorChan:
		if err != nil {
			log.Fatal(err)
		}
	}
	s.defaultClient.Close()
}
