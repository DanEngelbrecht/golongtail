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
	chunkHashes      []uint64
	asyncCompleteAPI longtaillib.Longtail_AsyncGetExistingContentAPI
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

func putStoredBlock(
	ctx context.Context,
	s *remoteStore,
	blobClient BlobClient,
	blockIndexMessages chan<- blockIndexMessage,
	storedBlock longtaillib.Longtail_StoredBlock) int {

	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count], 1)

	blockIndex := storedBlock.GetBlockIndex()
	blockHash := blockIndex.GetBlockHash()
	key := getBlockPath("chunks", blockHash)
	objHandle, err := blobClient.NewObject(key)
	if err != nil {
		return longtaillib.EIO
	}
	if exists, err := objHandle.Exists(); err == nil && !exists {
		blob, errno := longtaillib.WriteStoredBlockToBuffer(storedBlock)
		if errno != 0 {
			return errno
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
			return longtaillib.EIO
		}

		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count], (uint64)(len(blob)))
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count], (uint64)(blockIndex.GetChunkCount()))
	}

	blockIndexMessages <- blockIndexMessage{blockIndex: blockIndex}
	return 0
}

func getStoredBlock(
	ctx context.Context,
	s *remoteStore,
	blobClient BlobClient,
	blockHash uint64) (longtaillib.Longtail_StoredBlock, int) {

	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count], 1)

	key := getBlockPath("chunks", blockHash)
	objHandle, err := blobClient.NewObject(key)
	if err != nil {
		return longtaillib.Longtail_StoredBlock{}, longtaillib.EIO
	}

	storedBlockData, err := objHandle.Read()
	if err != nil {
		if exists, err := objHandle.Exists(); err == nil && !exists {
			return longtaillib.Longtail_StoredBlock{}, longtaillib.ENOENT
		}
		log.Printf("Retrying getBlob %s in store %s\n", key, s.String())
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_RetryCount], 1)
		storedBlockData, err = objHandle.Read()
	}
	if err != nil {
		log.Printf("Retrying 500 ms delayed getBlob %s in store %s\n", key, s.String())
		time.Sleep(500 * time.Millisecond)
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_RetryCount], 1)
		storedBlockData, err = objHandle.Read()
	}
	if err != nil {
		log.Printf("Retrying 2 s delayed getBlob %s in store %s\n", key, s.String())
		time.Sleep(2 * time.Second)
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_RetryCount], 1)
		storedBlockData, err = objHandle.Read()
	}

	if err != nil {
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1)
		return longtaillib.Longtail_StoredBlock{}, longtaillib.EIO
	}

	storedBlock, errno := longtaillib.ReadStoredBlockFromBuffer(storedBlockData)
	if errno != 0 {
		return longtaillib.Longtail_StoredBlock{}, errno
	}

	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count], (uint64)(len(storedBlockData)))
	blockIndex := storedBlock.GetBlockIndex()
	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count], (uint64)(blockIndex.GetChunkCount()))
	return storedBlock, 0
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
	storedBlock, getStoredBlockErrno := getStoredBlock(ctx, s, client, getMsg.blockHash)
	s.fetchedBlocksSync.Lock()
	completeCallbacks := prefetchedBlock.completeCallbacks
	delete(s.prefetchBlocks, getMsg.blockHash)
	s.fetchedBlocksSync.Unlock()
	for _, c := range completeCallbacks {
		if getStoredBlockErrno != 0 {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, getStoredBlockErrno)
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
	getMsg.asyncCompleteAPI.OnComplete(storedBlock, getStoredBlockErrno)
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

	storedBlock, getErrno := getStoredBlock(ctx, s, client, prefetchMsg.blockHash)
	if getErrno != 0 {
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
		if getErrno != 0 {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, getErrno)
			return
		}
		buf, errno := longtaillib.WriteStoredBlockToBuffer(storedBlock)
		if errno != 0 {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, errno)
			return
		}
		blockCopy, errno := longtaillib.ReadStoredBlockFromBuffer(buf)
		if errno != 0 {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, errno)
			return
		}
		c.OnComplete(blockCopy, 0)
	}
	completeCallbacks[0].OnComplete(storedBlock, getErrno)
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
			errno := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
			putMsg.asyncCompleteAPI.OnComplete(errno)
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
					errno := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
					putMsg.asyncCompleteAPI.OnComplete(errno)
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
					errno := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
					putMsg.asyncCompleteAPI.OnComplete(errno)
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
			errno := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
			putMsg.asyncCompleteAPI.OnComplete(errno)
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
	addedBlockIndexes []longtaillib.Longtail_BlockIndex) error {

	addedStoreIndex, errno := longtaillib.CreateStoreIndexFromBlocks(addedBlockIndexes)
	if errno != 0 {
		return fmt.Errorf("updateRemoteStoreIndex: longtaillib.CreateStoreIndexFromBlocks() failed with error %s", longtaillib.ErrNoToDescription(errno))
	}

	key := "store.lsi"
	objHandle, err := blobClient.NewObject(key)
	if err != nil {
		return errors.Wrap(err, key)
	}
	for {
		exists, err := objHandle.LockWriteVersion()
		if err != nil {
			return errors.Wrap(err, key)
		}
		if exists {
			blob, err := objHandle.Read()
			if err != nil {
				log.Printf("updateRemoteStoreIndex: objHandle.Read() failed with %q\n", err)
				return err
			}

			remoteStoreIndex, errno := longtaillib.ReadStoreIndexFromBuffer(blob)
			if errno != 0 {
				return fmt.Errorf("updateRemoteStoreIndex: longtaillib.ReadStoreIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
			}
			defer remoteStoreIndex.Dispose()

			newStoreIndex, errno := longtaillib.MergeStoreIndex(addedStoreIndex, remoteStoreIndex)
			if errno != 0 {
				return fmt.Errorf("updateRemoteStoreIndex: longtaillib.MergeContentIndex() failed with error %s", longtaillib.ErrNoToDescription(errno))
			}
			defer newStoreIndex.Dispose()

			storeBlob, errno := longtaillib.WriteStoreIndexToBuffer(newStoreIndex)
			if errno != 0 {
				return fmt.Errorf("updateRemoteStoreIndex: longtaillib.WriteContentIndexToBuffer() failed with error %s", longtaillib.ErrNoToDescription(errno))
			}

			ok, err := objHandle.Write(storeBlob)
			if err != nil {
				return fmt.Errorf("updateRemoteStoreIndex: objHandle.Write() failed with error %q", err)
			}
			if ok {
				break
			}
		} else {
			storeBlob, errno := longtaillib.WriteStoreIndexToBuffer(addedStoreIndex)
			if errno != 0 {
				return fmt.Errorf("updateRemoteStoreIndex: longtaillib.WriteContentIndexToBuffer() failed with error %s", longtaillib.ErrNoToDescription(errno))
			}

			ok, err := objHandle.Write(storeBlob)
			if err != nil {
				return fmt.Errorf("updateRemoteStoreIndex: objHandle.Write() failed with error %q", err)
			}
			if ok {
				break
			}
		}
	}
	return nil
}

func getBlockIndexes(
	ctx context.Context,
	s *remoteStore,
	blobClient BlobClient,
	blockKeys []string) ([]longtaillib.Longtail_BlockIndex, int) {
	var blockIndexes []longtaillib.Longtail_BlockIndex

	batchCount := runtime.NumCPU()
	batchStart := 0

	if batchCount > len(blockKeys) {
		batchCount = len(blockKeys)
	}
	clients := make([]BlobClient, batchCount)
	for c := 0; c < batchCount; c++ {
		client, err := s.blobStore.NewClient(ctx)
		if err != nil {
			return blockIndexes, longtaillib.EIO
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
				objHandle, err := client.NewObject(blockKey)
				if err != nil {
					wg.Done()
					return
				}
				storedBlockData, err := objHandle.Read()
				if err != nil {
					if err != nil {
						log.Printf("Retrying getBlob %s in store %s\n", blockKey, s.String())
						storedBlockData, err = objHandle.Read()
					}
					if err != nil {
						log.Printf("Retrying 500 ms delayed getBlob %s in store %s\n", blockKey, s.String())
						time.Sleep(500 * time.Millisecond)
						storedBlockData, err = objHandle.Read()
					}
					if err != nil {
						log.Printf("Retrying 2 s delayed getBlob %s in store %s\n", blockKey, s.String())
						time.Sleep(2 * time.Second)
						storedBlockData, err = objHandle.Read()
					}

					if err != nil {
						wg.Done()
						return
					}
				}
				blockIndex, errno := longtaillib.ReadBlockIndexFromBuffer(storedBlockData)
				if errno != 0 {
					wg.Done()
					return
				}

				batchBlockIndexes[batchPos] = blockIndex
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
		blockIndexes = append(blockIndexes, batchBlockIndexes[:writeIndex]...)
		batchStart += batchLength
		fmt.Printf("Scanned %d/%d blocks in %s\n", batchStart, len(blockKeys), blobClient.String())
	}

	for c := 0; c < batchCount; c++ {
		clients[c].Close()
	}

	return blockIndexes, 0
}

func getBlockIndexesFromStore(
	ctx context.Context,
	s *remoteStore,
	blobClient BlobClient) ([]longtaillib.Longtail_BlockIndex, int) {

	var blockIndexes []longtaillib.Longtail_BlockIndex
	var items []string
	blobs, err := blobClient.GetObjects()
	if err != nil {
		return blockIndexes, longtaillib.EIO
	}

	for _, blob := range blobs {
		if blob.Size == 0 {
			continue
		}
		if strings.HasSuffix(blob.Name, ".lsb") {
			items = append(items, blob.Name)
		}
	}

	return getBlockIndexes(ctx, s, blobClient, items)
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

func contentIndexWorker(
	ctx context.Context,
	s *remoteStore,
	preflightGetMessages <-chan preflightGetMessage,
	prefetchBlockMessages chan<- prefetchBlockMessage,
	blockIndexMessages <-chan blockIndexMessage,
	getExistingContentMessages <-chan getExistingContentMessage,
	flushMessages <-chan int,
	flushReplyMessages chan<- int,
	stopMessages <-chan stopMessage) error {

	client, err := s.blobStore.NewClient(ctx)
	if err != nil {
		storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, flushMessages, flushReplyMessages, stopMessages)
		return errors.Wrap(err, s.blobStore.String())
	}
	defer client.Close()

	var errno int
	var storeIndex longtaillib.Longtail_StoreIndex

	key := "store.lci"

	objHandle, err := client.NewObject(key)
	if exists, err := objHandle.Exists(); err == nil && exists {
		storedStoredIndexData, err := objHandle.Read()
		if err != nil {
			log.Printf("Retrying getBlob %s in store %s\n", key, s.String())
			storedStoredIndexData, err = objHandle.Read()
		}
		if err != nil {
			log.Printf("Retrying 500 ms delayed getBlob %s in store %s\n", key, s.String())
			time.Sleep(500 * time.Millisecond)
			storedStoredIndexData, err = objHandle.Read()
		}
		if err != nil {
			log.Printf("Retrying 2 s delayed getBlob %s in store %s\n", key, s.String())
			time.Sleep(2 * time.Second)
			storedStoredIndexData, err = objHandle.Read()
		}

		if err == nil {
			storeIndex, errno = longtaillib.ReadStoreIndexFromBuffer(storedStoredIndexData)
			if errno != 0 {
				storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, flushMessages, flushReplyMessages, stopMessages)
				return fmt.Errorf("contentIndexWorker: longtaillib.ReadStoreIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
			}
		}
	}

	//	if storeIndex.IsValid() {
	//		s.maxBlockSize = storeIndex.GetMaxBlockSize()
	//		s.maxChunksPerBlock = storeInex.GetMaxChunksPerBlock()
	//	}
	defer storeIndex.Dispose()

	var addedBlockIndexes []longtaillib.Longtail_BlockIndex
	defer func(addedBlockIndexes []longtaillib.Longtail_BlockIndex) {
		for _, blockIndex := range addedBlockIndexes {
			blockIndex.Dispose()
		}
	}(addedBlockIndexes)

	if !storeIndex.IsValid() {
		storeIndex, errno = longtaillib.CreateStoreIndexFromBlocks(
			[]longtaillib.Longtail_BlockIndex{})
		if errno != 0 {
			storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, flushMessages, flushReplyMessages, stopMessages)
			return fmt.Errorf("contentIndexWorker: longtaillib.CreateStoreIndexFromBlocks() failed with %s", longtaillib.ErrNoToDescription(errno))
		}

		addedBlockIndexes, errno = getBlockIndexesFromStore(
			ctx,
			s,
			client)

		if errno == 0 {
			fmt.Printf("Rebuilt remote index with %d blocks\n", len(addedBlockIndexes))
		}
	}

	run := true
	for run {
		received := 0
		select {
		case preflightGetMsg := <-preflightGetMessages:
			received += 1
			addedStoreIndex, errno := longtaillib.CreateStoreIndexFromBlocks(addedBlockIndexes)
			if errno != 0 {
				// TODO: Log error
				continue
			}
			fullStoreIndex, errno := longtaillib.MergeStoreIndex(addedStoreIndex, storeIndex)
			defer fullStoreIndex.Dispose()
			existingContentIndex, errno := longtaillib.GetExistingContentIndex(fullStoreIndex, preflightGetMsg.chunkHashes, s.maxBlockSize, s.maxChunksPerBlock)
			if errno != 0 {
				// TODO: Log error
				continue
			}
			blockHashes := existingContentIndex.GetBlockHashes()
			// TODO: Order is not ideal for prefetch here sadly
			for _, blockHash := range blockHashes {
				prefetchBlockMessages <- prefetchBlockMessage{blockHash: blockHash}
			}
		case blockIndexMsg := <-blockIndexMessages:
			received += 1
			addedBlockIndexes = append(addedBlockIndexes, blockIndexMsg.blockIndex)
		case getExistingContentMessage := <-getExistingContentMessages:
			received += 1
			addedStoreIndex, errno := longtaillib.CreateStoreIndexFromBlocks(addedBlockIndexes)
			if errno != 0 {
				getExistingContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
				continue
			}
			fullStoreIndex, errno := longtaillib.MergeStoreIndex(addedStoreIndex, storeIndex)
			defer fullStoreIndex.Dispose()
			existingContentIndex, errno := longtaillib.GetExistingContentIndex(fullStoreIndex, getExistingContentMessage.chunkHashes, s.maxBlockSize, s.maxChunksPerBlock)
			if errno != 0 {
				getExistingContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
				continue
			}
			getExistingContentMessage.asyncCompleteAPI.OnComplete(existingContentIndex, 0)
		default:
		}

		if received == 0 {
			select {
			case _ = <-flushMessages:
				flushReplyMessages <- 0
			case preflightGetMsg := <-preflightGetMessages:
				addedStoreIndex, errno := longtaillib.CreateStoreIndexFromBlocks(addedBlockIndexes)
				if errno != 0 {
					// TODO: Log error
					continue
				}
				fullStoreIndex, errno := longtaillib.MergeStoreIndex(addedStoreIndex, storeIndex)
				defer fullStoreIndex.Dispose()
				existingContentIndex, errno := longtaillib.GetExistingContentIndex(fullStoreIndex, preflightGetMsg.chunkHashes, s.maxBlockSize, s.maxChunksPerBlock)
				if errno != 0 {
					// TODO: Log error
					continue
				}
				blockHashes := existingContentIndex.GetBlockHashes()
				// TODO: Order is not ideal for prefetch here sadly
				for _, blockHash := range blockHashes {
					prefetchBlockMessages <- prefetchBlockMessage{blockHash: blockHash}
				}
			case blockIndexMsg := <-blockIndexMessages:
				addedBlockIndexes = append(addedBlockIndexes, blockIndexMsg.blockIndex)
			case getExistingContentMessage := <-getExistingContentMessages:
				addedStoreIndex, errno := longtaillib.CreateStoreIndexFromBlocks(addedBlockIndexes)
				if errno != 0 {
					getExistingContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
					continue
				}
				fullStoreIndex, errno := longtaillib.MergeStoreIndex(addedStoreIndex, storeIndex)
				defer fullStoreIndex.Dispose()
				existingContentIndex, errno := longtaillib.GetExistingContentIndex(fullStoreIndex, getExistingContentMessage.chunkHashes, s.maxBlockSize, s.maxChunksPerBlock)
				if errno != 0 {
					getExistingContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
					continue
				}
				getExistingContentMessage.asyncCompleteAPI.OnComplete(existingContentIndex, 0)
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
		err := updateRemoteStoreIndex(ctx, client, s.jobAPI, addedBlockIndexes)
		if err != nil {
			log.Printf("Retrying store index %s in store %s\n", key, s.String())
			err = updateRemoteStoreIndex(ctx, client, s.jobAPI, addedBlockIndexes)
		}
		if err != nil {
			log.Printf("Retrying 500 ms delayed store index %s in store %s\n", key, s.String())
			time.Sleep(500 * time.Millisecond)
			err = updateRemoteStoreIndex(ctx, client, s.jobAPI, addedBlockIndexes)
		}
		if err != nil {
			log.Printf("Retrying 2 s delayed store index %s in store %s\n", key, s.String())
			time.Sleep(2 * time.Second)
			err = updateRemoteStoreIndex(ctx, client, s.jobAPI, addedBlockIndexes)
		}

		if err != nil {
			return fmt.Errorf("WARNING: Failed to write store content index failed with %q", err)
		}
	}
	return nil
}

// NewRemoteBlockStore ...
func NewRemoteBlockStore(
	jobAPI longtaillib.Longtail_JobAPI,
	blobStore BlobStore,
	maxBlockSize uint32,
	maxChunksPerBlock uint32) (longtaillib.BlockStoreAPI, error) {
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
		err := contentIndexWorker(ctx, s, s.preflightGetChan, s.prefetchBlockChan, s.blockIndexChan, s.getExistingContentChan, s.indexFlushChan, s.indexFlushReplyChan, s.indexStopChan)
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
	asyncCompleteAPI longtaillib.Longtail_AsyncGetExistingContentAPI) int {
	s.getExistingContentChan <- getExistingContentMessage{chunkHashes: chunkHashes, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// GetStats ...
func (s *remoteStore) GetStats() (longtaillib.BlockStoreStats, int) {
	return s.stats, 0
}

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
