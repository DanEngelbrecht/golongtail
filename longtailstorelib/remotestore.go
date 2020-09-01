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

type contentIndexMessage struct {
	contentIndex longtaillib.Longtail_ContentIndex
}

type retargetContentMessage struct {
	contentIndex     longtaillib.Longtail_ContentIndex
	asyncCompleteAPI longtaillib.Longtail_AsyncRetargetContentAPI
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

	putBlockChan         chan putBlockMessage
	getBlockChan         chan getBlockMessage
	prefetchBlockChan    chan prefetchBlockMessage
	contentIndexChan     chan contentIndexMessage
	retargetContentChan  chan retargetContentMessage
	workerFlushChan      chan int
	workerFlushReplyChan chan int
	workerStopChan       chan stopMessage
	indexFlushChan       chan int
	indexFlushReplyChan  chan int
	indexStopChan        chan stopMessage
	workerErrorChan      chan error
	prefetchMemory       int64
	maxPrefetchMemory    int64

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
	contentIndexMessages chan<- contentIndexMessage,
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

	newBlocks := []longtaillib.Longtail_BlockIndex{blockIndex}
	addedContentIndex, errno := longtaillib.CreateContentIndexFromBlocks(s.maxBlockSize, s.maxChunksPerBlock, newBlocks)
	if errno != 0 {
		return errno
	}
	contentIndexMessages <- contentIndexMessage{contentIndex: addedContentIndex}
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
			log.Printf("Block %s is not in store %s\n", key, s.String())
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
	contentIndexMessages chan<- contentIndexMessage,
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
			errno := putStoredBlock(ctx, s, client, contentIndexMessages, putMsg.storedBlock)
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
					errno := putStoredBlock(ctx, s, client, contentIndexMessages, putMsg.storedBlock)
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
					errno := putStoredBlock(ctx, s, client, contentIndexMessages, putMsg.storedBlock)
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
			errno := putStoredBlock(ctx, s, client, contentIndexMessages, putMsg.storedBlock)
			putMsg.asyncCompleteAPI.OnComplete(errno)
		default:
		}
	}

	return nil
}

func updateRemoteContentIndex(
	ctx context.Context,
	blobClient BlobClient,
	//	prefix string,
	jobAPI longtaillib.Longtail_JobAPI,
	addedContentIndex longtaillib.Longtail_ContentIndex) error {
	key := "store.lci"
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
				log.Printf("updateRemoteContentIndex: objHandle.Read() failed with %q\n", err)
				return err
			}

			remoteContentIndex, errno := longtaillib.ReadContentIndexFromBuffer(blob)
			if errno != 0 {
				return fmt.Errorf("updateRemoteContentIndex: longtaillib.ReadContentIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
			}
			defer remoteContentIndex.Dispose()

			newContentIndex, errno := longtaillib.MergeContentIndex(jobAPI, remoteContentIndex, addedContentIndex)
			if errno != 0 {
				return fmt.Errorf("updateRemoteContentIndex: longtaillib.MergeContentIndex() failed with error %s", longtaillib.ErrNoToDescription(errno))
			}
			defer newContentIndex.Dispose()

			storeBlob, errno := longtaillib.WriteContentIndexToBuffer(newContentIndex)
			if errno != 0 {
				return fmt.Errorf("updateRemoteContentIndex: longtaillib.WriteContentIndexToBuffer() failed with error %s", longtaillib.ErrNoToDescription(errno))
			}

			ok, err := objHandle.Write(storeBlob)
			if err != nil {
				return fmt.Errorf("updateRemoteContentIndex: objHandle.Write() failed with error %q", err)
			}
			if ok {
				break
			}
		} else {
			storeBlob, errno := longtaillib.WriteContentIndexToBuffer(addedContentIndex)
			if errno != 0 {
				return fmt.Errorf("updateRemoteContentIndex: longtaillib.WriteContentIndexToBuffer() failed with error %s", longtaillib.ErrNoToDescription(errno))
			}

			ok, err := objHandle.Write(storeBlob)
			if err != nil {
				return fmt.Errorf("updateRemoteContentIndex: objHandle.Write() failed with error %q", err)
			}
			if ok {
				break
			}
		}
	}
	return nil
}

func buildContentIndexFromBlocks(
	ctx context.Context,
	s *remoteStore,
	blobClient BlobClient) (longtaillib.Longtail_ContentIndex, int) {

	var items []string
	blobs, err := blobClient.GetObjects()
	if err != nil {
		return longtaillib.Longtail_ContentIndex{}, longtaillib.EIO
	}

	for _, blob := range blobs {
		if blob.Size == 0 {
			continue
		}
		if strings.HasSuffix(blob.Name, ".lsb") {
			items = append(items, blob.Name)
		}
	}

	contentIndex, errno := longtaillib.CreateContentIndexFromBlocks(
		s.maxBlockSize,
		s.maxChunksPerBlock,
		[]longtaillib.Longtail_BlockIndex{})

	batchCount := 32
	batchStart := 0

	var clients []BlobClient
	if batchCount > len(items) {
		batchCount = len(items)
	}
	for c := 0; c < batchCount; c++ {
		client, err := s.blobStore.NewClient(ctx)
		if err != nil {
			return longtaillib.Longtail_ContentIndex{}, longtaillib.EIO
		}
		clients = append(clients, client)
	}

	var wg sync.WaitGroup

	for batchStart < len(items) {
		batchLength := batchCount
		if batchStart+batchLength > len(items) {
			batchLength = len(items) - batchStart
		}
		blockIndexes := make([]longtaillib.Longtail_BlockIndex, batchLength)
		wg.Add(batchLength)
		for batchPos := 0; batchPos < batchLength; batchPos++ {
			i := batchStart + batchPos
			blockKey := items[i]
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

				blockIndexes[batchPos] = blockIndex
				wg.Done()
			}(clients[batchPos], batchPos, blockKey)
		}
		wg.Wait()
		writeIndex := 0
		for i, blockIndex := range blockIndexes {
			if !blockIndex.IsValid() {
				continue
			}
			if i > writeIndex {
				blockIndexes[writeIndex] = blockIndex
			}
			writeIndex++
		}
		addedContentIndex, errno := longtaillib.CreateContentIndexFromBlocks(s.maxBlockSize, s.maxChunksPerBlock, blockIndexes[:writeIndex])
		if errno == 0 {
			newContentIndex, errno := longtaillib.AddContentIndex(contentIndex, addedContentIndex)
			if errno == 0 {
				addedContentIndex.Dispose()
				contentIndex.Dispose()
				contentIndex = newContentIndex
			} else {
				addedContentIndex.Dispose()
			}
		}
		for _, blockIndex := range blockIndexes {
			blockIndex.Dispose()
		}
		batchStart += batchLength
		fmt.Printf("Scanned %d/%d blocks in %s\n", batchStart, len(items), blobClient.String())
	}

	for c := 0; c < batchCount; c++ {
		clients[c].Close()
	}

	return contentIndex, errno
}

func contentIndexWorkerReplyErrorState(
	contentIndexMessages <-chan contentIndexMessage,
	retargetContentMessages <-chan retargetContentMessage,
	flushMessages <-chan int,
	flushReplyMessages chan<- int,
	stopMessages <-chan stopMessage) {
	run := true
	for run {
		select {
		case _ = <-flushMessages:
			flushReplyMessages <- 0
		case _ = <-contentIndexMessages:
		case retargetContentMessage := <-retargetContentMessages:
			retargetContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, longtaillib.EINVAL)
		case _ = <-stopMessages:
			run = false
		}
	}
}

func contentIndexWorker(
	ctx context.Context,
	s *remoteStore,
	contentIndexMessages <-chan contentIndexMessage,
	retargetContentMessages <-chan retargetContentMessage,
	flushMessages <-chan int,
	flushReplyMessages chan<- int,
	stopMessages <-chan stopMessage) error {

	client, err := s.blobStore.NewClient(ctx)
	if err != nil {
		contentIndexWorkerReplyErrorState(contentIndexMessages, retargetContentMessages, flushMessages, flushReplyMessages, stopMessages)
		return errors.Wrap(err, s.blobStore.String())
	}
	defer client.Close()

	var errno int
	var contentIndex longtaillib.Longtail_ContentIndex

	key := "store.lci"

	objHandle, err := client.NewObject(key)
	if exists, err := objHandle.Exists(); err == nil && exists {
		storedContentIndexData, err := objHandle.Read()
		if err != nil {
			log.Printf("Retrying getBlob %s in store %s\n", key, s.String())
			storedContentIndexData, err = objHandle.Read()
		}
		if err != nil {
			log.Printf("Retrying 500 ms delayed getBlob %s in store %s\n", key, s.String())
			time.Sleep(500 * time.Millisecond)
			storedContentIndexData, err = objHandle.Read()
		}
		if err != nil {
			log.Printf("Retrying 2 s delayed getBlob %s in store %s\n", key, s.String())
			time.Sleep(2 * time.Second)
			storedContentIndexData, err = objHandle.Read()
		}

		if err == nil {
			contentIndex, errno = longtaillib.ReadContentIndexFromBuffer(storedContentIndexData)
			if errno != 0 {
				contentIndexWorkerReplyErrorState(contentIndexMessages, retargetContentMessages, flushMessages, flushReplyMessages, stopMessages)
				return fmt.Errorf("contentIndexWorker: longtaillib.ReadContentIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
			}
		}
	}

	if contentIndex.IsValid() {
		s.maxBlockSize = contentIndex.GetMaxBlockSize()
		s.maxChunksPerBlock = contentIndex.GetMaxChunksPerBlock()
	}
	defer contentIndex.Dispose()

	var addedContentIndex longtaillib.Longtail_ContentIndex

	if !contentIndex.IsValid() {
		contentIndex, errno = longtaillib.CreateContentIndexFromBlocks(
			s.maxBlockSize,
			s.maxChunksPerBlock,
			[]longtaillib.Longtail_BlockIndex{})
		if errno != 0 {
			contentIndexWorkerReplyErrorState(contentIndexMessages, retargetContentMessages, flushMessages, flushReplyMessages, stopMessages)
			return fmt.Errorf("contentIndexWorker: longtaillib.CreateContentIndexFromBlocks() failed with %s", longtaillib.ErrNoToDescription(errno))
		}

		addedContentIndex, errno = buildContentIndexFromBlocks(
			ctx,
			s,
			client)

		if errno == 0 {
			fmt.Printf("Rebuilt remote index with %d blocks\n", addedContentIndex.GetBlockCount())
		}

		if errno != 0 {
			addedContentIndex, errno = longtaillib.CreateContentIndexFromBlocks(
				s.maxBlockSize,
				s.maxChunksPerBlock,
				[]longtaillib.Longtail_BlockIndex{})
			if errno != 0 {
				contentIndexWorkerReplyErrorState(contentIndexMessages, retargetContentMessages, flushMessages, flushReplyMessages, stopMessages)
				return fmt.Errorf("contentIndexWorker: longtaillib.CreateContentIndexFromBlocks() failed with %s", longtaillib.ErrNoToDescription(errno))
			}
		}
	} else {
		addedContentIndex, errno = longtaillib.CreateContentIndexFromBlocks(
			s.maxBlockSize,
			s.maxChunksPerBlock,
			[]longtaillib.Longtail_BlockIndex{})
		if errno != 0 {
			contentIndexWorkerReplyErrorState(contentIndexMessages, retargetContentMessages, flushMessages, flushReplyMessages, stopMessages)
			return fmt.Errorf("contentIndexWorker: longtaillib.CreateContentIndexFromBlocks() failed with %s", longtaillib.ErrNoToDescription(errno))
		}
	}
	defer addedContentIndex.Dispose()

	run := true
	for run {
		received := 0
		select {
		case contentIndexMsg := <-contentIndexMessages:
			received += 1
			newAddedContentIndex, errno := longtaillib.AddContentIndex(addedContentIndex, contentIndexMsg.contentIndex)
			if errno != 0 {
				contentIndexWorkerReplyErrorState(contentIndexMessages, retargetContentMessages, flushMessages, flushReplyMessages, stopMessages)
				return fmt.Errorf("contentIndexWorker: longtaillib.AddContentIndex() failed with %s", longtaillib.ErrNoToDescription(errno))
			}
			addedContentIndex.Dispose()
			addedContentIndex = newAddedContentIndex
			contentIndexMsg.contentIndex.Dispose()
		case retargetContentMessage := <-retargetContentMessages:
			received += 1
			fullContentIndex, errno := longtaillib.MergeContentIndex(s.jobAPI, contentIndex, addedContentIndex)
			if errno != 0 {
				retargetContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
				continue
			}
			defer fullContentIndex.Dispose()
			retargetedIndex, errno := longtaillib.RetargetContent(fullContentIndex, retargetContentMessage.contentIndex)
			if errno != 0 {
				retargetContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
				continue
			}
			retargetContentMessage.asyncCompleteAPI.OnComplete(retargetedIndex, 0)
		default:
		}

		if received == 0 {
			select {
			case _ = <-flushMessages:
				flushReplyMessages <- 0
			case contentIndexMsg := <-contentIndexMessages:
				newAddedContentIndex, errno := longtaillib.AddContentIndex(addedContentIndex, contentIndexMsg.contentIndex)
				if errno != 0 {
					contentIndexWorkerReplyErrorState(contentIndexMessages, retargetContentMessages, flushMessages, flushReplyMessages, stopMessages)
					return fmt.Errorf("contentIndexWorker: longtaillib.AddContentIndex() failed with %s", longtaillib.ErrNoToDescription(errno))
				}
				addedContentIndex.Dispose()
				addedContentIndex = newAddedContentIndex
				contentIndexMsg.contentIndex.Dispose()
			case retargetContentMessage := <-retargetContentMessages:
				fullContentIndex, errno := longtaillib.MergeContentIndex(s.jobAPI, contentIndex, addedContentIndex)
				if errno != 0 {
					retargetContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
					continue
				}
				defer fullContentIndex.Dispose()
				retargetedIndex, errno := longtaillib.RetargetContent(fullContentIndex, retargetContentMessage.contentIndex)
				if errno != 0 {
					retargetContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
					continue
				}
				retargetContentMessage.asyncCompleteAPI.OnComplete(retargetedIndex, 0)
			case _ = <-stopMessages:
				run = false
			}
		}
	}

	for len(contentIndexMessages) > 0 {
		select {
		case contentIndexMsg := <-contentIndexMessages:
			newAddedContentIndex, errno := longtaillib.AddContentIndex(addedContentIndex, contentIndexMsg.contentIndex)
			if errno != 0 {
				return fmt.Errorf("contentIndexWorker: longtaillib.AddContentIndex() failed with %s", longtaillib.ErrNoToDescription(errno))
			}
			addedContentIndex.Dispose()
			addedContentIndex = newAddedContentIndex
			contentIndexMsg.contentIndex.Dispose()
		default:
		}
	}

	if addedContentIndex.GetBlockCount() > 0 {
		err := updateRemoteContentIndex(ctx, client, s.jobAPI, addedContentIndex)
		if err != nil {
			log.Printf("Retrying store index %s in store %s\n", key, s.String())
			err = updateRemoteContentIndex(ctx, client, s.jobAPI, addedContentIndex)
		}
		if err != nil {
			log.Printf("Retrying 500 ms delayed store index %s in store %s\n", key, s.String())
			time.Sleep(500 * time.Millisecond)
			err = updateRemoteContentIndex(ctx, client, s.jobAPI, addedContentIndex)
		}
		if err != nil {
			log.Printf("Retrying 2 s delayed store index %s in store %s\n", key, s.String())
			time.Sleep(2 * time.Second)
			err = updateRemoteContentIndex(ctx, client, s.jobAPI, addedContentIndex)
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
	s.contentIndexChan = make(chan contentIndexMessage, s.workerCount*2048)
	s.retargetContentChan = make(chan retargetContentMessage, 16)
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
		err := contentIndexWorker(ctx, s, s.contentIndexChan, s.retargetContentChan, s.indexFlushChan, s.indexFlushReplyChan, s.indexStopChan)
		s.workerErrorChan <- err
	}()

	for i := 0; i < s.workerCount; i++ {
		go func() {
			err := remoteWorker(ctx, s, s.putBlockChan, s.getBlockChan, s.prefetchBlockChan, s.contentIndexChan, s.workerFlushChan, s.workerFlushReplyChan, s.workerStopChan)
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
func (s *remoteStore) PreflightGet(contentIndex longtaillib.Longtail_ContentIndex) int {
	blockHashes := contentIndex.GetBlockHashes()
	blockCount := uint64(len(blockHashes))
	for b := uint64(0); b < blockCount; b++ {
		s.prefetchBlockChan <- prefetchBlockMessage{blockHash: blockHashes[blockCount-1-b]}
	}
	return 0
}

// GetStoredBlock ...
func (s *remoteStore) GetStoredBlock(blockHash uint64, asyncCompleteAPI longtaillib.Longtail_AsyncGetStoredBlockAPI) int {
	s.getBlockChan <- getBlockMessage{blockHash: blockHash, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// RetargetContent ...
func (s *remoteStore) RetargetContent(
	contentIndex longtaillib.Longtail_ContentIndex,
	asyncCompleteAPI longtaillib.Longtail_AsyncRetargetContentAPI) int {
	s.retargetContentChan <- retargetContentMessage{contentIndex: contentIndex, asyncCompleteAPI: asyncCompleteAPI}
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
