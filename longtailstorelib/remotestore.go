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

type getIndexMessage struct {
	asyncCompleteAPI longtaillib.Longtail_AsyncGetIndexAPI
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
	storedBlock         longtaillib.Longtail_StoredBlock
	completeCallbacks   []longtaillib.Longtail_AsyncGetStoredBlockAPI
	workerPrefetchCount *int64
}

type remoteStore struct {
	jobAPI            longtaillib.Longtail_JobAPI
	maxBlockSize      uint32
	maxChunksPerBlock uint32
	blobStore         BlobStore
	defaultClient     BlobClient

	workerCount int

	putBlockChan        chan putBlockMessage
	getBlockChan        chan getBlockMessage
	prefetchBlockChan   chan prefetchBlockMessage
	contentIndexChan    chan contentIndexMessage
	getIndexChan        chan getIndexMessage
	retargetContentChan chan retargetContentMessage
	workerStopChan      chan stopMessage
	indexStopChan       chan stopMessage
	workerErrorChan     chan error

	fetchedBlocksSync sync.Mutex
	fetchedBlocks     map[uint64]bool
	prefetchBlocks    map[uint64]*pendingPrefetchedBlock

	stats         longtaillib.BlockStoreStats
	outFinalStats *longtaillib.BlockStoreStats
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
			log.Printf("Retrying putBlob %s", key)
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount], 1)
			ok, err = objHandle.Write(blob)
		}
		if err != nil || !ok {
			log.Printf("Retrying 500 ms delayed putBlob %s", key)
			time.Sleep(500 * time.Millisecond)
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount], 1)
			ok, err = objHandle.Write(blob)
		}
		if err != nil || !ok {
			log.Printf("Retrying 2 s delayed putBlob %s", key)
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
			return longtaillib.Longtail_StoredBlock{}, longtaillib.ENOENT
		}
		log.Printf("Retrying getBlob %s", key)
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_RetryCount], 1)
		storedBlockData, err = objHandle.Read()
	}
	if err != nil {
		log.Printf("Retrying 500 ms delayed getBlob %s", key)
		time.Sleep(500 * time.Millisecond)
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_RetryCount], 1)
		storedBlockData, err = objHandle.Read()
	}
	if err != nil {
		log.Printf("Retrying 2 s delayed getBlob %s", key)
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
			atomic.AddInt64(prefetchedBlock.workerPrefetchCount, -1)
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

func remoteWorker(
	ctx context.Context,
	s *remoteStore,
	putBlockMessages <-chan putBlockMessage,
	getBlockMessages <-chan getBlockMessage,
	prefetchBlockChan <-chan prefetchBlockMessage,
	contentIndexMessages chan<- contentIndexMessage,
	stopMessages <-chan stopMessage) error {
	client, err := s.blobStore.NewClient(ctx)
	if err != nil {
		return errors.Wrap(err, s.blobStore.String())
	}
	workerPrefetchCount := int64(0)
	run := true
	for run {
		select {
		case putMsg := <-putBlockMessages:
			errno := putStoredBlock(ctx, s, client, contentIndexMessages, putMsg.storedBlock)
			putMsg.asyncCompleteAPI.OnComplete(errno)
		case getMsg := <-getBlockMessages:
			fetchBlock(ctx, s, client, getMsg)
		default:
		}
		if workerPrefetchCount > 2 {
			select {
			case putMsg := <-putBlockMessages:
				errno := putStoredBlock(ctx, s, client, contentIndexMessages, putMsg.storedBlock)
				putMsg.asyncCompleteAPI.OnComplete(errno)
			case getMsg := <-getBlockMessages:
				fetchBlock(ctx, s, client, getMsg)
			case _ = <-stopMessages:
				run = false
			}
		} else {
			select {
			case putMsg := <-putBlockMessages:
				errno := putStoredBlock(ctx, s, client, contentIndexMessages, putMsg.storedBlock)
				putMsg.asyncCompleteAPI.OnComplete(errno)
			case getMsg := <-getBlockMessages:
				fetchBlock(ctx, s, client, getMsg)
			case prefetchMsg := <-prefetchBlockChan:
				s.fetchedBlocksSync.Lock()
				_, exists := s.fetchedBlocks[prefetchMsg.blockHash]
				if exists {
					// Already fetched
					s.fetchedBlocksSync.Unlock()
					continue
				}
				s.fetchedBlocks[prefetchMsg.blockHash] = true
				prefetchedBlock := &pendingPrefetchedBlock{storedBlock: longtaillib.Longtail_StoredBlock{}}
				s.prefetchBlocks[prefetchMsg.blockHash] = prefetchedBlock
				s.fetchedBlocksSync.Unlock()

				storedBlock, getErrno := getStoredBlock(ctx, s, client, prefetchMsg.blockHash)

				s.fetchedBlocksSync.Lock()
				prefetchedBlock = s.prefetchBlocks[prefetchMsg.blockHash]
				completeCallbacks := prefetchedBlock.completeCallbacks
				if len(completeCallbacks) == 0 {
					prefetchedBlock.storedBlock = storedBlock
					prefetchedBlock.workerPrefetchCount = &workerPrefetchCount
					atomic.AddInt64(&workerPrefetchCount, 1)
					s.fetchedBlocksSync.Unlock()
					continue
				}
				delete(s.prefetchBlocks, prefetchMsg.blockHash)
				s.fetchedBlocksSync.Unlock()
				for i := 1; i < len(completeCallbacks)-1; i++ {
					c := completeCallbacks[i]
					if getErrno != 0 {
						c.OnComplete(longtaillib.Longtail_StoredBlock{}, getErrno)
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
				completeCallbacks[0].OnComplete(storedBlock, getErrno)
			case _ = <-stopMessages:
				run = false
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
				log.Printf("updateRemoteContentIndex: objHandle.Read() failed with %q", err)
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
			go func(batchPos int, blockKey string) {
				client, err := s.blobStore.NewClient(ctx)
				if err != nil {
					wg.Done()
					return
				}

				objHandle, err := client.NewObject(blockKey)
				if err != nil {
					wg.Done()
					return
				}
				storedBlockData, err := objHandle.Read()
				if err != nil {
					wg.Done()
					return
				}
				blockIndex, errno := longtaillib.ReadBlockIndexFromBuffer(storedBlockData)
				if errno != 0 {
					wg.Done()
					return
				}

				blockIndexes[batchPos] = blockIndex
				wg.Done()
			}(batchPos, blockKey)
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
	}

	return contentIndex, errno
}

func contentIndexWorkerReplyErrorState(
	contentIndexMessages <-chan contentIndexMessage,
	getIndexMessages <-chan getIndexMessage,
	retargetContentMessages <-chan retargetContentMessage,
	stopMessages <-chan stopMessage) {
	run := true
	for run {
		select {
		case _ = <-contentIndexMessages:
		case getIndexMessage := <-getIndexMessages:
			getIndexMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, longtaillib.EINVAL)
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
	getIndexMessages <-chan getIndexMessage,
	retargetContentMessages <-chan retargetContentMessage,
	stopMessages <-chan stopMessage) error {

	client, err := s.blobStore.NewClient(ctx)
	if err != nil {
		contentIndexWorkerReplyErrorState(contentIndexMessages, getIndexMessages, retargetContentMessages, stopMessages)
		return errors.Wrap(err, s.blobStore.String())
	}

	var errno int
	var contentIndex longtaillib.Longtail_ContentIndex

	key := "store.lci"

	objHandle, err := client.NewObject(key)

	if exists, err := objHandle.Exists(); err == nil && exists {
		storedContentIndexData, err := objHandle.Read()
		if err != nil {
			log.Printf("Retrying getBlob %s", key)
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetIndex_RetryCount], 1)
			storedContentIndexData, err = objHandle.Read()
		}
		if err != nil {
			log.Printf("Retrying 500 ms delayed getBlob %s", key)
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetIndex_RetryCount], 1)
			storedContentIndexData, err = objHandle.Read()
		}
		if err != nil {
			log.Printf("Retrying 2 s delayed getBlob %s", key)
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetIndex_RetryCount], 1)
			storedContentIndexData, err = objHandle.Read()
		}

		if err == nil {
			contentIndex, errno = longtaillib.ReadContentIndexFromBuffer(storedContentIndexData)
			if errno != 0 {
				contentIndexWorkerReplyErrorState(contentIndexMessages, getIndexMessages, retargetContentMessages, stopMessages)
				return fmt.Errorf("contentIndexWorker: longtaillib.ReadContentIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
			}
		} else {
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetIndex_FailCount], 1)
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
			contentIndexWorkerReplyErrorState(contentIndexMessages, getIndexMessages, retargetContentMessages, stopMessages)
			return fmt.Errorf("contentIndexWorker: longtaillib.CreateContentIndexFromBlocks() failed with %s", longtaillib.ErrNoToDescription(errno))
		}

		addedContentIndex, errno = buildContentIndexFromBlocks(
			ctx,
			s,
			client)

		if errno != 0 {
			addedContentIndex, errno = longtaillib.CreateContentIndexFromBlocks(
				s.maxBlockSize,
				s.maxChunksPerBlock,
				[]longtaillib.Longtail_BlockIndex{})
			if errno != 0 {
				contentIndexWorkerReplyErrorState(contentIndexMessages, getIndexMessages, retargetContentMessages, stopMessages)
				return fmt.Errorf("contentIndexWorker: longtaillib.CreateContentIndexFromBlocks() failed with %s", longtaillib.ErrNoToDescription(errno))
			}
		}
	} else {
		addedContentIndex, errno = longtaillib.CreateContentIndexFromBlocks(
			s.maxBlockSize,
			s.maxChunksPerBlock,
			[]longtaillib.Longtail_BlockIndex{})
		if errno != 0 {
			contentIndexWorkerReplyErrorState(contentIndexMessages, getIndexMessages, retargetContentMessages, stopMessages)
			return fmt.Errorf("contentIndexWorker: longtaillib.CreateContentIndexFromBlocks() failed with %s", longtaillib.ErrNoToDescription(errno))
		}
	}
	defer addedContentIndex.Dispose()

	run := true
	for run {
		select {
		case contentIndexMsg := <-contentIndexMessages:
			newAddedContentIndex, errno := longtaillib.AddContentIndex(addedContentIndex, contentIndexMsg.contentIndex)
			if errno != 0 {
				contentIndexWorkerReplyErrorState(contentIndexMessages, getIndexMessages, retargetContentMessages, stopMessages)
				return fmt.Errorf("contentIndexWorker: longtaillib.AddContentIndex() failed with %s", longtaillib.ErrNoToDescription(errno))
			}
			addedContentIndex.Dispose()
			addedContentIndex = newAddedContentIndex
			contentIndexMsg.contentIndex.Dispose()
		case getIndexMessage := <-getIndexMessages:
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetIndex_Count], 1)
			contentIndexCopy, errno := longtaillib.MergeContentIndex(s.jobAPI, contentIndex, addedContentIndex)
			if errno != 0 {
				getIndexMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
				continue
			}
			getIndexMessage.asyncCompleteAPI.OnComplete(contentIndexCopy, 0)
		case retargetContentMessage := <-retargetContentMessages:
			fullContentIndex, errno := longtaillib.MergeContentIndex(s.jobAPI, contentIndex, addedContentIndex)
			if errno != 0 {
				retargetContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
				continue
			}
			defer fullContentIndex.Dispose()
			retargetedIndex, errno := longtaillib.RetargetContent(fullContentIndex, contentIndex)
			if errno != 0 {
				retargetContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
				continue
			}
			retargetContentMessage.asyncCompleteAPI.OnComplete(retargetedIndex, 0)
		case _ = <-stopMessages:
			run = false
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
			log.Printf("Retrying store index")
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetIndex_RetryCount], 1)
			err = updateRemoteContentIndex(ctx, client, s.jobAPI, addedContentIndex)
		}
		if err != nil {
			log.Printf("Retrying 500 ms delayed store index")
			time.Sleep(500 * time.Millisecond)
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetIndex_RetryCount], 1)
			err = updateRemoteContentIndex(ctx, client, s.jobAPI, addedContentIndex)
		}
		if err != nil {
			log.Printf("Retrying 2 s delayed store index")
			time.Sleep(2 * time.Second)
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetIndex_RetryCount], 1)
			err = updateRemoteContentIndex(ctx, client, s.jobAPI, addedContentIndex)
		}

		if err != nil {
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetIndex_FailCount], 1)
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
	maxChunksPerBlock uint32,
	outFinalStats *longtaillib.BlockStoreStats) (longtaillib.BlockStoreAPI, error) {
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
		defaultClient:     defaultClient,
		outFinalStats:     outFinalStats}

	s.workerCount = runtime.NumCPU()
	s.putBlockChan = make(chan putBlockMessage, s.workerCount*8)
	s.getBlockChan = make(chan getBlockMessage, s.workerCount*2048)
	s.prefetchBlockChan = make(chan prefetchBlockMessage, s.workerCount*2048)
	s.contentIndexChan = make(chan contentIndexMessage, s.workerCount*2048)
	s.getIndexChan = make(chan getIndexMessage)
	s.retargetContentChan = make(chan retargetContentMessage, 16)
	s.workerStopChan = make(chan stopMessage, s.workerCount)
	s.indexStopChan = make(chan stopMessage, 1)
	s.workerErrorChan = make(chan error, 1+s.workerCount)
	s.fetchedBlocks = map[uint64]bool{}
	s.prefetchBlocks = map[uint64]*pendingPrefetchedBlock{}

	go func() {
		err := contentIndexWorker(ctx, s, s.contentIndexChan, s.getIndexChan, s.retargetContentChan, s.indexStopChan)
		s.workerErrorChan <- err
	}()

	for i := 0; i < s.workerCount; i++ {
		go func() {
			err := remoteWorker(ctx, s, s.putBlockChan, s.getBlockChan, s.prefetchBlockChan, s.contentIndexChan, s.workerStopChan)
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
func (s *remoteStore) PreflightGet(blockCount uint64, hashes []uint64, refCounts []uint32) int {
	for b := uint64(0); b < blockCount; b++ {
		s.prefetchBlockChan <- prefetchBlockMessage{blockHash: hashes[blockCount-1-b]}
	}
	return 0
}

// GetStoredBlock ...
func (s *remoteStore) GetStoredBlock(blockHash uint64, asyncCompleteAPI longtaillib.Longtail_AsyncGetStoredBlockAPI) int {
	s.getBlockChan <- getBlockMessage{blockHash: blockHash, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// GetIndex ...
func (s *remoteStore) GetIndex(asyncCompleteAPI longtaillib.Longtail_AsyncGetIndexAPI) int {
	s.getIndexChan <- getIndexMessage{asyncCompleteAPI: asyncCompleteAPI}
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
	asyncCompleteAPI.OnComplete(0)
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
	if s.outFinalStats != nil {
		*s.outFinalStats = s.stats
	}
}
