package remotestore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// AccessType defines how we will access the data in the store
type AccessType int

const (
	// Init - read/write access with forced rebuild of store index
	Init AccessType = iota
	// ReadWrite - read/write access with optional rebuild of store index
	ReadWrite
	// ReadOnly - read only access
	ReadOnly
)

type putBlockMessage struct {
	storedBlock      longtaillib.Longtail_StoredBlock
	asyncCompleteAPI longtaillib.Longtail_AsyncPutStoredBlockAPI
}

type getBlockMessage struct {
	blockHash        uint64
	asyncCompleteAPI longtaillib.Longtail_AsyncGetStoredBlockAPI
}

type deleteBlockMessage struct {
	blockHash      uint64
	completeSignal *sync.WaitGroup
	successCounter *uint32
}

type prefetchBlockMessage struct {
	blockHash uint64
}

type preflightGetMessage struct {
	blockHashes      []uint64
	asyncCompleteAPI longtaillib.Longtail_AsyncPreflightStartedAPI
}

type blockIndexMessage struct {
	blockIndex longtaillib.Longtail_BlockIndex
}

type getExistingContentMessage struct {
	chunkHashes          []uint64
	minBlockUsagePercent uint32
	asyncCompleteAPI     longtaillib.Longtail_AsyncGetExistingContentAPI
}

type pruneBlocksMessage struct {
	keepBlockHashes  []uint64
	asyncCompleteAPI longtaillib.Longtail_AsyncPruneBlocksAPI
}

type pendingPrefetchedBlock struct {
	storedBlock       longtaillib.Longtail_StoredBlock
	completeCallbacks []longtaillib.Longtail_AsyncGetStoredBlockAPI
}

type remoteStore struct {
	jobAPI        longtaillib.Longtail_JobAPI
	blobStore     longtailstorelib.BlobStore
	defaultClient longtailstorelib.BlobClient

	workerCount int

	putBlockChan           chan putBlockMessage
	getBlockChan           chan getBlockMessage
	preflightGetChan       chan preflightGetMessage
	prefetchBlockChan      chan prefetchBlockMessage
	deleteBlockChan        chan deleteBlockMessage
	blockIndexChan         chan blockIndexMessage
	getExistingContentChan chan getExistingContentMessage
	pruneBlocksChan        chan pruneBlocksMessage
	workerFlushChan        chan int
	workerFlushReplyChan   chan int
	indexFlushChan         chan int
	indexFlushReplyChan    chan int
	workerErrorChan        chan error
	prefetchMemory         int64
	maxPrefetchMemory      int64

	fetchedBlocksSync sync.Mutex
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
	blobClient longtailstorelib.BlobClient,
	blockIndexMessages chan<- blockIndexMessage,
	storedBlock longtaillib.Longtail_StoredBlock) error {
	const fname = "putStoredBlock"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"s":     s,
	})
	log.Debug(fname)

	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count], 1)

	blockIndex := storedBlock.GetBlockIndex()
	blockHash := blockIndex.GetBlockHash()
	key := getBlockPath("chunks", blockHash)

	log = logrus.WithFields(logrus.Fields{
		"blockHash": blockHash,
		"key":       key,
	})

	objHandle, err := blobClient.NewObject(key)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	if exists, err := objHandle.Exists(); err == nil && !exists {
		blob, errno := longtaillib.WriteStoredBlockToBuffer(storedBlock)
		if errno != 0 {
			return longtaillib.ErrnoToError(errno, longtaillib.ErrEIO)
		}

		ok, err := objHandle.Write(blob)
		if err != nil || !ok {
			log.Warning("Retrying putBlob")
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount], 1)
			ok, err = objHandle.Write(blob)
		}
		if err != nil || !ok {
			log.Warning("Retrying 500 ms delayed putBlob")
			time.Sleep(500 * time.Millisecond)
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount], 1)
			ok, err = objHandle.Write(blob)
		}
		if err != nil || !ok {
			log.Warning("Retrying 2 s delayed putBlob")
			time.Sleep(2 * time.Second)
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount], 1)
			ok, err = objHandle.Write(blob)
		}

		if err != nil || !ok {
			atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount], 1)
			err := longtailutils.MakeError(errno, fmt.Sprintf("Failed to put stored block at `%s` in `%s`", key, s))
			return errors.Wrap(err, fname)
		}

		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count], (uint64)(len(blob)))
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count], (uint64)(blockIndex.GetChunkCount()))
	}

	blockIndexCopy, err := blockIndex.Copy()
	if err != nil {
		return errors.Wrap(err, fname)
	}
	blockIndexMessages <- blockIndexMessage{blockIndex: blockIndexCopy}
	return nil
}

func getStoredBlock(
	ctx context.Context,
	s *remoteStore,
	blobClient longtailstorelib.BlobClient,
	blockHash uint64) (longtaillib.Longtail_StoredBlock, error) {
	const fname = "getStoredBlock"
	log := logrus.WithFields(logrus.Fields{
		"fname":     fname,
		"s":         s,
		"blockHash": blockHash,
	})
	log.Debug(fname)

	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count], 1)

	key := getBlockPath("chunks", blockHash)

	storedBlockData, retryCount, err := longtailutils.ReadBlobWithRetry(ctx, blobClient, key)
	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_RetryCount], uint64(retryCount))

	if err != nil || storedBlockData == nil {
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1)
		return longtaillib.Longtail_StoredBlock{}, errors.Wrap(err, fname)
	}

	storedBlock, errno := longtaillib.ReadStoredBlockFromBuffer(storedBlockData)
	if errno != 0 {
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1)
		err = longtailutils.MakeError(errno, fmt.Sprintf("Failed to parse stored block `%s`", key))
		return longtaillib.Longtail_StoredBlock{}, errors.Wrap(err, fname)
	}

	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count], (uint64)(len(storedBlockData)))
	blockIndex := storedBlock.GetBlockIndex()
	if blockIndex.GetBlockHash() != blockHash {
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1)
		err = longtailutils.MakeError(longtaillib.EBADF, "Block hash does not path")
		return longtaillib.Longtail_StoredBlock{}, errors.Wrap(err, fname)
	}
	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count], (uint64)(blockIndex.GetChunkCount()))
	return storedBlock, nil
}

func fetchBlock(
	ctx context.Context,
	s *remoteStore,
	client longtailstorelib.BlobClient,
	getMsg getBlockMessage) {
	const fname = "fetchBlock"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"s":      s,
		"getMsg": getMsg,
	})
	log.Debug(fname)
	s.fetchedBlocksSync.Lock()
	prefetchedBlock := s.prefetchBlocks[getMsg.blockHash]
	if prefetchedBlock != nil {
		storedBlock := prefetchedBlock.storedBlock
		if storedBlock.IsValid() {
			s.prefetchBlocks[getMsg.blockHash] = nil
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
	s.fetchedBlocksSync.Unlock()
	storedBlock, getStoredBlockErr := getStoredBlock(ctx, s, client, getMsg.blockHash)
	s.fetchedBlocksSync.Lock()
	prefetchedBlock, exists := s.prefetchBlocks[getMsg.blockHash]
	if exists && prefetchedBlock == nil {
		storedBlock.Dispose()
		s.fetchedBlocksSync.Unlock()
		return
	}
	completeCallbacks := prefetchedBlock.completeCallbacks
	s.prefetchBlocks[getMsg.blockHash] = nil
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

func deleteBlock(
	ctx context.Context,
	s *remoteStore,
	client longtailstorelib.BlobClient,
	blockHash uint64) error {
	const fname = "deleteBlock"
	log := logrus.WithFields(logrus.Fields{
		"fname":     fname,
		"s":         s,
		"blockHash": blockHash,
	})
	log.Debug(fname)
	s.fetchedBlocksSync.Lock()
	key := getBlockPath("chunks", blockHash)
	objHandle, err := client.NewObject(key)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	err = objHandle.Delete()
	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

func prefetchBlock(
	ctx context.Context,
	s *remoteStore,
	client longtailstorelib.BlobClient,
	prefetchMsg prefetchBlockMessage) {
	const fname = "prefetchBlock"
	log := logrus.WithFields(logrus.Fields{
		"fname":       fname,
		"s":           s,
		"prefetchMsg": prefetchMsg,
	})
	log.Debug(fname)
	s.fetchedBlocksSync.Lock()
	_, exists := s.prefetchBlocks[prefetchMsg.blockHash]
	if exists {
		// Already pre-fetched
		s.fetchedBlocksSync.Unlock()
		return
	}
	prefetchedBlock := &pendingPrefetchedBlock{storedBlock: longtaillib.Longtail_StoredBlock{}}
	s.prefetchBlocks[prefetchMsg.blockHash] = prefetchedBlock
	s.fetchedBlocksSync.Unlock()

	storedBlock, getErr := getStoredBlock(ctx, s, client, prefetchMsg.blockHash)
	if getErr != nil {
		log.WithError(getErr).Error("Failed to get block")
		return
	}

	s.fetchedBlocksSync.Lock()

	prefetchedBlock, exists = s.prefetchBlocks[prefetchMsg.blockHash]
	if prefetchedBlock == nil {
		storedBlock.Dispose()
		s.fetchedBlocksSync.Unlock()
		return
	}
	completeCallbacks := prefetchedBlock.completeCallbacks
	if len(completeCallbacks) == 0 {
		// Nobody is actively waiting for the block
		blockSize := int64(storedBlock.GetBlockSize())
		prefetchedBlock.storedBlock = storedBlock
		atomic.AddInt64(&s.prefetchMemory, blockSize)
		s.fetchedBlocksSync.Unlock()
		return
	}
	s.prefetchBlocks[prefetchMsg.blockHash] = nil
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

func flushPrefetch(
	s *remoteStore,
	prefetchBlockChan <-chan prefetchBlockMessage) {
	const fname = "flushPrefetch"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"s":     s,
	})
	log.Debug(fname)

L:
	for {
		select {
		case <-prefetchBlockChan:
		default:
			break L
		}
	}

	s.fetchedBlocksSync.Lock()
	flushBlocks := []uint64{}
	for k, v := range s.prefetchBlocks {
		if v != nil && len(v.completeCallbacks) > 0 {
			log.WithField("blockHash", k).Debug("Somebody is still waiting for prefetch")
			continue
		}
		flushBlocks = append(flushBlocks, k)
	}
	for _, h := range flushBlocks {
		b := s.prefetchBlocks[h]
		if b != nil {
			if b.storedBlock.IsValid() {
				blockSize := -int64(b.storedBlock.GetBlockSize())
				atomic.AddInt64(&s.prefetchMemory, blockSize)
				b.storedBlock.Dispose()
			}
		}
		delete(s.prefetchBlocks, h)
	}
	s.fetchedBlocksSync.Unlock()
}

func remoteWorker(
	ctx context.Context,
	s *remoteStore,
	putBlockMessages <-chan putBlockMessage,
	getBlockMessages <-chan getBlockMessage,
	prefetchBlockChan <-chan prefetchBlockMessage,
	deleteBlocksChan <-chan deleteBlockMessage,
	blockIndexMessages chan<- blockIndexMessage,
	flushMessages <-chan int,
	flushReplyMessages chan<- int,
	accessType AccessType) error {
	const fname = "remoteWorker"
	log := logrus.WithFields(logrus.Fields{
		"fname":      fname,
		"s":          s,
		"accessType": accessType,
	})
	log.Debug(fname)
	client, err := s.blobStore.NewClient(ctx)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	defer client.Close()
	run := true
	for run {
		received := 0
		select {
		case putMsg, more := <-putBlockMessages:
			if more {
				received++
				if accessType == ReadOnly {
					putMsg.asyncCompleteAPI.OnComplete(longtaillib.EACCES)
					continue
				}
				err := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
				putMsg.asyncCompleteAPI.OnComplete(longtaillib.ErrorToErrno(err, longtaillib.EIO))
			} else {
				run = false
			}
		case getMsg := <-getBlockMessages:
			received++
			fetchBlock(ctx, s, client, getMsg)
		case deleteMsg := <-deleteBlocksChan:
			received++
			err := deleteBlock(ctx, s, client, deleteMsg.blockHash)
			if err == nil {
				atomic.AddUint32(deleteMsg.successCounter, 1)
			}
			deleteMsg.completeSignal.Done()
		default:
		}
		if received == 0 {
			if s.prefetchMemory < s.maxPrefetchMemory {
				select {
				case <-flushMessages:
					flushPrefetch(s, prefetchBlockChan)
					flushReplyMessages <- 0
				case putMsg, more := <-putBlockMessages:
					if more {
						if accessType == ReadOnly {
							putMsg.asyncCompleteAPI.OnComplete(longtaillib.EACCES)
							continue
						}
						err := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
						putMsg.asyncCompleteAPI.OnComplete(longtaillib.ErrorToErrno(err, longtaillib.EIO))
					} else {
						run = false
					}
				case getMsg := <-getBlockMessages:
					fetchBlock(ctx, s, client, getMsg)
				case prefetchMsg := <-prefetchBlockChan:
					prefetchBlock(ctx, s, client, prefetchMsg)
				case deleteMsg := <-deleteBlocksChan:
					err := deleteBlock(ctx, s, client, deleteMsg.blockHash)
					if err == nil {
						atomic.AddUint32(deleteMsg.successCounter, 1)
					}
					deleteMsg.completeSignal.Done()
				}
			} else {
				select {
				case <-flushMessages:
					flushPrefetch(s, prefetchBlockChan)
					flushReplyMessages <- 0
				case putMsg, more := <-putBlockMessages:
					if more {
						if accessType == ReadOnly {
							putMsg.asyncCompleteAPI.OnComplete(longtaillib.EACCES)
							continue
						}
						err := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
						putMsg.asyncCompleteAPI.OnComplete(longtaillib.ErrorToErrno(err, longtaillib.EIO))
					} else {
						run = false
					}
				case getMsg := <-getBlockMessages:
					fetchBlock(ctx, s, client, getMsg)
				case deleteMsg := <-deleteBlocksChan:
					err := deleteBlock(ctx, s, client, deleteMsg.blockHash)
					if err == nil {
						atomic.AddUint32(deleteMsg.successCounter, 1)
					}
					deleteMsg.completeSignal.Done()
				}
			}
		}
	}

	flushPrefetch(s, prefetchBlockChan)
	return nil
}

func addBlocksToRemoteStoreIndex(
	ctx context.Context,
	s *remoteStore,
	blobClient longtailstorelib.BlobClient,
	addedBlockIndexes []longtaillib.Longtail_BlockIndex) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "addBlocksToRemoteStoreIndex"
	log := logrus.WithFields(logrus.Fields{
		"fname":                  fname,
		"s":                      s,
		"len(addedBlockIndexes)": addedBlockIndexes,
	})
	log.Debug(fname)

	addedStoreIndex, errno := longtaillib.CreateStoreIndexFromBlocks(addedBlockIndexes)
	if errno != 0 {
		err := longtailutils.MakeError(errno, "Failed to create store index from block indexes")
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	defer addedStoreIndex.Dispose()
	return addToRemoteStoreIndex(ctx, blobClient, addedStoreIndex)
}

func storeIndexWorkerReplyErrorState(
	blockIndexMessages <-chan blockIndexMessage,
	getExistingContentMessages <-chan getExistingContentMessage,
	pruneBlocksMessages <-chan pruneBlocksMessage,
	flushMessages <-chan int,
	flushReplyMessages chan<- int) {
	const fname = "storeIndexWorkerReplyErrorState"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)
	for {
		select {
		case <-flushMessages:
			flushReplyMessages <- 0
		case _, more := <-blockIndexMessages:
			if !more {
				return
			}
		case getExistingContentMessage := <-getExistingContentMessages:
			getExistingContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_StoreIndex{}, longtaillib.EINVAL)
		case pruneBlocksMessage := <-pruneBlocksMessages:
			pruneBlocksMessage.asyncCompleteAPI.OnComplete(0, longtaillib.EINVAL)
		}
	}
}

func onPreflighMessage(
	s *remoteStore,
	storeIndex longtaillib.Longtail_StoreIndex,
	message preflightGetMessage,
	prefetchBlockMessages chan<- prefetchBlockMessage) {
	const fname = "onPreflighMessage"
	log := logrus.WithFields(logrus.Fields{
		"fname":   fname,
		"s":       s,
		"message": message,
	})
	log.Debug(fname)

	for _, blockHash := range message.blockHashes {
		prefetchBlockMessages <- prefetchBlockMessage{blockHash: blockHash}
	}
	message.asyncCompleteAPI.OnComplete(message.blockHashes, 0)
}

func onGetExistingContentMessage(
	s *remoteStore,
	storeIndex longtaillib.Longtail_StoreIndex,
	message getExistingContentMessage) {
	const fname = "onGetExistingContentMessage"
	log := logrus.WithFields(logrus.Fields{
		"fname":   fname,
		"s":       s,
		"message": message,
	})
	log.Debug(fname)

	existingStoreIndex, errno := longtaillib.GetExistingStoreIndex(storeIndex, message.chunkHashes, message.minBlockUsagePercent)
	if errno != 0 {
		message.asyncCompleteAPI.OnComplete(longtaillib.Longtail_StoreIndex{}, errno)
		return
	}
	message.asyncCompleteAPI.OnComplete(existingStoreIndex, 0)
}

func onPruneBlocksMessage(
	ctx context.Context,
	s *remoteStore,
	blobClient longtailstorelib.BlobClient,
	storeIndex longtaillib.Longtail_StoreIndex,
	keepBlockHashes []uint64) (uint32, longtaillib.Longtail_StoreIndex, int) {
	const fname = "onPruneBlocksMessage"
	log := logrus.WithFields(logrus.Fields{
		"fname":                fname,
		"s":                    s,
		"len(keepBlockHashes)": len(keepBlockHashes),
	})
	log.Debug(fname)

	prunedIndex, errno := longtaillib.PruneStoreIndex(storeIndex, keepBlockHashes)
	if errno != 0 {
		return 0, longtaillib.Longtail_StoreIndex{}, errno
	}

	err := tryOverwriteStoreIndexWithRetry(ctx, prunedIndex, blobClient)
	if err != nil {
		prunedIndex.Dispose()
		return 0, longtaillib.Longtail_StoreIndex{}, longtaillib.ErrorToErrno(err, longtaillib.EIO)
	}

	keptBlockHashes := prunedIndex.GetBlockHashes()
	keptBlocksMap := make(map[uint64]bool)
	for _, blockHash := range keptBlockHashes {
		keptBlocksMap[blockHash] = true
	}

	var wg sync.WaitGroup

	prunedCount := uint32(0)
	existingBlockHashes := storeIndex.GetBlockHashes()
	for _, blockHash := range existingBlockHashes {
		if _, exists := keptBlocksMap[blockHash]; exists {
			continue
		}
		wg.Add(1)
		s.deleteBlockChan <- deleteBlockMessage{blockHash: blockHash, completeSignal: &wg, successCounter: &prunedCount}
	}
	wg.Wait()
	return prunedCount, prunedIndex, 0
}

func getCurrentStoreIndex(
	ctx context.Context,
	s *remoteStore,
	optionalStoreIndexPath string,
	client longtailstorelib.BlobClient,
	accessType AccessType,
	storeIndex longtaillib.Longtail_StoreIndex,
	addedBlockIndexes []longtaillib.Longtail_BlockIndex) (longtaillib.Longtail_StoreIndex, longtaillib.Longtail_StoreIndex, error) {
	const fname = "getCurrentStoreIndex"
	log := logrus.WithFields(logrus.Fields{
		"fname":                  fname,
		"s":                      s,
		"optionalStoreIndexPath": optionalStoreIndexPath,
		"accessType":             accessType,
		"len(addedBlockIndexes)": len(addedBlockIndexes),
	})
	log.Debug(fname)
	var err error = nil
	if !storeIndex.IsValid() {
		storeIndex, err = readRemoteStoreIndex(ctx, optionalStoreIndexPath, s.blobStore, client, accessType, s.workerCount)
		if err != nil {
			return storeIndex, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
	}
	if len(addedBlockIndexes) == 0 {
		return storeIndex, longtaillib.Longtail_StoreIndex{}, nil
	}
	updatedStoreIndex, err := addBlocksToStoreIndex(storeIndex, addedBlockIndexes)
	if err != nil {
		log.Printf("WARNING: Failed to update store index with added blocks %v", err)
		return storeIndex, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	return storeIndex, updatedStoreIndex, nil
}

func contentIndexWorker(
	ctx context.Context,
	s *remoteStore,
	optionalStoreIndexPath string,
	preflightGetMessages <-chan preflightGetMessage,
	prefetchBlockMessages chan<- prefetchBlockMessage,
	blockIndexMessages <-chan blockIndexMessage,
	getExistingContentMessages <-chan getExistingContentMessage,
	pruneBlocksMessages <-chan pruneBlocksMessage,
	flushMessages <-chan int,
	flushReplyMessages chan<- int,
	accessType AccessType) error {
	const fname = "contentIndexWorker"
	log := logrus.WithFields(logrus.Fields{
		"fname":                  fname,
		"s":                      s,
		"optionalStoreIndexPath": optionalStoreIndexPath,
		"accessType":             accessType,
	})
	log.Debug(fname)
	client, err := s.blobStore.NewClient(ctx)
	if err != nil {
		storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, pruneBlocksMessages, flushMessages, flushReplyMessages)
		return errors.Wrap(err, fname)
	}
	defer client.Close()

	storeIndex := longtaillib.Longtail_StoreIndex{}
	updatedStoreIndex := longtaillib.Longtail_StoreIndex{}

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
			received++
			storeIndex, updatedStoreIndex, err = getCurrentStoreIndex(ctx, s, optionalStoreIndexPath, client, accessType, storeIndex, addedBlockIndexes)
			if err != nil {
				storeIndex.Dispose()
				preflightGetMsg.asyncCompleteAPI.OnComplete([]uint64{}, longtaillib.ErrorToErrno(err, longtaillib.EIO))
				storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, pruneBlocksMessages, flushMessages, flushReplyMessages)
				return errors.Wrap(err, fname)
			}
			if updatedStoreIndex.IsValid() {
				onPreflighMessage(s, updatedStoreIndex, preflightGetMsg, prefetchBlockMessages)
				updatedStoreIndex.Dispose()
			} else {
				onPreflighMessage(s, storeIndex, preflightGetMsg, prefetchBlockMessages)
			}
		case blockIndexMsg, more := <-blockIndexMessages:
			if more {
				received++
				addedBlockIndexes = append(addedBlockIndexes, blockIndexMsg.blockIndex)
			} else {
				run = false
			}
		case getExistingContentMessage := <-getExistingContentMessages:
			received++
			storeIndex, updatedStoreIndex, err = getCurrentStoreIndex(ctx, s, optionalStoreIndexPath, client, accessType, storeIndex, addedBlockIndexes)
			if err != nil {
				storeIndex.Dispose()
				getExistingContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_StoreIndex{}, longtaillib.ErrorToErrno(err, longtaillib.EIO))
				storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, pruneBlocksMessages, flushMessages, flushReplyMessages)
				return errors.Wrap(err, fname)
			}
			if updatedStoreIndex.IsValid() {
				onGetExistingContentMessage(s, updatedStoreIndex, getExistingContentMessage)
				updatedStoreIndex.Dispose()
			} else {
				onGetExistingContentMessage(s, storeIndex, getExistingContentMessage)
			}
		case pruneBlocksMessage := <-pruneBlocksMessages:
			if accessType == ReadOnly {
				pruneBlocksMessage.asyncCompleteAPI.OnComplete(0, longtaillib.EACCES)
				continue
			}
			received++
			storeIndex, updatedStoreIndex, err = getCurrentStoreIndex(ctx, s, optionalStoreIndexPath, client, accessType, storeIndex, addedBlockIndexes)
			if err != nil {
				storeIndex.Dispose()
				pruneBlocksMessage.asyncCompleteAPI.OnComplete(0, longtaillib.ErrorToErrno(err, longtaillib.EIO))
				storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, pruneBlocksMessages, flushMessages, flushReplyMessages)
				return errors.Wrap(err, fname)
			}
			errno := 0
			prunedCount := uint32(0)
			prunedStoreIndex := longtaillib.Longtail_StoreIndex{}
			if updatedStoreIndex.IsValid() {
				prunedCount, prunedStoreIndex, errno = onPruneBlocksMessage(ctx, s, client, updatedStoreIndex, pruneBlocksMessage.keepBlockHashes)
				updatedStoreIndex.Dispose()
			} else {
				prunedCount, prunedStoreIndex, errno = onPruneBlocksMessage(ctx, s, client, storeIndex, pruneBlocksMessage.keepBlockHashes)
			}
			if prunedStoreIndex.IsValid() {
				storeIndex.Dispose()
				storeIndex = prunedStoreIndex
			}
			pruneBlocksMessage.asyncCompleteAPI.OnComplete(prunedCount, errno)
		default:
		}

		if received > 0 {
			continue
		}

		select {
		case <-flushMessages:
			if len(addedBlockIndexes) > 0 && accessType != ReadOnly {
				newStoreIndex, err := addBlocksToRemoteStoreIndex(ctx, s, client, addedBlockIndexes)
				if err != nil {
					flushReplyMessages <- longtaillib.ErrorToErrno(err, longtaillib.ENOMEM)
					continue
				}
				addedBlockIndexes = nil
				if newStoreIndex.IsValid() {
					storeIndex.Dispose()
					storeIndex = newStoreIndex
				}
			}
			flushReplyMessages <- 0
		case preflightGetMsg := <-preflightGetMessages:
			storeIndex, updatedStoreIndex, err = getCurrentStoreIndex(ctx, s, optionalStoreIndexPath, client, accessType, storeIndex, addedBlockIndexes)
			if err != nil {
				storeIndex.Dispose()
				preflightGetMsg.asyncCompleteAPI.OnComplete([]uint64{}, longtaillib.ErrorToErrno(err, longtaillib.EIO))
				storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, pruneBlocksMessages, flushMessages, flushReplyMessages)
				return errors.Wrap(err, fname)
			}
			if updatedStoreIndex.IsValid() {
				onPreflighMessage(s, updatedStoreIndex, preflightGetMsg, prefetchBlockMessages)
				updatedStoreIndex.Dispose()
			} else {
				onPreflighMessage(s, storeIndex, preflightGetMsg, prefetchBlockMessages)
			}
		case blockIndexMsg, more := <-blockIndexMessages:
			if more {
				addedBlockIndexes = append(addedBlockIndexes, blockIndexMsg.blockIndex)
			} else {
				run = false
			}
		case getExistingContentMessage := <-getExistingContentMessages:
			storeIndex, updatedStoreIndex, err = getCurrentStoreIndex(ctx, s, optionalStoreIndexPath, client, accessType, storeIndex, addedBlockIndexes)
			if err != nil {
				storeIndex.Dispose()
				getExistingContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_StoreIndex{}, longtaillib.ErrorToErrno(err, longtaillib.EIO))
				storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, pruneBlocksMessages, flushMessages, flushReplyMessages)
				return errors.Wrap(err, fname)
			}
			if updatedStoreIndex.IsValid() {
				onGetExistingContentMessage(s, updatedStoreIndex, getExistingContentMessage)
				updatedStoreIndex.Dispose()
			} else {
				onGetExistingContentMessage(s, storeIndex, getExistingContentMessage)
			}
		case pruneBlocksMessage := <-pruneBlocksMessages:
			if accessType == ReadOnly {
				pruneBlocksMessage.asyncCompleteAPI.OnComplete(0, longtaillib.EACCES)
				continue
			}
			storeIndex, updatedStoreIndex, err = getCurrentStoreIndex(ctx, s, optionalStoreIndexPath, client, accessType, storeIndex, addedBlockIndexes)
			if err != nil {
				storeIndex.Dispose()
				pruneBlocksMessage.asyncCompleteAPI.OnComplete(0, longtaillib.ErrorToErrno(err, longtaillib.EIO))
				storeIndexWorkerReplyErrorState(blockIndexMessages, getExistingContentMessages, pruneBlocksMessages, flushMessages, flushReplyMessages)
				return errors.Wrap(err, fname)
			}
			errno := 0
			prunedCount := uint32(0)
			prunedStoreIndex := longtaillib.Longtail_StoreIndex{}
			if updatedStoreIndex.IsValid() {
				prunedCount, prunedStoreIndex, errno = onPruneBlocksMessage(ctx, s, client, updatedStoreIndex, pruneBlocksMessage.keepBlockHashes)
				updatedStoreIndex.Dispose()
			} else {
				prunedCount, prunedStoreIndex, errno = onPruneBlocksMessage(ctx, s, client, storeIndex, pruneBlocksMessage.keepBlockHashes)
			}
			if prunedStoreIndex.IsValid() {
				storeIndex.Dispose()
				storeIndex = prunedStoreIndex
			}
			pruneBlocksMessage.asyncCompleteAPI.OnComplete(prunedCount, errno)
		}
	}

	if accessType == ReadOnly {
		storeIndex.Dispose()
		return nil
	}

	if accessType == Init || len(addedBlockIndexes) > 0 {
		newIndex, err := addBlocksToRemoteStoreIndex(ctx, s, client, addedBlockIndexes)
		storeIndex.Dispose()
		if err != nil {
			return errors.Wrap(err, fname)
		}
		newIndex.Dispose()
	}
	return nil
}

// NewRemoteBlockStore ...
func NewRemoteBlockStore(
	jobAPI longtaillib.Longtail_JobAPI,
	blobStore longtailstorelib.BlobStore,
	optionalStoreIndexPath string,
	workerCount int,
	accessType AccessType) (longtaillib.BlockStoreAPI, error) {
	const fname = "NewRemoteBlockStore"
	log := logrus.WithFields(logrus.Fields{
		"fname":                  fname,
		"optionalStoreIndexPath": optionalStoreIndexPath,
		"workerCount":            workerCount,
		"accessType":             accessType,
	})
	log.Debug(fname)
	ctx := context.Background()
	defaultClient, err := blobStore.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}

	s := &remoteStore{
		jobAPI:        jobAPI,
		blobStore:     blobStore,
		defaultClient: defaultClient}

	s.workerCount = workerCount
	s.putBlockChan = make(chan putBlockMessage, s.workerCount*8)
	s.getBlockChan = make(chan getBlockMessage, s.workerCount*2048)
	s.prefetchBlockChan = make(chan prefetchBlockMessage, s.workerCount*2048)
	s.deleteBlockChan = make(chan deleteBlockMessage, s.workerCount*8)
	s.preflightGetChan = make(chan preflightGetMessage, 16)
	s.blockIndexChan = make(chan blockIndexMessage, s.workerCount*2048)
	s.getExistingContentChan = make(chan getExistingContentMessage, 16)
	s.pruneBlocksChan = make(chan pruneBlocksMessage, 1)
	s.workerFlushChan = make(chan int, s.workerCount)
	s.workerFlushReplyChan = make(chan int, s.workerCount)
	s.indexFlushChan = make(chan int, 1)
	s.indexFlushReplyChan = make(chan int, 1)
	s.workerErrorChan = make(chan error, 1+s.workerCount)

	s.prefetchMemory = 0
	s.maxPrefetchMemory = 512 * 1024 * 1024

	s.prefetchBlocks = map[uint64]*pendingPrefetchedBlock{}

	go func() {
		err := contentIndexWorker(
			ctx,
			s,
			optionalStoreIndexPath,
			s.preflightGetChan,
			s.prefetchBlockChan,
			s.blockIndexChan,
			s.getExistingContentChan,
			s.pruneBlocksChan,
			s.indexFlushChan,
			s.indexFlushReplyChan,
			accessType)
		s.workerErrorChan <- errors.Wrap(err, fname)
	}()

	for i := 0; i < s.workerCount; i++ {
		go func() {
			err := remoteWorker(ctx,
				s,
				s.putBlockChan,
				s.getBlockChan,
				s.prefetchBlockChan,
				s.deleteBlockChan,
				s.blockIndexChan,
				s.workerFlushChan,
				s.workerFlushReplyChan,
				accessType)
			s.workerErrorChan <- errors.Wrap(err, fname)
		}()
	}

	return s, nil
}

// PutStoredBlock ...
func (s *remoteStore) PutStoredBlock(storedBlock longtaillib.Longtail_StoredBlock, asyncCompleteAPI longtaillib.Longtail_AsyncPutStoredBlockAPI) int {
	s.putBlockChan <- putBlockMessage{storedBlock: storedBlock, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// PreflightGet ...
func (s *remoteStore) PreflightGet(blockHashes []uint64, asyncCompleteAPI longtaillib.Longtail_AsyncPreflightStartedAPI) int {
	s.preflightGetChan <- preflightGetMessage{blockHashes: blockHashes, asyncCompleteAPI: asyncCompleteAPI}
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

// PruneBlocks ...
func (s *remoteStore) PruneBlocks(
	keepBlockHashes []uint64,
	asyncCompleteAPI longtaillib.Longtail_AsyncPruneBlocksAPI) int {
	s.pruneBlocksChan <- pruneBlocksMessage{keepBlockHashes: keepBlockHashes, asyncCompleteAPI: asyncCompleteAPI}
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
			errno := <-s.workerFlushReplyChan
			if errno != 0 && any_errno == 0 {
				any_errno = errno
			}
		}
		s.indexFlushChan <- 1
		errno := <-s.indexFlushReplyChan
		if errno != 0 && any_errno == 0 {
			any_errno = errno
		}
		asyncCompleteAPI.OnComplete(any_errno)
	}()
	return 0
}

// Close ...
func (s *remoteStore) Close() {
	close(s.putBlockChan)
	for i := 0; i < s.workerCount; i++ {
		err := <-s.workerErrorChan
		if err != nil {
			log.Fatal(err)
		}
	}
	close(s.blockIndexChan)
	err := <-s.workerErrorChan
	if err != nil {
		log.Fatal(err)
	}

	s.defaultClient.Close()
}

func tryAddRemoteStoreIndexWithLocking(
	ctx context.Context,
	addStoreIndex longtaillib.Longtail_StoreIndex,
	blobClient longtailstorelib.BlobClient) (bool, longtaillib.Longtail_StoreIndex, error) {
	const fname = "NewRemoteBlockStore"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)

	key := "store.lsi"
	objHandle, err := blobClient.NewObject(key)
	if err != nil {
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	exists, err := objHandle.LockWriteVersion()
	if err != nil {
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	if exists {
		blob, err := objHandle.Read()
		if err != nil {
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}

		remoteStoreIndex, errno := longtaillib.ReadStoreIndexFromBuffer(blob)
		if errno != 0 {
			err = longtailutils.MakeError(errno, fmt.Sprintf("Cant parse store index from `%s`", key))
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		defer remoteStoreIndex.Dispose()

		newStoreIndex, errno := longtaillib.MergeStoreIndex(remoteStoreIndex, addStoreIndex)
		if errno != 0 {
			err = longtailutils.MakeError(errno, fmt.Sprintf("Failed merging store index for `%s`", key))
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}

		storeBlob, errno := longtaillib.WriteStoreIndexToBuffer(newStoreIndex)
		if errno != 0 {
			newStoreIndex.Dispose()
			err = longtailutils.MakeError(errno, fmt.Sprintf("Failed serializing store index for `%s`", key))
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}

		ok, err := objHandle.Write(storeBlob)
		if err != nil {
			newStoreIndex.Dispose()
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		if !ok {
			newStoreIndex.Dispose()
			return false, longtaillib.Longtail_StoreIndex{}, nil
		}
		return ok, newStoreIndex, nil
	}
	storeBlob, errno := longtaillib.WriteStoreIndexToBuffer(addStoreIndex)
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Failed serializing store index for `%s`", key))
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	ok, err := objHandle.Write(storeBlob)
	if err != nil {
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	return ok, longtaillib.Longtail_StoreIndex{}, nil
}

func tryWriteRemoteStoreIndex(
	ctx context.Context,
	storeIndex longtaillib.Longtail_StoreIndex,
	existingIndexItems []string,
	blobClient longtailstorelib.BlobClient) (bool, error) {
	const fname = "tryWriteRemoteStoreIndex"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)

	storeBlob, errno := longtaillib.WriteStoreIndexToBuffer(storeIndex)
	if errno != 0 {
		err := longtailutils.MakeError(errno, fmt.Sprintf("Failed serializing store index"))
		return false, errors.Wrap(err, fname)
	}

	sha256 := sha256.Sum256(storeBlob)
	key := fmt.Sprintf("store_%x.lsi", sha256)

	for _, item := range existingIndexItems {
		if item == key {
			return true, nil
		}
	}

	objHandle, err := blobClient.NewObject(key)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	exists, err := objHandle.Exists()
	if err != nil {
		return false, errors.Wrap(err, fname)
	}
	if exists {
		return false, nil
	}

	ok, err := objHandle.Write(storeBlob)
	if !ok || err != nil {
		return ok, errors.Wrap(err, fname)
	}

	for _, item := range existingIndexItems {
		objHandle, err := blobClient.NewObject(item)
		if err != nil {
			continue
		}
		err = objHandle.Delete()
		if err != nil {
			continue
		}
	}

	return true, nil
}

func tryAddRemoteStoreIndex(
	ctx context.Context,
	addStoreIndex longtaillib.Longtail_StoreIndex,
	blobClient longtailstorelib.BlobClient) (bool, longtaillib.Longtail_StoreIndex, error) {
	const fname = "tryAddRemoteStoreIndex"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)

	if blobClient.SupportsLocking() {
		return tryAddRemoteStoreIndexWithLocking(ctx, addStoreIndex, blobClient)
	}

	storeIndex, items, err := readStoreStoreIndexWithItems(ctx, blobClient)
	if err != nil {
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	if !storeIndex.IsValid() {
		ok, err := tryWriteRemoteStoreIndex(ctx, addStoreIndex, items, blobClient)
		return ok, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	mergedStoreIndex, errno := longtaillib.MergeStoreIndex(storeIndex, addStoreIndex)
	storeIndex.Dispose()
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Failed merging store index for"))
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	ok, err := tryWriteRemoteStoreIndex(ctx, mergedStoreIndex, items, blobClient)
	if err != nil {
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	return ok, mergedStoreIndex, nil
}

func addToRemoteStoreIndex(
	ctx context.Context,
	blobClient longtailstorelib.BlobClient,
	addStoreIndex longtaillib.Longtail_StoreIndex) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "addToRemoteStoreIndex"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)

	errorRetries := 0
	for {
		ok, newStoreIndex, err := tryAddRemoteStoreIndex(
			ctx,
			addStoreIndex,
			blobClient)
		if ok {
			return newStoreIndex, nil
		}
		if err != nil {
			errorRetries++
			if errorRetries == 3 {
				return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			} else {
				log.Printf("Error from tryAddRemoteStoreIndex %q\n", err)
			}
		}
		log.Printf("Retrying updating remote store index\n")
	}
	return longtaillib.Longtail_StoreIndex{}, nil
}

func tryOverwriteRemoteStoreIndexWithLocking(
	ctx context.Context,
	storeIndex longtaillib.Longtail_StoreIndex,
	client longtailstorelib.BlobClient) (bool, error) {
	const fname = "tryOverwriteRemoteStoreIndexWithLocking"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)

	storeBlob, errno := longtaillib.WriteStoreIndexToBuffer(storeIndex)
	if errno != 0 {
		err := longtailutils.MakeError(errno, "Failed serializing store index")
		return false, errors.Wrap(err, fname)
	}

	key := "store.lsi"
	objHandle, err := client.NewObject(key)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	_, err = objHandle.LockWriteVersion()
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	ok, err := objHandle.Write(storeBlob)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}
	return ok, nil
}

func tryOverwriteRemoteStoreIndexWithoutLocking(
	ctx context.Context,
	storeIndex longtaillib.Longtail_StoreIndex,
	client longtailstorelib.BlobClient) (bool, error) {
	const fname = "tryOverwriteRemoteStoreIndexWithoutLocking"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)

	items, err := getStoreStoreIndexes(ctx, client)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	storeBlob, errno := longtaillib.WriteStoreIndexToBuffer(storeIndex)
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Failed serializing store index"))
		return false, errors.Wrap(err, fname)
	}

	sha256 := sha256.Sum256(storeBlob)
	key := fmt.Sprintf("store_%x.lsi", sha256)

	objHandle, err := client.NewObject(key)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	exists, err := objHandle.Exists()
	if err != nil {
		return false, errors.Wrap(err, fname)
	}
	if !exists {
		ok, err := objHandle.Write(storeBlob)
		if !ok || err != nil {
			return ok, errors.Wrap(err, fname)
		}
	}

	for _, item := range items {
		if item == key {
			continue
		}
		objHandle, err := client.NewObject(item)
		if err != nil {
			continue
		}
		err = objHandle.Delete()
		if err != nil {
			continue
		}
	}

	return true, nil
}

func tryOverwriteRemoteStoreIndex(
	ctx context.Context,
	storeIndex longtaillib.Longtail_StoreIndex,
	blobClient longtailstorelib.BlobClient) (bool, error) {
	const fname = "tryOverwriteRemoteStoreIndex"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)

	if blobClient.SupportsLocking() {
		return tryOverwriteRemoteStoreIndexWithLocking(ctx, storeIndex, blobClient)
	}
	return tryOverwriteRemoteStoreIndexWithoutLocking(ctx, storeIndex, blobClient)
}

func tryOverwriteStoreIndexWithRetry(
	ctx context.Context,
	storeIndex longtaillib.Longtail_StoreIndex,
	blobClient longtailstorelib.BlobClient) error {
	const fname = "tryOverwriteStoreIndexWithRetry"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)

	errorRetries := 0
	for {
		ok, err := tryOverwriteRemoteStoreIndex(
			ctx,
			storeIndex,
			blobClient)
		if ok {
			return nil
		}
		if err != nil {
			errorRetries++
			if errorRetries == 3 {
				return errors.Wrapf(err, fname)
			} else {
				log.Warningf("Error from tryOverwriteStoreIndexWithRetry %q", err)
			}
		}
		log.Info("Retrying updating remote store index")
	}
}

func getStoreIndexFromBlocks(
	ctx context.Context,
	blobStore longtailstorelib.BlobStore,
	blobClient longtailstorelib.BlobClient,
	workerCount int,
	blockKeys []string) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "getStoreIndexFromBlocks"
	log := logrus.WithFields(logrus.Fields{
		"fname":          fname,
		"workerCount":    workerCount,
		"len(blockKeys)": blockKeys,
	})
	log.Debug(fname)

	storeIndex, errno := longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
	if errno != 0 {
		err := longtailutils.MakeError(errno, fmt.Sprintf("Failed creating empty store index"))
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
	}

	batchCount := workerCount
	batchStart := 0

	if batchCount > len(blockKeys) {
		batchCount = len(blockKeys)
	}
	clients := make([]longtailstorelib.BlobClient, batchCount)
	for c := 0; c < batchCount; c++ {
		client, err := blobStore.NewClient(ctx)
		if err != nil {
			storeIndex.Dispose()
			return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
		}
		clients[c] = client
	}

	defer func(clients []longtailstorelib.BlobClient) {
		for _, client := range clients {
			client.Close()
		}
	}(clients)

	progress := longtailutils.CreateProgress("Scanning blocks")
	defer progress.Dispose()

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
			go func(client longtailstorelib.BlobClient, batchPos int, blockKey string) {
				storedBlockData, _, err := longtailutils.ReadBlobWithRetry(
					ctx,
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
			batchStoreIndex.Dispose()
			storeIndex.Dispose()
			return longtaillib.Longtail_StoreIndex{}, longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
		}
		newStoreIndex, errno := longtaillib.MergeStoreIndex(storeIndex, batchStoreIndex)
		if errno != 0 {
			err := longtailutils.MakeError(errno, "Failed merging store index")
			batchStoreIndex.Dispose()
			storeIndex.Dispose()
			return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
		}
		batchStoreIndex.Dispose()
		storeIndex.Dispose()
		storeIndex = newStoreIndex
		progress.OnProgress(uint32(len(blockKeys)), uint32(batchStart+batchLength))
		batchStart += batchLength
	}

	return storeIndex, nil
}

func buildStoreIndexFromStoreBlocks(
	ctx context.Context,
	blobStore longtailstorelib.BlobStore,
	blobClient longtailstorelib.BlobClient,
	workerCount int) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "buildStoreIndexFromStoreBlocks"
	log := logrus.WithFields(logrus.Fields{
		"fname":       fname,
		"workerCount": workerCount,
	})
	log.Debug(fname)

	var items []string
	blobs, err := blobClient.GetObjects("")
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
	}

	for _, blob := range blobs {
		if blob.Size == 0 {
			continue
		}
		if strings.HasSuffix(blob.Name, ".lsb") {
			items = append(items, blob.Name)
		}
	}

	return getStoreIndexFromBlocks(ctx, blobStore, blobClient, workerCount, items)
}

func readStoreStoreIndexFromPath(
	ctx context.Context,
	key string,
	client longtailstorelib.BlobClient) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "readStoreStoreIndexFromPath"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"key":   key,
	})
	log.Debug(fname)

	blobData, _, err := longtailutils.ReadBlobWithRetry(ctx, client, key)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
	}
	if blobData == nil {
		return longtaillib.Longtail_StoreIndex{}, nil
	}
	storeIndex, errno := longtaillib.ReadStoreIndexFromBuffer(blobData)
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Cant parse store index from `%s`", key))
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
	}
	return storeIndex, nil
}

func getStoreStoreIndexes(
	ctx context.Context,
	client longtailstorelib.BlobClient) ([]string, error) {
	const fname = "getStoreStoreIndexes"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)

	var items []string
	blobs, err := client.GetObjects("store")
	if err != nil {
		return nil, errors.Wrapf(err, fname)
	}

	for _, blob := range blobs {
		if blob.Size == 0 {
			continue
		}
		if strings.HasSuffix(blob.Name, ".lsi") {
			items = append(items, blob.Name)
		}
	}
	return items, nil
}

func mergeStoreIndexItems(
	ctx context.Context,
	client longtailstorelib.BlobClient,
	items []string) (longtaillib.Longtail_StoreIndex, []string, error) {
	const fname = "mergeStoreIndexItems"
	log := logrus.WithFields(logrus.Fields{
		"fname":      fname,
		"len(items)": len(items),
	})
	log.Debug(fname)

	var usedItems []string
	storeIndex := longtaillib.Longtail_StoreIndex{}
	for _, item := range items {
		tmpStoreIndex, err := readStoreStoreIndexFromPath(ctx, item, client)
		if err != nil || (!tmpStoreIndex.IsValid()) {
			// The file we expected is no longer there, tell caller that we need to try again
			storeIndex.Dispose()
			return longtaillib.Longtail_StoreIndex{}, nil, nil
		}
		if !storeIndex.IsValid() {
			storeIndex = tmpStoreIndex
			usedItems = append(usedItems, item)
			continue
		}
		mergedStoreIndex, errno := longtaillib.MergeStoreIndex(storeIndex, tmpStoreIndex)
		tmpStoreIndex.Dispose()
		storeIndex.Dispose()
		if errno != 0 {
			return longtaillib.Longtail_StoreIndex{}, nil, errors.Wrap(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "contentIndexWorker: longtaillib.MergeStoreIndex() failed")
		}
		storeIndex = mergedStoreIndex
		usedItems = append(usedItems, item)
	}
	return storeIndex, usedItems, nil
}

func readStoreStoreIndexWithItems(
	ctx context.Context,
	client longtailstorelib.BlobClient) (longtaillib.Longtail_StoreIndex, []string, error) {
	const fname = "readStoreStoreIndexWithItems"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)

	for {
		items, err := getStoreStoreIndexes(ctx, client)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, nil, err
		}

		if len(items) == 0 {
			storeIndex, errno := longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
			if errno != 0 {
				err := longtailutils.MakeError(errno, "Failed to create empty store index")
				return longtaillib.Longtail_StoreIndex{}, nil, errors.Wrapf(err, fname)
			}
			return storeIndex, nil, nil
		}

		storeIndex, usedItems, err := mergeStoreIndexItems(ctx, client, items)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, nil, errors.Wrapf(err, fname)
		}
		if len(usedItems) == 0 {
			// The underlying index files changed as we were scanning them, abort and try again
			continue
		}
		if storeIndex.IsValid() {
			return storeIndex, usedItems, nil
		}
		log.Printf("Retrying reading remote store index\n")
	}
}

func addBlocksToStoreIndex(
	storeIndex longtaillib.Longtail_StoreIndex,
	addedBlockIndexes []longtaillib.Longtail_BlockIndex) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "readStoreStoreIndexWithItems"
	log := logrus.WithFields(logrus.Fields{
		"fname":                  fname,
		"len(addedBlockIndexes)": addedBlockIndexes,
	})
	log.Debug(fname)

	addedStoreIndex, errno := longtaillib.CreateStoreIndexFromBlocks(addedBlockIndexes)
	if errno != 0 {
		err := longtailutils.MakeError(errno, "Failed to create store index from block indexes")
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
	}

	if !storeIndex.IsValid() {
		return addedStoreIndex, nil
	}
	updatedStoreIndex, errno := longtaillib.MergeStoreIndex(storeIndex, addedStoreIndex)
	addedStoreIndex.Dispose()
	if errno != 0 {
		updatedStoreIndex.Dispose()
		err := longtailutils.MakeError(errno, fmt.Sprintf("Failed merging store index with %d blocks", len(addedBlockIndexes)))
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
	}
	return updatedStoreIndex, nil
}

func readRemoteStoreIndex(
	ctx context.Context,
	optionalStoreIndexPath string,
	blobStore longtailstorelib.BlobStore,
	client longtailstorelib.BlobClient,
	accessType AccessType,
	workerCount int) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "readRemoteStoreIndex"
	log := logrus.WithFields(logrus.Fields{
		"fname":       fname,
		"accessType":  accessType,
		"workerCount": workerCount,
	})
	log.Debug(fname)

	var err error
	var errno int
	var storeIndex longtaillib.Longtail_StoreIndex
	if accessType != Init {
		if accessType == ReadOnly && len(optionalStoreIndexPath) > 0 {
			sbuffer, err := longtailutils.ReadFromURI(optionalStoreIndexPath)
			if err == nil {
				storeIndex, errno = longtaillib.ReadStoreIndexFromBuffer(sbuffer)
				if errno != 0 {
					err = longtailutils.MakeError(errno, fmt.Sprintf("Cant parse optional store index from `%s`", optionalStoreIndexPath))
					log.WithError(err).Info("Failed parsing optional store index")
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				log.WithError(err).Info("Failed reading optional store index")
			}
		}
		if !storeIndex.IsValid() {
			storeIndex, _, err = readStoreStoreIndexWithItems(ctx, client)
			if err != nil {
				log.WithError(err).Info("Failed reading existsing store index")
			}
		}
	}

	if storeIndex.IsValid() {
		return storeIndex, nil
	}

	if accessType == ReadOnly {
		storeIndex, errno = longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
		if errno != 0 {
			err := longtailutils.MakeError(errno, fmt.Sprintf("Failed creating empty store index"))
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		return storeIndex, nil
	}

	storeIndex, err = buildStoreIndexFromStoreBlocks(
		ctx,
		blobStore,
		client,
		workerCount)

	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	log.Printf("Rebuilt remote index with %d blocks\n", len(storeIndex.GetBlockHashes()))
	newStoreIndex, err := addToRemoteStoreIndex(ctx, client, storeIndex)
	if err != nil {
		log.WithError(err).Error("Failed to update store index")
	}
	if newStoreIndex.IsValid() {
		storeIndex.Dispose()
		storeIndex = newStoreIndex
	}
	return storeIndex, nil
}

func getBlockPath(basePath string, blockHash uint64) string {
	fileName := fmt.Sprintf("0x%016x.lsb", blockHash)
	dir := filepath.Join(basePath, fileName[2:6])
	name := filepath.Join(dir, fileName)
	name = strings.Replace(name, "\\", "/", -1)
	return name
}

func CreateBlockStoreForURI(
	uri string,
	optionalStoreIndexPath string,
	jobAPI longtaillib.Longtail_JobAPI,
	numWorkerCount int,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	accessType AccessType) (longtaillib.Longtail_BlockStoreAPI, error) {
	const fname = "CreateBlockStoreForURI"
	log := logrus.WithFields(logrus.Fields{
		"fname":             fname,
		"numWorkerCount":    numWorkerCount,
		"targetBlockSize":   targetBlockSize,
		"maxChunksPerBlock": maxChunksPerBlock,
		"accessType":        accessType,
	})
	log.Debug(fname)

	blobStoreURL, err := url.Parse(uri)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			gcsBlobStore, err := longtailstorelib.NewGCSBlobStore(blobStoreURL, false)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, errors.Wrap(err, fname)
			}
			gcsBlockStore, err := NewRemoteBlockStore(
				jobAPI,
				gcsBlobStore,
				optionalStoreIndexPath,
				numWorkerCount,
				accessType)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, errors.Wrap(err, fname)
			}
			return longtaillib.CreateBlockStoreAPI(gcsBlockStore), nil
		case "s3":
			s3BlobStore, err := longtailstorelib.NewS3BlobStore(blobStoreURL)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, errors.Wrap(err, fname)
			}
			s3BlockStore, err := NewRemoteBlockStore(
				jobAPI,
				s3BlobStore,
				optionalStoreIndexPath,
				numWorkerCount,
				accessType)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, errors.Wrap(err, fname)
			}
			return longtaillib.CreateBlockStoreAPI(s3BlockStore), nil
		case "abfs":
			err := fmt.Errorf("azure Gen1 storage not yet implemented for path %s", uri)
			return longtaillib.Longtail_BlockStoreAPI{}, errors.Wrap(err, fname)
		case "abfss":
			err := fmt.Errorf("azure Gen2 storage not yet implemented for path %s", uri)
			return longtaillib.Longtail_BlockStoreAPI{}, errors.Wrap(err, fname)
		case "file":
			return longtaillib.CreateFSBlockStore(jobAPI, longtaillib.CreateFSStorageAPI(), blobStoreURL.Path[1:]), nil
		}
	}
	return longtaillib.CreateFSBlockStore(jobAPI, longtaillib.CreateFSStorageAPI(), uri), nil
}
