package remotestore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
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
	// ReadWrite - read/write access
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
	err               error
}

type remoteStore struct {
	jobAPI           longtaillib.Longtail_JobAPI
	blobStore        longtailstorelib.BlobStore
	blobStoreOptions []longtailstorelib.BlobStoreOption
	defaultClient    longtailstorelib.BlobClient

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
	workerFlushReplyChan   chan error
	indexFlushChan         chan int
	indexFlushReplyChan    chan error
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
		"fname":      fname,
		"s":          s,
		"blobClient": blobClient.String(),
	})
	log.Debug(fname)

	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count], 1)

	blockIndex := storedBlock.GetBlockIndex()
	blockHash := blockIndex.GetBlockHash()
	key := getBlockPath("chunks", blockHash)

	log = log.WithFields(logrus.Fields{
		"blockHash": blockHash,
		"key":       key,
	})

	objHandle, err := blobClient.NewObject(key)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	if exists, err := objHandle.Exists(); err == nil && !exists {
		blob, err := longtaillib.WriteStoredBlockToBuffer(storedBlock)
		if err != nil {
			return errors.Wrap(err, fname)
		}
		defer blob.Dispose()

		var sleepTimes = [...]time.Duration{100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second}
		ok, err := objHandle.Write(blob.ToBuffer())
		if err != nil {
			err := errors.Wrap(err, fmt.Sprintf("failed to put stored block at `%s` in `%s`", key, s))
			err = errors.Wrap(err, fname)
			log.Error(err)
			return err
		}
		if !ok {
			for _, sleepTime := range sleepTimes {
				atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount], 1)
				log.Warningf("retrying putBlob (sleeping %s)", sleepTime)
				time.Sleep(sleepTime)
				ok, err = objHandle.Write(blob.ToBuffer())
				if err != nil {
					err = errors.Wrap(err, fmt.Sprintf("failed to put stored block at `%s` in `%s`", key, s))
					err = errors.Wrap(err, fname)
					log.Error(err)
					return err
				}
				if ok {
					break
				}
			}
			if !ok {
				err = fmt.Errorf("failed to put stored block at `%s` in `%s` even after retries", key, s)
				err = errors.Wrap(err, fname)
				log.Error(err)
				return err
			}
		}

		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count], (uint64)(blob.Size()))
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count], (uint64)(blockIndex.GetChunkCount()))

		log.WithFields(logrus.Fields{
			"chunks": blockIndex.GetChunkCount(),
			"bytes":  blob.Size(),
			"path":   objHandle.String(),
		}).Info("wrote block")
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
		"fname":      fname,
		"s":          s,
		"blobClient": blobClient.String(),
		"blockHash":  blockHash,
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

	storedBlock, err := longtaillib.ReadStoredBlockFromBuffer(storedBlockData)
	storedBlockData = nil
	if err != nil {
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1)
		err = errors.Wrap(err, fmt.Sprintf("Failed to parse stored block `%s`", key))
		return longtaillib.Longtail_StoredBlock{}, errors.Wrap(err, fname)
	}

	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count], (uint64)(len(storedBlockData)))
	blockIndex := storedBlock.GetBlockIndex()
	if blockIndex.GetBlockHash() != blockHash {
		atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount], 1)
		err = errors.Wrap(longtaillib.BadFormatErr(), "Block hash does not match path")
		return longtaillib.Longtail_StoredBlock{}, errors.Wrap(err, fname)
	}
	atomic.AddUint64(&s.stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count], (uint64)(blockIndex.GetChunkCount()))
	log.WithFields(logrus.Fields{
		"chunks": blockIndex.GetChunkCount(),
		"bytes":  len(storedBlockData),
		"path":   fmt.Sprintf("%s/%s", blobClient.String(), key)}).Info("read block")
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
		"client": client.String(),
		"getMsg": getMsg,
	})
	log.Debug(fname)
	s.fetchedBlocksSync.Lock()
	prefetchedBlock := s.prefetchBlocks[getMsg.blockHash]
	if prefetchedBlock != nil {
		storedBlock := prefetchedBlock.storedBlock
		err := prefetchedBlock.err
		if storedBlock.IsValid() {
			blockSize := -int64(storedBlock.GetBlockSize())
			atomic.AddInt64(&s.prefetchMemory, blockSize)
		}
		if storedBlock.IsValid() || err != nil {
			s.prefetchBlocks[getMsg.blockHash] = nil
			s.fetchedBlocksSync.Unlock()
			getMsg.asyncCompleteAPI.OnComplete(storedBlock, err)
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
		fmt.Printf("Prefetch in background while fetching block, dropping duplicate\n")
		storedBlock.Dispose()
		s.fetchedBlocksSync.Unlock()
		return
	}
	completeCallbacks := prefetchedBlock.completeCallbacks
	s.prefetchBlocks[getMsg.blockHash] = nil
	s.fetchedBlocksSync.Unlock()
	for _, c := range completeCallbacks {
		if getStoredBlockErr != nil {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, errors.Wrap(getStoredBlockErr, fname))
			continue
		}
		buf, err := longtaillib.WriteStoredBlockToBuffer(storedBlock)
		if err != nil {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, errors.Wrap(err, fname))
			continue
		}
		blockCopy, err := longtaillib.ReadStoredBlockFromBuffer(buf.ToBuffer())
		fmt.Printf("Making block copy due to multiple waiters\n")
		buf.Dispose()
		if err != nil {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, errors.Wrap(err, fname))
			continue
		}
		c.OnComplete(blockCopy, nil)
	}
	getMsg.asyncCompleteAPI.OnComplete(storedBlock, errors.Wrap(getStoredBlockErr, fname))
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
		"client":    client.String(),
		"blockHash": blockHash,
	})
	log.Debug(fname)
	s.fetchedBlocksSync.Lock()
	defer s.fetchedBlocksSync.Unlock()
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
		"client":      client.String(),
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

	s.fetchedBlocksSync.Lock()

	prefetchedBlock, exists = s.prefetchBlocks[prefetchMsg.blockHash]
	if prefetchedBlock == nil {
		fmt.Printf("Dropping prefetched block due to background prefetch\n")
		storedBlock.Dispose()
		s.fetchedBlocksSync.Unlock()
		return
	}

	completeCallbacks := prefetchedBlock.completeCallbacks
	if len(completeCallbacks) == 0 {
		// Nobody is actively waiting for the block
		if storedBlock.IsValid() {
			blockSize := int64(storedBlock.GetBlockSize())
			prefetchedBlock.storedBlock = storedBlock
			atomic.AddInt64(&s.prefetchMemory, blockSize)
		}
		prefetchedBlock.err = getErr
		s.fetchedBlocksSync.Unlock()
		return
	}
	s.prefetchBlocks[prefetchMsg.blockHash] = nil
	s.fetchedBlocksSync.Unlock()

	for i := 1; i < len(completeCallbacks)-1; i++ {
		c := completeCallbacks[i]
		if getErr != nil {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, errors.Wrap(getErr, fname))
			continue
		}
		buf, err := longtaillib.WriteStoredBlockToBuffer(storedBlock)
		if err != nil {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, errors.Wrap(err, fname))
			continue
		}
		blockCopy, err := longtaillib.ReadStoredBlockFromBuffer(buf.ToBuffer())
		buf.Dispose()
		if err != nil {
			c.OnComplete(longtaillib.Longtail_StoredBlock{}, errors.Wrap(err, fname))
			continue
		}
		c.OnComplete(blockCopy, nil)
	}
	if getErr != nil {
		getErr = errors.Wrap(getErr, fname)
	}
	completeCallbacks[0].OnComplete(storedBlock, getErr)
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
	flushReplyMessages chan<- error,
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
					putMsg.asyncCompleteAPI.OnComplete(errors.Wrap(longtaillib.AccessViolationErr(), fname))
					continue
				}
				err := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
				putMsg.asyncCompleteAPI.OnComplete(errors.Wrap(err, fname))
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
					flushReplyMessages <- nil
				case putMsg, more := <-putBlockMessages:
					if more {
						if accessType == ReadOnly {
							putMsg.asyncCompleteAPI.OnComplete(errors.Wrap(longtaillib.AccessViolationErr(), fname))
							continue
						}
						err := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
						putMsg.asyncCompleteAPI.OnComplete(errors.Wrap(err, fname))
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
					flushReplyMessages <- nil
				case putMsg, more := <-putBlockMessages:
					if more {
						if accessType == ReadOnly {
							putMsg.asyncCompleteAPI.OnComplete(errors.Wrap(longtaillib.AccessViolationErr(), fname))
							continue
						}
						err := putStoredBlock(ctx, s, client, blockIndexMessages, putMsg.storedBlock)
						putMsg.asyncCompleteAPI.OnComplete(errors.Wrap(err, fname))
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
		"blobClient":             blobClient.String(),
		"len(addedBlockIndexes)": addedBlockIndexes,
	})
	log.Debug(fname)

	addedStoreIndex, err := longtaillib.CreateStoreIndexFromBlocks(addedBlockIndexes)
	if err != nil {
		err := errors.Wrap(err, "Failed to create store index from block indexes")
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	defer addedStoreIndex.Dispose()
	return addToRemoteStoreIndex(ctx, blobClient, addedStoreIndex)
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
	message.asyncCompleteAPI.OnComplete(message.blockHashes, nil)
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

	existingStoreIndex, err := longtaillib.GetExistingStoreIndex(storeIndex, message.chunkHashes, message.minBlockUsagePercent)
	if err != nil {
		message.asyncCompleteAPI.OnComplete(longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname))
		return
	}
	message.asyncCompleteAPI.OnComplete(existingStoreIndex, nil)
}

func onPruneBlocksMessage(
	ctx context.Context,
	s *remoteStore,
	blobClient longtailstorelib.BlobClient,
	storeIndex longtaillib.Longtail_StoreIndex,
	keepBlockHashes []uint64) (uint32, longtaillib.Longtail_StoreIndex, error) {
	const fname = "onPruneBlocksMessage"
	log := logrus.WithFields(logrus.Fields{
		"fname":                fname,
		"s":                    s,
		"blobClient":           blobClient.String(),
		"len(keepBlockHashes)": len(keepBlockHashes),
	})
	log.Debug(fname)

	prunedIndex, err := longtaillib.PruneStoreIndex(storeIndex, keepBlockHashes)
	if err != nil {
		return 0, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	err = tryOverwriteStoreIndexWithRetry(ctx, prunedIndex, blobClient)
	if err != nil {
		prunedIndex.Dispose()
		return 0, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
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
	return prunedCount, prunedIndex, nil
}

func getCurrentStoreIndex(
	ctx context.Context,
	s *remoteStore,
	optionalStoreIndexPaths []string,
	client longtailstorelib.BlobClient,
	accessType AccessType,
	storeIndex longtaillib.Longtail_StoreIndex,
	addedBlockIndexes []longtaillib.Longtail_BlockIndex) (longtaillib.Longtail_StoreIndex, longtaillib.Longtail_StoreIndex, error) {
	const fname = "getCurrentStoreIndex"
	log := logrus.WithFields(logrus.Fields{
		"fname":                   fname,
		"s":                       s,
		"optionalStoreIndexPaths": optionalStoreIndexPaths,
		"client":                  client.String(),
		"accessType":              accessType,
		"len(addedBlockIndexes)":  len(addedBlockIndexes),
	})
	log.Debug(fname)
	var err error = nil
	if !storeIndex.IsValid() {
		storeIndex, err = readRemoteStoreIndex(ctx, optionalStoreIndexPaths, s.blobStore, client, accessType, s.workerCount)
		if err != nil {
			return storeIndex, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
	}
	if len(addedBlockIndexes) == 0 {
		return storeIndex, longtaillib.Longtail_StoreIndex{}, nil
	}
	updatedStoreIndex, err := addBlocksToStoreIndex(storeIndex, addedBlockIndexes)
	if err != nil {
		log.Warnf("Failed to update store index with added blocks %s", err)
		return storeIndex, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	return storeIndex, updatedStoreIndex, nil
}

func contentIndexWorker(
	ctx context.Context,
	s *remoteStore,
	optionalStoreIndexPaths []string,
	preflightGetMessages <-chan preflightGetMessage,
	prefetchBlockMessages chan<- prefetchBlockMessage,
	blockIndexMessages <-chan blockIndexMessage,
	getExistingContentMessages <-chan getExistingContentMessage,
	pruneBlocksMessages <-chan pruneBlocksMessage,
	flushMessages <-chan int,
	flushReplyMessages chan<- error,
	accessType AccessType) error {
	const fname = "contentIndexWorker"
	log := logrus.WithFields(logrus.Fields{
		"fname":                   fname,
		"s":                       s,
		"optionalStoreIndexPaths": optionalStoreIndexPaths,
		"accessType":              accessType,
	})
	log.Debug(fname)
	client, err := s.blobStore.NewClient(ctx)
	if err != nil {
	out:
		for {
			select {
			case <-flushMessages:
				flushReplyMessages <- nil
			case _, more := <-blockIndexMessages:
				if !more {
					break out
				}
			case getExistingContentMessage := <-getExistingContentMessages:
				getExistingContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_StoreIndex{}, errors.Wrap(longtaillib.InvalidArgumentError(), fname))
			case pruneBlocksMessage := <-pruneBlocksMessages:
				pruneBlocksMessage.asyncCompleteAPI.OnComplete(0, errors.Wrap(longtaillib.InvalidArgumentError(), fname))
			}
		}
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
			storeIndex, updatedStoreIndex, err = getCurrentStoreIndex(ctx, s, optionalStoreIndexPaths, client, accessType, storeIndex, addedBlockIndexes)
			if err != nil {
				storeIndex.Dispose()
				preflightGetMsg.asyncCompleteAPI.OnComplete([]uint64{}, errors.Wrap(err, fname))
			} else if updatedStoreIndex.IsValid() {
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
			storeIndex, updatedStoreIndex, err = getCurrentStoreIndex(ctx, s, optionalStoreIndexPaths, client, accessType, storeIndex, addedBlockIndexes)
			if err != nil {
				storeIndex.Dispose()
				getExistingContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname))
			} else if accessType == ReadOnly {
				go func() {
					onGetExistingContentMessage(s, storeIndex, getExistingContentMessage)
				}()
			} else if updatedStoreIndex.IsValid() {
				onGetExistingContentMessage(s, updatedStoreIndex, getExistingContentMessage)
				updatedStoreIndex.Dispose()
			} else {
				onGetExistingContentMessage(s, storeIndex, getExistingContentMessage)
			}
		case pruneBlocksMessage := <-pruneBlocksMessages:
			if accessType == ReadOnly {
				pruneBlocksMessage.asyncCompleteAPI.OnComplete(0, errors.Wrap(longtaillib.AccessViolationErr(), fname))
				continue
			}
			received++
			storeIndex, updatedStoreIndex, err = getCurrentStoreIndex(ctx, s, optionalStoreIndexPaths, client, accessType, storeIndex, addedBlockIndexes)
			if err != nil {
				storeIndex.Dispose()
				pruneBlocksMessage.asyncCompleteAPI.OnComplete(0, errors.Wrap(err, fname))
			} else {
				prunedCount := uint32(0)
				prunedStoreIndex := longtaillib.Longtail_StoreIndex{}
				if updatedStoreIndex.IsValid() {
					prunedCount, prunedStoreIndex, err = onPruneBlocksMessage(ctx, s, client, updatedStoreIndex, pruneBlocksMessage.keepBlockHashes)
					updatedStoreIndex.Dispose()
				} else {
					prunedCount, prunedStoreIndex, err = onPruneBlocksMessage(ctx, s, client, storeIndex, pruneBlocksMessage.keepBlockHashes)
				}
				if prunedStoreIndex.IsValid() {
					storeIndex.Dispose()
					storeIndex = prunedStoreIndex
				}
				pruneBlocksMessage.asyncCompleteAPI.OnComplete(prunedCount, errors.Wrap(err, fname))
			}
		default:
		}

		if received > 0 {
			continue
		}

		select {
		case <-flushMessages:
			if err != nil {
				flushReplyMessages <- err
				err = nil
				continue
			}
			if len(addedBlockIndexes) > 0 && accessType != ReadOnly {
				newStoreIndex, err := addBlocksToRemoteStoreIndex(ctx, s, client, addedBlockIndexes)
				if err != nil {
					flushReplyMessages <- err
					err = nil
					continue
				}
				addedBlockIndexes = nil
				if newStoreIndex.IsValid() {
					storeIndex.Dispose()
					storeIndex = newStoreIndex
				}
			}
			flushReplyMessages <- nil
		case preflightGetMsg := <-preflightGetMessages:
			storeIndex, updatedStoreIndex, err = getCurrentStoreIndex(ctx, s, optionalStoreIndexPaths, client, accessType, storeIndex, addedBlockIndexes)
			if err != nil {
				storeIndex.Dispose()
				preflightGetMsg.asyncCompleteAPI.OnComplete([]uint64{}, errors.Wrap(err, fname))
			} else if updatedStoreIndex.IsValid() {
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
			storeIndex, updatedStoreIndex, err = getCurrentStoreIndex(ctx, s, optionalStoreIndexPaths, client, accessType, storeIndex, addedBlockIndexes)
			if err != nil {
				storeIndex.Dispose()
				getExistingContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname))
			} else if accessType == ReadOnly {
				go func() {
					onGetExistingContentMessage(s, storeIndex, getExistingContentMessage)
				}()
			} else if updatedStoreIndex.IsValid() {
				onGetExistingContentMessage(s, updatedStoreIndex, getExistingContentMessage)
				updatedStoreIndex.Dispose()
			} else {
				onGetExistingContentMessage(s, storeIndex, getExistingContentMessage)
			}
		case pruneBlocksMessage := <-pruneBlocksMessages:
			if accessType == ReadOnly {
				pruneBlocksMessage.asyncCompleteAPI.OnComplete(0, errors.Wrap(longtaillib.AccessViolationErr(), fname))
				continue
			}
			storeIndex, updatedStoreIndex, err = getCurrentStoreIndex(ctx, s, optionalStoreIndexPaths, client, accessType, storeIndex, addedBlockIndexes)
			if err != nil {
				storeIndex.Dispose()
				pruneBlocksMessage.asyncCompleteAPI.OnComplete(0, errors.Wrap(err, fname))
			} else {
				prunedCount := uint32(0)
				prunedStoreIndex := longtaillib.Longtail_StoreIndex{}
				if updatedStoreIndex.IsValid() {
					prunedCount, prunedStoreIndex, err = onPruneBlocksMessage(ctx, s, client, updatedStoreIndex, pruneBlocksMessage.keepBlockHashes)
					updatedStoreIndex.Dispose()
				} else {
					prunedCount, prunedStoreIndex, err = onPruneBlocksMessage(ctx, s, client, storeIndex, pruneBlocksMessage.keepBlockHashes)
				}
				if prunedStoreIndex.IsValid() {
					storeIndex.Dispose()
					storeIndex = prunedStoreIndex
				}
				pruneBlocksMessage.asyncCompleteAPI.OnComplete(prunedCount, errors.Wrap(err, fname))
			}
		}
	}

	if err != nil && !longtaillib.IsNotExist(err) {
		storeIndex.Dispose()
		return err
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
	optionalStoreIndexPaths []string,
	workerCount int,
	accessType AccessType,
	opts ...longtailstorelib.BlobStoreOption) (longtaillib.BlockStoreAPI, error) {
	const fname = "NewRemoteBlockStore"
	log := logrus.WithFields(logrus.Fields{
		"fname":                   fname,
		"blobStore":               blobStore,
		"optionalStoreIndexPaths": optionalStoreIndexPaths,
		"workerCount":             workerCount,
		"accessType":              accessType,
		"opts":                    opts,
	})
	log.Debug(fname)
	ctx := context.Background()
	defaultClient, err := blobStore.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}

	s := &remoteStore{
		jobAPI:           jobAPI,
		blobStore:        blobStore,
		blobStoreOptions: opts,
		defaultClient:    defaultClient}

	s.workerCount = workerCount
	s.putBlockChan = make(chan putBlockMessage, 16+s.workerCount*8)
	s.getBlockChan = make(chan getBlockMessage, 32+s.workerCount*4)
	s.prefetchBlockChan = make(chan prefetchBlockMessage, 16+s.workerCount*2048)
	s.deleteBlockChan = make(chan deleteBlockMessage, 16+s.workerCount*8)
	s.preflightGetChan = make(chan preflightGetMessage, 16)
	s.blockIndexChan = make(chan blockIndexMessage, 16+s.workerCount*2048)
	s.getExistingContentChan = make(chan getExistingContentMessage, 16)
	s.pruneBlocksChan = make(chan pruneBlocksMessage, 1)
	s.workerFlushChan = make(chan int, s.workerCount)
	s.workerFlushReplyChan = make(chan error, s.workerCount)
	s.indexFlushChan = make(chan int, 1)
	s.indexFlushReplyChan = make(chan error, 1)
	s.workerErrorChan = make(chan error, 1+s.workerCount)

	s.prefetchMemory = 0
	s.maxPrefetchMemory = 512 * 1024 * 1024

	s.prefetchBlocks = map[uint64]*pendingPrefetchedBlock{}

	go func() {
		err := contentIndexWorker(
			ctx,
			s,
			optionalStoreIndexPaths,
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
func (s *remoteStore) PutStoredBlock(storedBlock longtaillib.Longtail_StoredBlock, asyncCompleteAPI longtaillib.Longtail_AsyncPutStoredBlockAPI) error {
	s.putBlockChan <- putBlockMessage{storedBlock: storedBlock, asyncCompleteAPI: asyncCompleteAPI}
	return nil
}

// PreflightGet ...
func (s *remoteStore) PreflightGet(blockHashes []uint64, asyncCompleteAPI longtaillib.Longtail_AsyncPreflightStartedAPI) error {
	s.preflightGetChan <- preflightGetMessage{blockHashes: blockHashes, asyncCompleteAPI: asyncCompleteAPI}
	return nil
}

// GetStoredBlock ...
func (s *remoteStore) GetStoredBlock(blockHash uint64, asyncCompleteAPI longtaillib.Longtail_AsyncGetStoredBlockAPI) error {
	s.getBlockChan <- getBlockMessage{blockHash: blockHash, asyncCompleteAPI: asyncCompleteAPI}
	return nil
}

// GetExistingContent ...
func (s *remoteStore) GetExistingContent(
	chunkHashes []uint64,
	minBlockUsagePercent uint32,
	asyncCompleteAPI longtaillib.Longtail_AsyncGetExistingContentAPI) error {
	s.getExistingContentChan <- getExistingContentMessage{chunkHashes: chunkHashes, minBlockUsagePercent: minBlockUsagePercent, asyncCompleteAPI: asyncCompleteAPI}
	return nil
}

// PruneBlocks ...
func (s *remoteStore) PruneBlocks(
	keepBlockHashes []uint64,
	asyncCompleteAPI longtaillib.Longtail_AsyncPruneBlocksAPI) error {
	s.pruneBlocksChan <- pruneBlocksMessage{keepBlockHashes: keepBlockHashes, asyncCompleteAPI: asyncCompleteAPI}
	return nil
}

// GetStats ...
func (s *remoteStore) GetStats() (longtaillib.BlockStoreStats, error) {
	return s.stats, nil
}

// Flush ...
func (s *remoteStore) Flush(asyncCompleteAPI longtaillib.Longtail_AsyncFlushAPI) error {
	const fname = "remoteStore.Flush"
	go func() {
		any_err := error(nil)
		for i := 0; i < s.workerCount; i++ {
			s.workerFlushChan <- 1
		}
		for i := 0; i < s.workerCount; i++ {
			err := <-s.workerFlushReplyChan
			if err != nil && any_err == nil {
				any_err = err
			}
		}
		s.indexFlushChan <- 1
		err := <-s.indexFlushReplyChan
		if err != nil && any_err == nil {
			any_err = err
		}
		asyncCompleteAPI.OnComplete(errors.Wrap(any_err, fname))
	}()
	return nil
}

// Close ...
func (s *remoteStore) Close() {
	close(s.putBlockChan)
	for i := 0; i < s.workerCount; i++ {
		err := <-s.workerErrorChan
		if err != nil {
			logrus.Error(err)
		}
	}
	close(s.blockIndexChan)
	err := <-s.workerErrorChan
	if err != nil {
		logrus.Error(err)
	}

	s.defaultClient.Close()
}

func tryAddRemoteStoreIndexWithLocking(
	ctx context.Context,
	addStoreIndex longtaillib.Longtail_StoreIndex,
	blobClient longtailstorelib.BlobClient) (bool, longtaillib.Longtail_StoreIndex, error) {
	const fname = "tryAddRemoteStoreIndexWithLocking"
	log := logrus.WithFields(logrus.Fields{
		"fname":      fname,
		"blobClient": blobClient,
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
		if longtaillib.IsNotExist(err) {
			return false, longtaillib.Longtail_StoreIndex{}, nil
		}

		remoteStoreIndex, err := longtaillib.ReadStoreIndexFromBuffer(blob)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Cant parse store index from `%s/%s`", blobClient.String(), key))
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		defer remoteStoreIndex.Dispose()
		log.WithFields(logrus.Fields{"path": objHandle.String(), "bytes": len(blob)}).Info("read store index")

		newStoreIndex, err := longtaillib.MergeStoreIndex(remoteStoreIndex, addStoreIndex)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Failed merging store index for `%s/%s`", blobClient.String(), key))
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}

		storeBlob, err := longtaillib.WriteStoreIndexToBuffer(newStoreIndex)
		if err != nil {
			newStoreIndex.Dispose()
			err = errors.Wrap(err, fmt.Sprintf("Failed serializing store index for `%s/%s`", blobClient.String(), key))
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		defer storeBlob.Dispose()

		ok, err := objHandle.Write(storeBlob.ToBuffer())
		if err != nil {
			newStoreIndex.Dispose()
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		if !ok {
			newStoreIndex.Dispose()
			return false, longtaillib.Longtail_StoreIndex{}, nil
		}
		log.WithFields(logrus.Fields{"path": objHandle.String(), "bytes": storeBlob.Size()}).Info("wrote store index")
		return ok, newStoreIndex, nil
	}
	storeBlob, err := longtaillib.WriteStoreIndexToBuffer(addStoreIndex)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Failed serializing store index for `%s/%s`", blobClient.String(), key))
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	defer storeBlob.Dispose()

	ok, err := objHandle.Write(storeBlob.ToBuffer())
	if err != nil {
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	if ok {
		log.WithFields(logrus.Fields{"path": objHandle.String(), "bytes": storeBlob.Size()}).Info("wrote store index")
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
		"fname":      fname,
		"blobClient": blobClient,
	})
	log.Debug(fname)

	storeBlob, err := longtaillib.WriteStoreIndexToBuffer(storeIndex)
	if err != nil {
		err := errors.Wrap(err, "Failed serializing store index")
		return false, errors.Wrap(err, fname)
	}
	defer storeBlob.Dispose()

	sha256 := sha256.Sum256(storeBlob.ToBuffer())
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

	ok, err := objHandle.Write(storeBlob.ToBuffer())
	if !ok || err != nil {
		return ok, errors.Wrap(err, fname)
	}

	log.WithFields(logrus.Fields{"path": objHandle.String(), "bytes": storeBlob.Size()}).Info("wrote store index")

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
		"fname":      fname,
		"blobClient": blobClient.String(),
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

	mergedStoreIndex, err := longtaillib.MergeStoreIndex(storeIndex, addStoreIndex)
	storeIndex.Dispose()
	if err != nil {
		err = errors.Wrap(err, "Failed merging store index")
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
		"fname":      fname,
		"blobClient": blobClient,
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
				log.Errorf("Failed updating remote store after %d tryAddRemoteStoreIndex: %s", 3, err)
				return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			} else {
				log.Warnf("Error from tryAddRemoteStoreIndex %s", err)
			}
		}
		log.Debug("Retrying updating remote store index")
	}
	return longtaillib.Longtail_StoreIndex{}, nil
}

func tryOverwriteRemoteStoreIndexWithLocking(
	ctx context.Context,
	storeIndex longtaillib.Longtail_StoreIndex,
	client longtailstorelib.BlobClient) (bool, error) {
	const fname = "tryOverwriteRemoteStoreIndexWithLocking"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"client": client,
	})
	log.Debug(fname)

	storeBlob, err := longtaillib.WriteStoreIndexToBuffer(storeIndex)
	if err != nil {
		err := errors.Wrap(err, "Failed serializing store index")
		return false, errors.Wrap(err, fname)
	}
	defer storeBlob.Dispose()

	key := "store.lsi"
	objHandle, err := client.NewObject(key)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	_, err = objHandle.LockWriteVersion()
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	ok, err := objHandle.Write(storeBlob.ToBuffer())
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
		"fname":  fname,
		"client": client.String(),
	})
	log.Debug(fname)

	items, err := getStoreStoreIndexes(ctx, client)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	storeBlob, err := longtaillib.WriteStoreIndexToBuffer(storeIndex)
	if err != nil {
		err = errors.Wrap(err, "Failed serializing store index")
		return false, errors.Wrap(err, fname)
	}
	defer storeBlob.Dispose()

	sha256 := sha256.Sum256(storeBlob.ToBuffer())
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
		ok, err := objHandle.Write(storeBlob.ToBuffer())
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
		"fname":      fname,
		"blobClient": blobClient,
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
		"fname":      fname,
		"blobClient": blobClient,
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
				log.Errorf("Failed updating remote store after %d tryAddRemoteStoreIndex: %s", 3, err)
				return errors.Wrapf(err, fname)
			} else {
				log.Warnf("Error from tryOverwriteStoreIndexWithRetry %s", err)
			}
		}
		log.Debug("Retrying updating remote store index")
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
		"blobClient":     blobClient,
		"workerCount":    workerCount,
		"len(blockKeys)": blockKeys,
	})
	log.Debug(fname)

	storeIndex, err := longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
	if err != nil {
		err := errors.Wrap(err, "Failed creating empty store index")
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

	progress := longtailutils.CreateProgress("Scanning blocks           ", 1)
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

				blockIndex, err := longtaillib.ReadBlockIndexFromBuffer(storedBlockData)
				if err != nil {
					wg.Done()
					return
				}

				blockPath := getBlockPath("chunks", blockIndex.GetBlockHash())
				if blockPath == longtailstorelib.NormalizeFileSystemPath(blockKey) {
					batchBlockIndexes[batchPos] = blockIndex
				} else {
					log.Warnf("Block %s name does not match content hash, expected name %s", blockKey, blockPath)
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
		batchStoreIndex, err := longtaillib.CreateStoreIndexFromBlocks(batchBlockIndexes)
		for _, blockIndex := range batchBlockIndexes {
			blockIndex.Dispose()
		}
		if err != nil {
			batchStoreIndex.Dispose()
			storeIndex.Dispose()
			return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
		}
		newStoreIndex, err := longtaillib.MergeStoreIndex(storeIndex, batchStoreIndex)
		if err != nil {
			err := errors.Wrap(err, "Failed merging store index")
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
		"blobStore":   blobStore,
		"blobClient":  blobClient,
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
		"fname":  fname,
		"key":    key,
		"client": client.String(),
	})
	log.Debug(fname)

	blobData, _, err := longtailutils.ReadBlobWithRetry(ctx, client, key)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
	}
	if len(blobData) == 0 {
		err = errors.Wrap(os.ErrNotExist, fmt.Sprintf("%s/%s contains no data", client.String(), key))
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	storeIndex, err := longtaillib.ReadStoreIndexFromBuffer(blobData)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Cant parse store index from `%s/%s`", client.String(), key))
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
	}
	return storeIndex, nil
}

func getStoreStoreIndexes(
	ctx context.Context,
	client longtailstorelib.BlobClient) ([]string, error) {
	const fname = "getStoreStoreIndexes"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"client": client.String(),
	})
	log.Debug(fname)

	var items []string
	retryCount := 0
	retryDelay := []time.Duration{0, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, 1 * time.Second, 2 * time.Second}
	blobs, err := client.GetObjects("store")
	for err != nil {
		if longtaillib.IsNotExist(err) {
			return items, nil
		}
		if retryCount == len(retryDelay) {
			err = errors.Wrapf(err, "Failed list store indexes in store %s", client.String())
			log.Error(err)
			return nil, errors.Wrap(err, fname)
		}
		err = errors.Wrapf(err, "Retrying list store indexes in store %s with %s delay", client.String(), retryDelay[retryCount])
		log.Info(err)

		time.Sleep(retryDelay[retryCount])
		retryCount++
		blobs, err = client.GetObjects("store")
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
		"client":     client.String(),
		"len(items)": len(items),
	})
	log.Debug(fname)

	var usedItems []string
	storeIndex := longtaillib.Longtail_StoreIndex{}
	for _, item := range items {
		tmpStoreIndex, err := readStoreStoreIndexFromPath(ctx, item, client)

		if err != nil || (!tmpStoreIndex.IsValid()) {
			storeIndex.Dispose()
			if longtaillib.IsNotExist(err) {
				// The file we expected is no longer there, tell caller that we need to try again
				return longtaillib.Longtail_StoreIndex{}, nil, nil
			}
			return longtaillib.Longtail_StoreIndex{}, nil, errors.Wrap(err, fname)
		}
		if !storeIndex.IsValid() {
			storeIndex = tmpStoreIndex
			usedItems = append(usedItems, item)
			continue
		}
		mergedStoreIndex, err := longtaillib.MergeStoreIndex(storeIndex, tmpStoreIndex)
		tmpStoreIndex.Dispose()
		storeIndex.Dispose()
		if err != nil {
			err := errors.Wrap(err, "contentIndexWorker: longtaillib.MergeStoreIndex() failed")
			return longtaillib.Longtail_StoreIndex{}, nil, errors.Wrap(err, fname)
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
		"fname":  fname,
		"client": client.String(),
	})
	log.Debug(fname)

	for {
		items, err := getStoreStoreIndexes(ctx, client)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, nil, err
		}

		if len(items) == 0 {
			storeIndex, err := longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
			if err != nil {
				err := errors.Wrap(err, "Failed to create empty store index")
				return longtaillib.Longtail_StoreIndex{}, nil, errors.Wrapf(err, fname)
			}
			log.Info("created empty store index")
			return storeIndex, nil, nil
		} else {
			log.WithFields(logrus.Fields{"items": items}).Info("read store index items")
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
		log.Infof("Retrying reading remote store index")
	}
}

func addBlocksToStoreIndex(
	storeIndex longtaillib.Longtail_StoreIndex,
	addedBlockIndexes []longtaillib.Longtail_BlockIndex) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "addBlocksToStoreIndex"
	log := logrus.WithFields(logrus.Fields{
		"fname":                  fname,
		"len(addedBlockIndexes)": addedBlockIndexes,
	})
	log.Debug(fname)

	addedStoreIndex, err := longtaillib.CreateStoreIndexFromBlocks(addedBlockIndexes)
	if err != nil {
		err := errors.Wrap(err, "Failed to create store index from block indexes")
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
	}

	if !storeIndex.IsValid() {
		return addedStoreIndex, nil
	}
	updatedStoreIndex, err := longtaillib.MergeStoreIndex(storeIndex, addedStoreIndex)
	addedStoreIndex.Dispose()
	if err != nil {
		updatedStoreIndex.Dispose()
		err := errors.Wrap(err, fmt.Sprintf("Failed merging store index with %d blocks", len(addedBlockIndexes)))
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
	}
	return updatedStoreIndex, nil
}

func readStoreIndexFromURI(
	ctx context.Context,
	path string,
	blobStoreOptions ...longtailstorelib.BlobStoreOption) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "readStoreIndexFromURI"
	log := logrus.WithFields(logrus.Fields{
		"fname":            fname,
		"path":             path,
		"blobStoreOptions": blobStoreOptions,
	})
	log.Debug(fname)
	sbuffer, err := longtailutils.ReadFromURI(path, blobStoreOptions...)
	if err != nil {
		if longtaillib.IsNotExist(err) {
			log.WithError(err).Info("Failed reading store index")
			return longtaillib.Longtail_StoreIndex{}, err
		}
		err = errors.Wrap(err, "Cant parse store index")
		log.WithError(err).Info("Failed parsing store index")
		return longtaillib.Longtail_StoreIndex{}, err
	}

	storeIndex, err := longtaillib.ReadStoreIndexFromBuffer(sbuffer)
	if err != nil {
		err = errors.Wrap(err, "Cant parse store index")
		log.WithError(err).Info("Failed parsing optional store index")
		return longtaillib.Longtail_StoreIndex{}, err
	}
	return storeIndex, nil
}

func readRemoteStoreIndex(
	ctx context.Context,
	optionalStoreIndexPaths []string,
	blobStore longtailstorelib.BlobStore,
	client longtailstorelib.BlobClient,
	accessType AccessType,
	workerCount int,
	blobStoreOptions ...longtailstorelib.BlobStoreOption) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "readRemoteStoreIndex"
	log := logrus.WithFields(logrus.Fields{
		"fname":            fname,
		"blobStore":        blobStore.String(),
		"client":           client.String(),
		"accessType":       accessType,
		"workerCount":      workerCount,
		"blobStoreOptions": blobStoreOptions,
	})
	log.Debug(fname)

	var err error
	var storeIndex longtaillib.Longtail_StoreIndex
	if accessType == Init {
		log.Info("bulding store index from blocks")
		storeIndex, err = buildStoreIndexFromStoreBlocks(
			ctx,
			blobStore,
			client,
			workerCount)

		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		log.Infof("Rebuilt remote index with %d blocks", len(storeIndex.GetBlockHashes()))
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

	if accessType == ReadOnly && len(optionalStoreIndexPaths) > 0 {
		for index, optionalStoreIndexPath := range optionalStoreIndexPaths {
			oneStoreIndex, err := readStoreIndexFromURI(ctx, optionalStoreIndexPath, blobStoreOptions...)
			if err != nil {
				storeIndex.Dispose()
				break
			}
			if index == 0 {
				storeIndex = oneStoreIndex
			} else {
				mergedStoreIndex, err := longtaillib.MergeStoreIndex(storeIndex, oneStoreIndex)
				if err != nil {
					log.WithError(err).Infof("Failed merging in store index from '%s'", optionalStoreIndexPath)
					oneStoreIndex.Dispose()
					storeIndex.Dispose()
					break
				}
				storeIndex.Dispose()
				oneStoreIndex.Dispose()
				storeIndex = mergedStoreIndex
			}
		}
	}

	if storeIndex.IsValid() && err == nil {
		return storeIndex, nil
	}

	storeIndex, _, err = readStoreStoreIndexWithItems(ctx, client)
	if err == nil {
		return storeIndex, nil
	} else if !longtaillib.IsNotExist(err) {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	storeIndex, err = longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
	if err != nil {
		err := errors.Wrap(err, "Failed creating empty store index")
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	log.Info("No remote index found, using empty store index")
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
	optionalStoreIndexPaths []string,
	jobAPI longtaillib.Longtail_JobAPI,
	numWorkerCount int,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	accessType AccessType,
	enableFileMapping bool,
	opts ...longtailstorelib.BlobStoreOption) (longtaillib.Longtail_BlockStoreAPI, error) {
	const fname = "CreateBlockStoreForURI"
	log := logrus.WithFields(logrus.Fields{
		"fname":             fname,
		"numWorkerCount":    numWorkerCount,
		"targetBlockSize":   targetBlockSize,
		"maxChunksPerBlock": maxChunksPerBlock,
		"accessType":        accessType,
		"opts":              opts,
	})
	log.Debug(fname)

	// Special case since filepaths may not parse nicely as a url
	if strings.HasPrefix(uri, "fsblob://") {
		fsBlobStore, err := longtailstorelib.NewFSBlobStore(uri[len("fsblob://"):], true)
		if err != nil {
			return longtaillib.Longtail_BlockStoreAPI{}, errors.Wrap(err, fname)
		}

		if numWorkerCount == 0 {
			numWorkerCount = runtime.NumCPU()
		}

		fsBlockStore, err := NewRemoteBlockStore(
			jobAPI,
			fsBlobStore,
			optionalStoreIndexPaths,
			numWorkerCount,
			accessType,
			opts...)
		if err != nil {
			return longtaillib.Longtail_BlockStoreAPI{}, errors.Wrap(err, fname)
		}
		return longtaillib.CreateBlockStoreAPI(fsBlockStore), nil
	}

	blobStoreURL, err := url.Parse(uri)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			gcsBlobStore, err := longtailstorelib.NewGCSBlobStore(blobStoreURL, false)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, errors.Wrap(err, fname)
			}

			if numWorkerCount == 0 {
				numWorkerCount = runtime.NumCPU()
				if numWorkerCount > 8 {
					numWorkerCount = 8
				}
			}

			gcsBlockStore, err := NewRemoteBlockStore(
				jobAPI,
				gcsBlobStore,
				optionalStoreIndexPaths,
				numWorkerCount,
				accessType,
				opts...)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, errors.Wrap(err, fname)
			}
			return longtaillib.CreateBlockStoreAPI(gcsBlockStore), nil
		case "s3":
			s3BlobStore, err := longtailstorelib.NewS3BlobStore(blobStoreURL, opts...)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, errors.Wrap(err, fname)
			}

			if numWorkerCount == 0 {
				numWorkerCount = runtime.NumCPU()
				if numWorkerCount > 8 {
					numWorkerCount = 8
				}
			}

			s3BlockStore, err := NewRemoteBlockStore(
				jobAPI,
				s3BlobStore,
				optionalStoreIndexPaths,
				numWorkerCount,
				accessType,
				opts...)
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
			return longtaillib.CreateFSBlockStore(jobAPI, longtaillib.CreateFSStorageAPI(), blobStoreURL.Path[1:], ".lsb", enableFileMapping), nil
		}
	}
	return longtaillib.CreateFSBlockStore(jobAPI, longtaillib.CreateFSStorageAPI(), uri, ".lsb", enableFileMapping), nil
}
