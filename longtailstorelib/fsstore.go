package longtailstorelib

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
)

type fsFileStorage struct {
}

func (fileStorage *fsFileStorage) ReadFromPath(ctx context.Context, path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}

func (fileStorage *fsFileStorage) WriteToPath(ctx context.Context, path string, data []byte) error {
	err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, data, 0644)
}

func (fileStorage *fsFileStorage) Close() {
}

// NewFSFileStorage ...
func NewFSFileStorage() (FileStorage, error) {
	s := &fsFileStorage{}
	return s, nil
}

type fsPutBlockMessage struct {
	storedBlock      longtaillib.Longtail_StoredBlock
	asyncCompleteAPI longtaillib.Longtail_AsyncPutStoredBlockAPI
}

type fsGetBlockMessage struct {
	blockHash        uint64
	asyncCompleteAPI longtaillib.Longtail_AsyncGetStoredBlockAPI
}

type fsGetIndexMessage struct {
	asyncCompleteAPI longtaillib.Longtail_AsyncGetIndexAPI
}

type fsRetargetContentMessage struct {
	contentIndex     longtaillib.Longtail_ContentIndex
	asyncCompleteAPI longtaillib.Longtail_AsyncRetargetContentAPI
}

type fsStopMessage struct {
}

type fsBlockStore struct {
	fsRoot       string
	fsBlockStore longtaillib.Longtail_BlockStoreAPI
	jobAPI       longtaillib.Longtail_JobAPI

	workerCount int

	putBlockChan        chan fsPutBlockMessage
	getBlockChan        chan fsGetBlockMessage
	getIndexChan        chan fsGetIndexMessage
	retargetContentChan chan fsRetargetContentMessage
	stopChan            chan fsStopMessage

	workerWaitGroup sync.WaitGroup
}

// String() ...
func (s *fsBlockStore) String() string {
	return s.fsRoot
}

func fsWorker(
	s *fsBlockStore,
	fsPutBlockMessages <-chan fsPutBlockMessage,
	fsGetBlockMessages <-chan fsGetBlockMessage,
	fsGetIndexMessages <-chan fsGetIndexMessage,
	fsRetargetContentMessages <-chan fsRetargetContentMessage,
	fsStopMessages <-chan fsStopMessage) error {

	run := true
	for run {
		select {
		case putMsg := <-fsPutBlockMessages:
			errno := s.fsBlockStore.PutStoredBlock(putMsg.storedBlock, putMsg.asyncCompleteAPI)
			if errno != 0 {
				putMsg.asyncCompleteAPI.OnComplete(errno)
			}
		case getMsg := <-fsGetBlockMessages:
			errno := s.fsBlockStore.GetStoredBlock(getMsg.blockHash, getMsg.asyncCompleteAPI)
			if errno != 0 {
				getMsg.asyncCompleteAPI.OnComplete(longtaillib.Longtail_StoredBlock{}, errno)
			}
		case indexMsg := <-fsGetIndexMessages:
			errno := s.fsBlockStore.GetIndex(indexMsg.asyncCompleteAPI)
			if errno != 0 {
				indexMsg.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
			}
		case retargetMsg := <-fsRetargetContentMessages:
			errno := s.fsBlockStore.RetargetContent(retargetMsg.contentIndex, retargetMsg.asyncCompleteAPI)
			if errno != 0 {
				retargetMsg.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
			}
		case _ = <-fsStopMessages:
			run = false
		}
	}

	select {
	case putMsg := <-fsPutBlockMessages:
		errno := s.fsBlockStore.PutStoredBlock(putMsg.storedBlock, putMsg.asyncCompleteAPI)
		if errno != 0 {
			log.Panicf("WARNING: putStoredBlock returned: %d", errno)
		}
	default:
	}

	s.workerWaitGroup.Done()
	return nil
}

// NewFSBlockStore ...
func NewFSBlockStore(path string, jobAPI longtaillib.Longtail_JobAPI, targetBlockSize uint32, maxChunksPerBlock uint32) (longtaillib.BlockStoreAPI, error) {
	s := &fsBlockStore{fsRoot: path, jobAPI: jobAPI}
	storageAPI := longtaillib.CreateFSStorageAPI()
	s.fsBlockStore = longtaillib.CreateFSBlockStore(storageAPI, path, targetBlockSize, maxChunksPerBlock)
	s.workerCount = runtime.NumCPU() * 4
	s.putBlockChan = make(chan fsPutBlockMessage, s.workerCount*4096)
	s.getBlockChan = make(chan fsGetBlockMessage, s.workerCount*4096)
	s.getIndexChan = make(chan fsGetIndexMessage, 16)
	s.retargetContentChan = make(chan fsRetargetContentMessage, 16)
	s.stopChan = make(chan fsStopMessage, s.workerCount)

	for i := 0; i < s.workerCount; i++ {
		go fsWorker(s, s.putBlockChan, s.getBlockChan, s.getIndexChan, s.retargetContentChan, s.stopChan)
	}
	s.workerWaitGroup.Add(s.workerCount)

	return s, nil
}

// PutStoredBlock ...
func (s *fsBlockStore) PutStoredBlock(storedBlock longtaillib.Longtail_StoredBlock, asyncCompleteAPI longtaillib.Longtail_AsyncPutStoredBlockAPI) int {
	if len(s.putBlockChan) < cap(s.putBlockChan) {
		s.putBlockChan <- fsPutBlockMessage{storedBlock: storedBlock, asyncCompleteAPI: asyncCompleteAPI}
		return 0
	}
	return s.fsBlockStore.PutStoredBlock(storedBlock, asyncCompleteAPI)
}

// PreflightGet ...
func (s *fsBlockStore) PreflightGet(blockCount uint64, hashes []uint64, refCounts []uint32) int {
	return 0
}

// GetStoredBlock ...
func (s *fsBlockStore) GetStoredBlock(blockHash uint64, asyncCompleteAPI longtaillib.Longtail_AsyncGetStoredBlockAPI) int {
	if len(s.getBlockChan) < cap(s.getBlockChan) {
		s.getBlockChan <- fsGetBlockMessage{blockHash: blockHash, asyncCompleteAPI: asyncCompleteAPI}
		return 0
	}
	return s.fsBlockStore.GetStoredBlock(blockHash, asyncCompleteAPI)
}

// GetIndex ...
func (s *fsBlockStore) GetIndex(asyncCompleteAPI longtaillib.Longtail_AsyncGetIndexAPI) int {
	s.getIndexChan <- fsGetIndexMessage{asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// RetargetContent ...
func (s *fsBlockStore) RetargetContent(
	contentIndex longtaillib.Longtail_ContentIndex,
	asyncCompleteAPI longtaillib.Longtail_AsyncRetargetContentAPI) int {
	s.retargetContentChan <- fsRetargetContentMessage{contentIndex: contentIndex, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// GetStats ...
func (s *fsBlockStore) GetStats() (longtaillib.BlockStoreStats, int) {
	return s.fsBlockStore.GetStats()
}

// Close ...
func (s *fsBlockStore) Close() {
	for i := 0; i < s.workerCount; i++ {
		s.stopChan <- fsStopMessage{}
	}
	s.workerWaitGroup.Wait()
}
