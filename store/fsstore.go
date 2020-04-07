package store

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/DanEngelbrecht/golongtail/lib"
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
	storedBlock      lib.Longtail_StoredBlock
	asyncCompleteAPI lib.Longtail_AsyncPutStoredBlockAPI
}

type fsGetBlockMessage struct {
	blockHash        uint64
	asyncCompleteAPI lib.Longtail_AsyncGetStoredBlockAPI
}

type fsGetIndexMessage struct {
	defaultHashAPIIdentifier uint32
	asyncCompleteAPI         lib.Longtail_AsyncGetIndexAPI
}

type fsStopMessage struct {
}

type fsBlockStore struct {
	fsRoot       string
	fsBlockStore lib.Longtail_BlockStoreAPI
	jobAPI       lib.Longtail_JobAPI

	defaultHashAPI uint32
	workerCount    int

	putBlockChan chan fsPutBlockMessage
	getBlockChan chan fsGetBlockMessage
	getIndexChan chan fsGetIndexMessage
	stopChan     chan fsStopMessage

	workerWaitGroup sync.WaitGroup

	stats lib.BlockStoreStats
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
	fsStopMessages <-chan fsStopMessage) error {

	run := true
	for run {
		select {
		case putMsg := <-fsPutBlockMessages:
			errno := s.fsBlockStore.PutStoredBlock(putMsg.storedBlock, putMsg.asyncCompleteAPI)
			if errno != 0 {
				log.Printf("WARNING: PutStoredBlock returned: %d", errno)
			}
		case getMsg := <-fsGetBlockMessages:
			errno := s.fsBlockStore.GetStoredBlock(getMsg.blockHash, getMsg.asyncCompleteAPI)
			if errno != 0 {
				log.Printf("WARNING: GetStoredBlock returned: %d", errno)
			}
		case getMsg := <-fsGetIndexMessages:
			errno := s.fsBlockStore.GetIndex(getMsg.defaultHashAPIIdentifier, getMsg.asyncCompleteAPI)
			if errno != 0 {
				log.Printf("WARNING: GetStoredBlock returned: %d", errno)
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
func NewFSBlockStore(path string, defaultHashAPI uint32, jobAPI lib.Longtail_JobAPI) (lib.BlockStoreAPI, error) {
	s := &fsBlockStore{fsRoot: path, jobAPI: jobAPI, defaultHashAPI: defaultHashAPI}
	storageAPI := lib.CreateFSStorageAPI()
	s.fsBlockStore = lib.CreateFSBlockStoreAPI(storageAPI, path)
	s.workerCount = runtime.NumCPU() * 4
	s.putBlockChan = make(chan fsPutBlockMessage, s.workerCount*4096)
	s.getBlockChan = make(chan fsGetBlockMessage, s.workerCount*4096)
	s.getIndexChan = make(chan fsGetIndexMessage, 16)
	s.stopChan = make(chan fsStopMessage, s.workerCount)

	for i := 0; i < s.workerCount; i++ {
		go fsWorker(s, s.putBlockChan, s.getBlockChan, s.getIndexChan, s.stopChan)
	}
	s.workerWaitGroup.Add(s.workerCount)

	return s, nil
}

// PutStoredBlock ...
func (s *fsBlockStore) PutStoredBlock(storedBlock lib.Longtail_StoredBlock, asyncCompleteAPI lib.Longtail_AsyncPutStoredBlockAPI) int {
	if len(s.putBlockChan) < cap(s.putBlockChan) {
		s.putBlockChan <- fsPutBlockMessage{storedBlock: storedBlock, asyncCompleteAPI: asyncCompleteAPI}
		return 0
	}
	return s.fsBlockStore.PutStoredBlock(storedBlock, asyncCompleteAPI)
}

// GetStoredBlock ...
func (s *fsBlockStore) GetStoredBlock(blockHash uint64, asyncCompleteAPI lib.Longtail_AsyncGetStoredBlockAPI) int {
	if len(s.getBlockChan) < cap(s.getBlockChan) {
		s.getBlockChan <- fsGetBlockMessage{blockHash: blockHash, asyncCompleteAPI: asyncCompleteAPI}
		return 0
	}
	return s.fsBlockStore.GetStoredBlock(blockHash, asyncCompleteAPI)
}

// GetIndex ...
func (s *fsBlockStore) GetIndex(defaultHashAPIIdentifier uint32, asyncCompleteAPI lib.Longtail_AsyncGetIndexAPI) int {
	s.getIndexChan <- fsGetIndexMessage{defaultHashAPIIdentifier: defaultHashAPIIdentifier, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// GetStats ...
func (s *fsBlockStore) GetStats() (lib.BlockStoreStats, int) {
	return s.stats, 0
}

// Close ...
func (s *fsBlockStore) Close() {
	for i := 0; i < s.workerCount; i++ {
		s.stopChan <- fsStopMessage{}
	}
	s.workerWaitGroup.Wait()
}
