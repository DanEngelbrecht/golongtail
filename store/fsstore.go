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
	asyncCompleteAPI lib.Longtail_AsyncCompleteAPI
}

type fsGetBlockMessage struct {
	blockHash        uint64
	outStoredBlock   lib.Longtail_StoredBlockPtr
	asyncCompleteAPI lib.Longtail_AsyncCompleteAPI
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
	stopChan     chan fsStopMessage

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
	fsStopMessages <-chan fsStopMessage) error {
	for true {
		select {
		case putMsg := <-fsPutBlockMessages:
			errno := s.fsBlockStore.PutStoredBlock(putMsg.storedBlock, putMsg.asyncCompleteAPI)
			if errno != 0 {
				log.Printf("WARNING: PutStoredBlock returned: %d", errno)
			}
		case getMsg := <-fsGetBlockMessages:
			errno := s.fsBlockStore.GetStoredBlock(getMsg.blockHash, getMsg.outStoredBlock, getMsg.asyncCompleteAPI)
			if errno != 0 {
				log.Printf("WARNING: GetStoredBlock returned: %d", errno)
			}
		case _ = <-fsStopMessages:
			s.workerWaitGroup.Done()
			return nil
		}
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
	s.stopChan = make(chan fsStopMessage, s.workerCount)

	for i := 0; i < s.workerCount; i++ {
		go fsWorker(s, s.putBlockChan, s.getBlockChan, s.stopChan)
	}
	s.workerWaitGroup.Add(s.workerCount)

	return s, nil
}

// PutStoredBlock ...
func (s *fsBlockStore) PutStoredBlock(storedBlock lib.Longtail_StoredBlock, asyncCompleteAPI lib.Longtail_AsyncCompleteAPI) int {
	if asyncCompleteAPI.IsValid() {
		if len(s.putBlockChan) < cap(s.putBlockChan) {
			s.putBlockChan <- fsPutBlockMessage{storedBlock: storedBlock, asyncCompleteAPI: asyncCompleteAPI}
			return 0
		}
	}
	return s.fsBlockStore.PutStoredBlock(storedBlock, asyncCompleteAPI)
}

// GetStoredBlock ...
func (s *fsBlockStore) GetStoredBlock(blockHash uint64, outStoredBlock lib.Longtail_StoredBlockPtr, asyncCompleteAPI lib.Longtail_AsyncCompleteAPI) int {
	if asyncCompleteAPI.IsValid() {
		if len(s.getBlockChan) < cap(s.getBlockChan) {
			s.getBlockChan <- fsGetBlockMessage{blockHash: blockHash, outStoredBlock: outStoredBlock, asyncCompleteAPI: asyncCompleteAPI}
			return 0
		}
	}
	return s.fsBlockStore.GetStoredBlock(blockHash, outStoredBlock, asyncCompleteAPI)
}

// GetIndex ...
func (s *fsBlockStore) GetIndex(defaultHashAPIIdentifier uint32, jobAPI lib.Longtail_JobAPI, progress lib.Longtail_ProgressAPI) (lib.Longtail_ContentIndex, int) {
	return s.fsBlockStore.GetIndex(s.defaultHashAPI, s.jobAPI, &progress)
}

// GetStoredBlockPath ...
func (s *fsBlockStore) GetStoredBlockPath(blockHash uint64) (string, int) {
	return s.GetStoredBlockPath(blockHash)
}

// Close ...
func (s *fsBlockStore) Close() {
	for i := 0; i < s.workerCount; i++ {
		s.stopChan <- fsStopMessage{}
	}
	s.workerWaitGroup.Wait()
}
