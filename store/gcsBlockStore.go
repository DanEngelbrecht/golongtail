package store

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/DanEngelbrecht/golongtail/lib"
	"github.com/pkg/errors"
)

type putBlockMessage struct {
	storedBlock      lib.Longtail_StoredBlock
	asyncCompleteAPI lib.Longtail_AsyncCompleteAPI
}

type getBlockMessage struct {
	blockHash        uint64
	outStoredBlock   lib.Longtail_StoredBlockPtr
	asyncCompleteAPI lib.Longtail_AsyncCompleteAPI
}

type contentIndexMessage struct {
	contentIndex lib.Longtail_ContentIndex
}

type queryContentIndexMessage struct {
}

type responseContentIndexMessage struct {
	contentIndex lib.Longtail_ContentIndex
	errno        int
}

type stopMessage struct {
}

type gcsBlockStore struct {
	url           *url.URL
	Location      string
	defaultClient *storage.Client
	defaultBucket *storage.BucketHandle

	defaultHashAPI uint32
	workerCount    int

	putBlockChan             chan putBlockMessage
	getBlockChan             chan getBlockMessage
	contentIndexChan         chan contentIndexMessage
	queryContentIndexChan    chan queryContentIndexMessage
	responseContentIndexChan chan responseContentIndexMessage
	stopChan                 chan stopMessage

	workerWaitGroup sync.WaitGroup
}

func readContentIndex(storageAPI lib.Longtail_StorageAPI) (lib.Longtail_ContentIndex, int) {
	contentIndex, err := lib.ReadContentIndex(storageAPI, "fake_storage/store.lci")
	if err == nil {
		return contentIndex, 0
	} else {
		hashAPI := lib.CreateBlake3HashAPI()
		defer hashAPI.Dispose()
		contentIndex, _ := lib.CreateContentIndex(
			hashAPI,
			[]uint64{},
			[]uint32{},
			[]uint32{},
			32768,
			65536)
		return contentIndex, 0
	}
}

// String() ...
func (s *gcsBlockStore) String() string {
	return s.Location
}

func PutStoredBlock(
	ctx context.Context,
	s *gcsBlockStore,
	bucket *storage.BucketHandle,
	contentIndexMessages chan<- contentIndexMessage,
	storedBlock lib.Longtail_StoredBlock,
	asyncCompleteAPI lib.Longtail_AsyncCompleteAPI) int {
	blockIndex := storedBlock.GetBlockIndex()
	blockHash := blockIndex.GetBlockHash()
	key := getBlockPath("chunks", blockHash)
	objHandle := bucket.Object(key)
	_, err := objHandle.Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		blockIndexBytes, err := lib.WriteBlockIndexToBuffer(storedBlock.GetBlockIndex())
		if err != nil {
			return asyncCompleteAPI.OnComplete(lib.ENOMEM)
		}

		blockData := storedBlock.GetChunksBlockData()
		blob := append(blockIndexBytes, blockData...)

		objWriter := objHandle.NewWriter(ctx)
		_, err = objWriter.Write(blob)
		if err != nil {
			objWriter.Close()
			//		return errors.Wrap(err, s.String()+"/"+key)
			return asyncCompleteAPI.OnComplete(lib.EIO)
		}

		err = objWriter.Close()
		if err != nil {
			//		return errors.Wrap(err, s.String()+"/"+key)
			return asyncCompleteAPI.OnComplete(lib.EIO)
		}

		_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: "application/octet-stream"})
		if err != nil {
			return asyncCompleteAPI.OnComplete(lib.EIO)
		}
	}

	newBlocks := []lib.Longtail_BlockIndex{blockIndex}
	addedContentIndex, err := lib.CreateContentIndexFromBlocks(s.defaultHashAPI, uint64(len(newBlocks)), newBlocks)
	if err != nil {
		return asyncCompleteAPI.OnComplete(lib.ENOMEM)
	}
	contentIndexMessages <- contentIndexMessage{contentIndex: addedContentIndex}
	return asyncCompleteAPI.OnComplete(0)
}

func GetStoredBlock(
	ctx context.Context,
	s *gcsBlockStore,
	bucket *storage.BucketHandle,
	blockHash uint64,
	outStoredBlock lib.Longtail_StoredBlockPtr,
	asyncCompleteAPI lib.Longtail_AsyncCompleteAPI) int {

	key := getBlockPath("chunks", blockHash)
	objHandle := bucket.Object(key)
	if !outStoredBlock.HasPtr() {
		_, err := objHandle.Attrs(ctx)
		if err == storage.ErrObjectNotExist {
			return asyncCompleteAPI.OnComplete(lib.ENOENT)
		}
		return asyncCompleteAPI.OnComplete(0)
	}
	obj, err := objHandle.NewReader(ctx)
	if err != nil {
		return asyncCompleteAPI.OnComplete(lib.ENOMEM)
	}
	defer obj.Close()

	storedBlockData, err := ioutil.ReadAll(obj)

	if err != nil {
		return asyncCompleteAPI.OnComplete(lib.EIO)
	}

	storedBlock, err := lib.InitStoredBlockFromData(storedBlockData)
	if err != nil {
		return asyncCompleteAPI.OnComplete(lib.ENOMEM)
	}
	outStoredBlock.Set(storedBlock)
	return asyncCompleteAPI.OnComplete(0)
}

func gcsWorker(
	ctx context.Context,
	s *gcsBlockStore,
	u *url.URL,
	putBlockMessages <-chan putBlockMessage,
	getBlockMessages <-chan getBlockMessage,
	contentIndexMessages chan<- contentIndexMessage,
	stopMessages <-chan stopMessage) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return errors.Wrap(err, u.String())
	}
	bucketName := u.Host
	bucket := client.Bucket(bucketName)

	for true {
		select {
		case putMsg := <-putBlockMessages:
			errno := PutStoredBlock(ctx, s, bucket, contentIndexMessages, putMsg.storedBlock, putMsg.asyncCompleteAPI)
			if errno != 0 {
				log.Printf("WARNING: PutStoredBlock returned: %d", errno)
			}
		case getMsg := <-getBlockMessages:
			errno := GetStoredBlock(ctx, s, bucket, getMsg.blockHash, getMsg.outStoredBlock, getMsg.asyncCompleteAPI)
			if errno != 0 {
				log.Printf("WARNING: GetStoredBlock returned: %d", errno)
			}
		case _ = <-stopMessages:
			s.workerWaitGroup.Done()
			return nil
		}
	}
	s.workerWaitGroup.Done()
	return nil
}

func contentIndexWorker(
	s *gcsBlockStore,
	contentIndexMessages <-chan contentIndexMessage,
	queryContentIndexMessages <-chan queryContentIndexMessage,
	responseContentIndexMessages chan<- responseContentIndexMessage,
	stopMessages <-chan stopMessage) error {
	backingStorage := lib.CreateFSStorageAPI()
	defer backingStorage.Dispose()
	contentIndex, errno := readContentIndex(backingStorage)
	if errno != 0 {
		log.Printf("WARNING: Failed to read store content index: %d", errno)
	}
	contentIndexChanged := false
	for true {
		select {
		case contentIndexMsg := <-contentIndexMessages:
			newContentIndex, err := lib.MergeContentIndex(contentIndex, contentIndexMsg.contentIndex)
			contentIndexMsg.contentIndex.Dispose()
			if err == nil {
				contentIndex.Dispose()
				contentIndex = newContentIndex
				contentIndexChanged = true
			}
		case _ = <-queryContentIndexMessages:
			{
				responseContentIndexMsg := responseContentIndexMessage{errno: lib.ENOMEM}
				buf, err := lib.WriteContentIndexToBuffer(contentIndex)
				if err == nil {
					contentIndexCopy, err := lib.ReadContentIndexFromBuffer(buf)
					if err == nil {
						responseContentIndexMsg = responseContentIndexMessage{contentIndex: contentIndexCopy, errno: 0}
					}
				}
				responseContentIndexMessages <- responseContentIndexMsg
			}
		case _ = <-stopMessages:
			if contentIndexChanged {
				err := lib.WriteContentIndex(backingStorage, contentIndex, "fake_storage/store.lci")
				if err != nil {
					log.Printf("WARNING: Failed to write store content index: %q", err)
				}
			}
			s.workerWaitGroup.Done()
			return nil
		}
	}
	s.workerWaitGroup.Done()
	return nil
}

// NewGCSBlockStore ...
func NewGCSBlockStore(u *url.URL, defaultHashAPI uint32) (lib.BlockStoreAPI, error) {
	//	var err error
	if u.Scheme != "gs" {
		return nil, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	ctx := context.Background()
	defaultClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, u.String())
	}

	bucketName := u.Host
	defaultBucket := defaultClient.Bucket(bucketName)

	//	backingStorage := lib.CreateFSStorageAPI()

	s := &gcsBlockStore{url: u, Location: u.String(), defaultClient: defaultClient, defaultBucket: defaultBucket, defaultHashAPI: defaultHashAPI} //, backingStorage: backingStorage}
	s.workerCount = runtime.NumCPU() * 4
	s.putBlockChan = make(chan putBlockMessage, s.workerCount*4096)
	s.getBlockChan = make(chan getBlockMessage, s.workerCount*4096)
	s.contentIndexChan = make(chan contentIndexMessage, s.workerCount*4096)
	s.queryContentIndexChan = make(chan queryContentIndexMessage)
	s.responseContentIndexChan = make(chan responseContentIndexMessage)
	s.stopChan = make(chan stopMessage, s.workerCount)

	go contentIndexWorker(s, s.contentIndexChan, s.queryContentIndexChan, s.responseContentIndexChan, s.stopChan)
	s.workerWaitGroup.Add(1)
	for i := 0; i < s.workerCount; i++ {
		go gcsWorker(ctx, s, u, s.putBlockChan, s.getBlockChan, s.contentIndexChan, s.stopChan)
	}
	s.workerWaitGroup.Add(s.workerCount)

	return s, nil
}

func getBlockPath(basePath string, blockHash uint64) string {
	sID := fmt.Sprintf("%x", blockHash)
	dir := filepath.Join(basePath, sID[0:4])
	name := filepath.Join(dir, sID) + ".lsb"
	name = strings.Replace(name, "\\", "/", -1)
	return name
}

// PutStoredBlock ...
func (s *gcsBlockStore) PutStoredBlock(storedBlock lib.Longtail_StoredBlock, asyncCompleteAPI lib.Longtail_AsyncCompleteAPI) int {
	if asyncCompleteAPI.IsValid() {
		if len(s.putBlockChan) < cap(s.putBlockChan) {
			s.putBlockChan <- putBlockMessage{storedBlock: storedBlock, asyncCompleteAPI: asyncCompleteAPI}
			return 0
		}
	}
	return PutStoredBlock(context.Background(), s, s.defaultBucket, s.contentIndexChan, storedBlock, asyncCompleteAPI)
}

// GetStoredBlock ...
func (s *gcsBlockStore) GetStoredBlock(blockHash uint64, outStoredBlock lib.Longtail_StoredBlockPtr, asyncCompleteAPI lib.Longtail_AsyncCompleteAPI) int {
	if asyncCompleteAPI.IsValid() {
		if len(s.getBlockChan) < cap(s.getBlockChan) {
			s.getBlockChan <- getBlockMessage{blockHash: blockHash, outStoredBlock: outStoredBlock, asyncCompleteAPI: asyncCompleteAPI}
			return 0
		}
	}
	return GetStoredBlock(context.Background(), s, s.defaultBucket, blockHash, outStoredBlock, asyncCompleteAPI)
}

// GetIndex ...
func (s *gcsBlockStore) GetIndex(defaultHashAPIIdentifier uint32, jobAPI lib.Longtail_JobAPI, progress lib.Longtail_ProgressAPI) (lib.Longtail_ContentIndex, int) {
	s.queryContentIndexChan <- queryContentIndexMessage{}
	responseContentIndexMsg := <-s.responseContentIndexChan
	return responseContentIndexMsg.contentIndex, responseContentIndexMsg.errno
}

// GetStoredBlockPath ...
func (s *gcsBlockStore) GetStoredBlockPath(blockHash uint64) (string, int) {
	return getBlockPath("chunks", blockHash), 0
}

// Close ...
func (s *gcsBlockStore) Close() {
	for i := 0; i < s.workerCount+1; i++ {
		s.stopChan <- stopMessage{}
	}
	s.workerWaitGroup.Wait()
}
