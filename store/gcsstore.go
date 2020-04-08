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
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/DanEngelbrecht/golongtail/lib"
	"github.com/pkg/errors"
)

type gcsFileStorage struct {
}

func (fileStorage *gcsFileStorage) ReadFromPath(ctx context.Context, path string) ([]byte, error) {
	u, err := url.Parse(path)
	if u.Scheme != "gs" {
		return nil, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bucketName := u.Host
	bucket := client.Bucket(bucketName)

	objHandle := bucket.Object(u.Path[1:])
	objReader, err := objHandle.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer objReader.Close()

	return ioutil.ReadAll(objReader)
}

func (fileStorage *gcsFileStorage) WriteToPath(ctx context.Context, path string, data []byte) error {
	u, err := url.Parse(path)
	if u.Scheme != "gs" {
		return fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	bucketName := u.Host
	bucket := client.Bucket(bucketName)

	objHandle := bucket.Object(u.Path[1:])
	{
		objWriter := objHandle.NewWriter(ctx)
		_, err := objWriter.Write(data)
		objWriter.Close()
		if err != nil {
			return err
		}
	}
	_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: "application/octet-stream"})
	return err
}

func (fileStorage *gcsFileStorage) Close() {
}

// NewGCSFileStorage ...
func NewGCSFileStorage(u *url.URL) (FileStorage, error) {
	if u.Scheme != "gs" {
		return nil, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	s := &gcsFileStorage{}
	return s, nil
}

type putBlockMessage struct {
	storedBlock      lib.Longtail_StoredBlock
	asyncCompleteAPI lib.Longtail_AsyncPutStoredBlockAPI
}

type getBlockMessage struct {
	blockHash        uint64
	asyncCompleteAPI lib.Longtail_AsyncGetStoredBlockAPI
}

type getIndexMessage struct {
	defaultHashAPIIdentifier uint32
	asyncCompleteAPI         lib.Longtail_AsyncGetIndexAPI
}

type contentIndexMessage struct {
	contentIndex lib.Longtail_ContentIndex
}

type stopMessage struct {
}

type gcsBlockStore struct {
	url               *url.URL
	Location          string
	prefix            string
	maxBlockSize      uint32
	maxChunksPerBlock uint32
	defaultClient     *storage.Client
	defaultBucket     *storage.BucketHandle

	defaultHashAPI lib.Longtail_HashAPI
	workerCount    int

	putBlockChan     chan putBlockMessage
	getBlockChan     chan getBlockMessage
	contentIndexChan chan contentIndexMessage
	getIndexChan     chan getIndexMessage
	stopChan         chan stopMessage

	workerWaitGroup sync.WaitGroup

	stats lib.BlockStoreStats
}

// String() ...
func (s *gcsBlockStore) String() string {
	return s.Location
}

func putBlob(ctx context.Context, objHandle *storage.ObjectHandle, blob []byte) int {
	objWriter := objHandle.NewWriter(ctx)
	_, err := objWriter.Write(blob)
	if err != nil {
		objWriter.Close()
		//		return errors.Wrap(err, s.String()+"/"+key)
		return lib.EIO
	}

	err = objWriter.Close()
	if err != nil {
		//		return errors.Wrap(err, s.String()+"/"+key)
		return lib.EIO
	}

	_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: "application/octet-stream"})
	if err != nil {
		return lib.EIO
	}
	return 0
}

func getBlob(ctx context.Context, objHandle *storage.ObjectHandle) ([]byte, int) {
	obj, err := objHandle.NewReader(ctx)
	if err != nil {
		return nil, lib.ENOMEM
	}
	defer obj.Close()

	storedBlockData, err := ioutil.ReadAll(obj)

	if err != nil {
		return nil, lib.EIO
	}

	return storedBlockData, 0
}

func putStoredBlock(
	ctx context.Context,
	s *gcsBlockStore,
	bucket *storage.BucketHandle,
	contentIndexMessages chan<- contentIndexMessage,
	storedBlock lib.Longtail_StoredBlock,
	asyncCompleteAPI lib.Longtail_AsyncPutStoredBlockAPI) int {
	blockIndex := storedBlock.GetBlockIndex()
	blockHash := blockIndex.GetBlockHash()
	key := getBlockPath(s.prefix+"chunks", blockHash)
	objHandle := bucket.Object(key)
	_, err := objHandle.Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		blob, err := lib.WriteStoredBlockToBuffer(storedBlock)
		if err != nil {
			return asyncCompleteAPI.OnComplete(lib.ENOMEM)
		}

		errno := putBlob(ctx, objHandle, blob)
		if errno != 0 {
			log.Printf("Retrying putBlob %s", key)
			atomic.AddUint64(&s.stats.BlockPutRetryCount, 1)
			errno = putBlob(ctx, objHandle, blob)
		}
		if errno != 0 {
			log.Printf("Retrying 500 ms delayed putBlob %s", key)
			time.Sleep(500 * time.Millisecond)
			atomic.AddUint64(&s.stats.BlockPutRetryCount, 1)
			errno = putBlob(ctx, objHandle, blob)
		}
		if errno != 0 {
			log.Printf("Retrying 2 s delayed putBlob %s", key)
			time.Sleep(2 * time.Second)
			atomic.AddUint64(&s.stats.BlockPutRetryCount, 1)
			errno = putBlob(ctx, objHandle, blob)
		}

		if errno != 0 {
			atomic.AddUint64(&s.stats.BlockPutFailCount, 1)
			return asyncCompleteAPI.OnComplete(lib.ENOMEM)
		}

		atomic.AddUint64(&s.stats.BlocksPutCount, 1)
		atomic.AddUint64(&s.stats.BytesPutCount, (uint64)(len(blob)))
		atomic.AddUint64(&s.stats.ChunksPutCount, (uint64)(blockIndex.GetChunkCount()))
	}

	newBlocks := []lib.Longtail_BlockIndex{blockIndex}
	addedContentIndex, err := lib.CreateContentIndexFromBlocks(s.defaultHashAPI.GetIdentifier(), s.maxBlockSize, s.maxChunksPerBlock, newBlocks)
	if err != nil {
		return asyncCompleteAPI.OnComplete(lib.ENOMEM)
	}
	contentIndexMessages <- contentIndexMessage{contentIndex: addedContentIndex}
	return asyncCompleteAPI.OnComplete(0)
}

func getStoredBlock(
	ctx context.Context,
	s *gcsBlockStore,
	bucket *storage.BucketHandle,
	blockHash uint64,
	asyncCompleteAPI lib.Longtail_AsyncGetStoredBlockAPI) int {

	key := getBlockPath(s.prefix+"chunks", blockHash)
	objHandle := bucket.Object(key)

	storedBlockData, errno := getBlob(ctx, objHandle)
	if errno != 0 {
		log.Printf("Retrying getBlob %s", key)
		atomic.AddUint64(&s.stats.BlockGetRetryCount, 1)
		storedBlockData, errno = getBlob(ctx, objHandle)
	}
	if errno != 0 {
		log.Printf("Retrying 500 ms delayed getBlob %s", key)
		time.Sleep(500 * time.Millisecond)
		atomic.AddUint64(&s.stats.BlockGetRetryCount, 1)
		storedBlockData, errno = getBlob(ctx, objHandle)
	}
	if errno != 0 {
		log.Printf("Retrying 2 s delayed getBlob %s", key)
		time.Sleep(2 * time.Second)
		atomic.AddUint64(&s.stats.BlockGetRetryCount, 1)
		storedBlockData, errno = getBlob(ctx, objHandle)
	}

	if errno != 0 {
		atomic.AddUint64(&s.stats.BlockGetFailCount, 1)
		return asyncCompleteAPI.OnComplete(lib.Longtail_StoredBlock{}, lib.EIO)
	}

	storedBlock, err := lib.ReadStoredBlockFromBuffer(storedBlockData)
	if err != nil {
		return asyncCompleteAPI.OnComplete(lib.Longtail_StoredBlock{}, lib.ENOMEM)
	}

	atomic.AddUint64(&s.stats.BlocksGetCount, 1)
	atomic.AddUint64(&s.stats.BytesGetCount, (uint64)(len(storedBlockData)))
	blockIndex := storedBlock.GetBlockIndex()
	atomic.AddUint64(&s.stats.ChunksGetCount, (uint64)(blockIndex.GetChunkCount()))
	return asyncCompleteAPI.OnComplete(storedBlock, 0)
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

	run := true
	for run {
		select {
		case putMsg := <-putBlockMessages:
			errno := putStoredBlock(ctx, s, bucket, contentIndexMessages, putMsg.storedBlock, putMsg.asyncCompleteAPI)
			if errno != 0 {
				log.Panicf("WARNING: putStoredBlock returned: %d", errno)
			}
		case getMsg := <-getBlockMessages:
			errno := getStoredBlock(ctx, s, bucket, getMsg.blockHash, getMsg.asyncCompleteAPI)
			if errno != 0 {
				log.Panicf("WARNING: getStoredBlock returned: %d", errno)
			}
		case _ = <-stopMessages:
			run = false
		}
	}

	select {
	case putMsg := <-putBlockMessages:
		errno := putStoredBlock(ctx, s, bucket, contentIndexMessages, putMsg.storedBlock, putMsg.asyncCompleteAPI)
		if errno != 0 {
			log.Panicf("WARNING: putStoredBlock returned: %d", errno)
		}
	default:
	}

	s.workerWaitGroup.Done()
	return nil
}

func updateRemoteContentIndex(
	ctx context.Context,
	bucket *storage.BucketHandle,
	prefix string,
	addedContentIndex lib.Longtail_ContentIndex) error {
	storeBlob, err := lib.WriteContentIndexToBuffer(addedContentIndex)
	if err != nil {
		return err
	}
	key := prefix + "store.lci"
	objHandle := bucket.Object(key)
	for {
		writeCondition := storage.Conditions{DoesNotExist: true}
		objAttrs, _ := objHandle.Attrs(ctx)
		if objAttrs != nil {
			writeCondition = storage.Conditions{GenerationMatch: objAttrs.Generation}
			reader, err := objHandle.If(writeCondition).NewReader(ctx)
			if err != nil {
				return err
			}
			if reader == nil {
				continue
			}
			blob, err := ioutil.ReadAll(reader)
			reader.Close()
			if err != nil {
				return err
			}

			remoteContentIndex, err := lib.ReadContentIndexFromBuffer(blob)
			if err != nil {
				return err
			}
			defer remoteContentIndex.Dispose()
			mergedContentIndex, err := lib.MergeContentIndex(remoteContentIndex, addedContentIndex)
			if err != nil {
				return err
			}
			defer mergedContentIndex.Dispose()

			storeBlob, err = lib.WriteContentIndexToBuffer(mergedContentIndex)
			if err != nil {
				return err
			}
		}
		writer := objHandle.If(writeCondition).NewWriter(ctx)
		if writer == nil {
			continue
		}
		_, err = writer.Write(storeBlob)
		if err != nil {
			writer.CloseWithError(err)
			return err
		}
		writer.Close()
		_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: "application/octet-stream"})
		if err != nil {
			return err
		}
		break
	}
	return nil
}

func contentIndexWorker(
	ctx context.Context,
	s *gcsBlockStore,
	u *url.URL,
	contentIndexMessages <-chan contentIndexMessage,
	getIndexMessages <-chan getIndexMessage,
	stopMessages <-chan stopMessage) error {

	client, err := storage.NewClient(ctx)
	if err != nil {
		return errors.Wrap(err, u.String())
	}
	bucketName := u.Host
	bucket := client.Bucket(bucketName)

	var contentIndex lib.Longtail_ContentIndex

	key := s.prefix + "store.lci"
	objHandle := bucket.Object(key)
	obj, err := objHandle.NewReader(ctx)
	if err == nil {
		defer obj.Close()
		storedContentIndexData, err := ioutil.ReadAll(obj)
		if err == nil {
			contentIndex, err = lib.ReadContentIndexFromBuffer(storedContentIndexData)
		}
	}

	if err != nil {
		hashAPI := lib.CreateBlake3HashAPI()
		defer hashAPI.Dispose()
		contentIndex, err = lib.CreateContentIndex(
			s.defaultHashAPI,
			[]uint64{},
			[]uint32{},
			[]uint32{},
			s.maxBlockSize,
			s.maxChunksPerBlock)
		if err != nil {
			return err
		}
	}

	// TODO: Might need safer update of these two fields?
	s.maxBlockSize = contentIndex.GetMaxBlockSize()
	s.maxChunksPerBlock = contentIndex.GetMaxChunksPerBlock()

	addedContentIndex, err := lib.CreateContentIndex(
		s.defaultHashAPI,
		[]uint64{},
		[]uint32{},
		[]uint32{},
		s.maxBlockSize,
		s.maxChunksPerBlock)
	if err != nil {
		return err
	}

	defer contentIndex.Dispose()
	defer addedContentIndex.Dispose()

	run := true
	for run {
		select {
		case contentIndexMsg := <-contentIndexMessages:
			newAddedContentIndex, err := lib.MergeContentIndex(addedContentIndex, contentIndexMsg.contentIndex)
			if err != nil {
				log.Panicf("ERROR: MergeContentIndex returned: %q", err)
				continue
			}
			addedContentIndex.Dispose()
			addedContentIndex = newAddedContentIndex
			contentIndexMsg.contentIndex.Dispose()
		case getIndexMessage := <-getIndexMessages:
			contentIndexCopy, err := lib.MergeContentIndex(contentIndex, addedContentIndex)
			if err != nil {
				log.Panicf("ERROR: MergeContentIndex returned: %q", err)
				continue
			}
			errno := getIndexMessage.asyncCompleteAPI.OnComplete(contentIndexCopy, 0)
			if errno != 0 {
				contentIndexCopy.Dispose()
			}
			atomic.AddUint64(&s.stats.IndexGetCount, 1)
		case _ = <-stopMessages:
			run = false
		}
	}

	select {
	case contentIndexMsg := <-contentIndexMessages:
		newAddedContentIndex, err := lib.MergeContentIndex(addedContentIndex, contentIndexMsg.contentIndex)
		if err != nil {
			log.Panicf("ERROR: MergeContentIndex returned: %q", err)
		}
		addedContentIndex.Dispose()
		addedContentIndex = newAddedContentIndex
		contentIndexMsg.contentIndex.Dispose()
	default:
	}

	if addedContentIndex.GetBlockCount() > 0 {
		err := updateRemoteContentIndex(ctx, bucket, s.prefix, addedContentIndex)
		if err != nil {
			log.Printf("WARNING: Failed to write store content index: %q", err)
		}
	}
	s.workerWaitGroup.Done()
	return nil
}

// NewGCSBlockStore ...
func NewGCSBlockStore(u *url.URL, defaultHashAPI lib.Longtail_HashAPI, maxBlockSize uint32, maxChunksPerBlock uint32) (lib.BlockStoreAPI, error) {
	if u.Scheme != "gs" {
		return nil, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	ctx := context.Background()
	defaultClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, u.String())
	}

	prefix := u.Path[1:] // strip initial slash

	if prefix != "" {
		prefix += "/"
	}
	bucketName := u.Host
	defaultBucket := defaultClient.Bucket(bucketName)

	s := &gcsBlockStore{url: u, Location: u.String(), prefix: prefix, maxBlockSize: maxBlockSize, maxChunksPerBlock: maxChunksPerBlock, defaultClient: defaultClient, defaultBucket: defaultBucket, defaultHashAPI: defaultHashAPI}
	s.workerCount = runtime.NumCPU()
	s.putBlockChan = make(chan putBlockMessage, s.workerCount*2048)
	s.getBlockChan = make(chan getBlockMessage, s.workerCount*2048)
	s.contentIndexChan = make(chan contentIndexMessage, s.workerCount*2048)
	s.getIndexChan = make(chan getIndexMessage)
	s.stopChan = make(chan stopMessage, s.workerCount)

	go contentIndexWorker(ctx, s, u, s.contentIndexChan, s.getIndexChan, s.stopChan)
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
func (s *gcsBlockStore) PutStoredBlock(storedBlock lib.Longtail_StoredBlock, asyncCompleteAPI lib.Longtail_AsyncPutStoredBlockAPI) int {
	s.putBlockChan <- putBlockMessage{storedBlock: storedBlock, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// GetStoredBlock ...
func (s *gcsBlockStore) GetStoredBlock(blockHash uint64, asyncCompleteAPI lib.Longtail_AsyncGetStoredBlockAPI) int {
	s.getBlockChan <- getBlockMessage{blockHash: blockHash, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// GetIndex ...
func (s *gcsBlockStore) GetIndex(defaultHashAPIIdentifier uint32, asyncCompleteAPI lib.Longtail_AsyncGetIndexAPI) int {
	s.getIndexChan <- getIndexMessage{defaultHashAPIIdentifier: defaultHashAPIIdentifier, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// GetStats ...
func (s *gcsBlockStore) GetStats() (lib.BlockStoreStats, int) {
	return s.stats, 0
}

// Close ...
func (s *gcsBlockStore) Close() {
	for i := 0; i < s.workerCount+1; i++ {
		s.stopChan <- stopMessage{}
	}
	s.workerWaitGroup.Wait()
}
