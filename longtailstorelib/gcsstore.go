package longtailstorelib

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
	"github.com/DanEngelbrecht/golongtail/longtaillib"
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
	storedBlock      longtaillib.Longtail_StoredBlock
	asyncCompleteAPI longtaillib.Longtail_AsyncPutStoredBlockAPI
}

type getBlockMessage struct {
	blockHash        uint64
	asyncCompleteAPI longtaillib.Longtail_AsyncGetStoredBlockAPI
}

type getIndexMessage struct {
	asyncCompleteAPI longtaillib.Longtail_AsyncGetIndexAPI
}

type contentIndexMessage struct {
	contentIndex longtaillib.Longtail_ContentIndex
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

	workerCount int

	putBlockChan     chan putBlockMessage
	getBlockChan     chan getBlockMessage
	contentIndexChan chan contentIndexMessage
	getIndexChan     chan getIndexMessage
	workerStopChan   chan stopMessage
	indexStopChan    chan stopMessage

	workerWaitGroup      sync.WaitGroup
	indexWorkerWaitGroup sync.WaitGroup

	stats         longtaillib.BlockStoreStats
	outFinalStats *longtaillib.BlockStoreStats
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
		return longtaillib.EIO
	}

	err = objWriter.Close()
	if err != nil {
		//		return errors.Wrap(err, s.String()+"/"+key)
		return longtaillib.EIO
	}

	_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: "application/octet-stream"})
	if err != nil {
		return longtaillib.EIO
	}
	return 0
}

func getBlob(ctx context.Context, objHandle *storage.ObjectHandle) ([]byte, int) {
	obj, err := objHandle.NewReader(ctx)
	if err != nil {
		return nil, longtaillib.ENOMEM
	}
	defer obj.Close()

	storedBlockData, err := ioutil.ReadAll(obj)

	if err != nil {
		return nil, longtaillib.EIO
	}

	return storedBlockData, 0
}

func putStoredBlock(
	ctx context.Context,
	s *gcsBlockStore,
	bucket *storage.BucketHandle,
	contentIndexMessages chan<- contentIndexMessage,
	storedBlock longtaillib.Longtail_StoredBlock) int {
	blockIndex := storedBlock.GetBlockIndex()
	blockHash := blockIndex.GetBlockHash()
	key := getBlockPath(s.prefix+"chunks", blockHash)
	objHandle := bucket.Object(key)
	_, err := objHandle.Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		blob, err := longtaillib.WriteStoredBlockToBuffer(storedBlock)
		if err != nil {
			return longtaillib.ENOMEM
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
			return errno
		}

		atomic.AddUint64(&s.stats.BlocksPutCount, 1)
		atomic.AddUint64(&s.stats.BytesPutCount, (uint64)(len(blob)))
		atomic.AddUint64(&s.stats.ChunksPutCount, (uint64)(blockIndex.GetChunkCount()))
	}

	newBlocks := []longtaillib.Longtail_BlockIndex{blockIndex}
	addedContentIndex, err := longtaillib.CreateContentIndexFromBlocks(s.maxBlockSize, s.maxChunksPerBlock, newBlocks)
	if err != nil {
		return longtaillib.ENOMEM
	}
	contentIndexMessages <- contentIndexMessage{contentIndex: addedContentIndex}
	return 0
}

func getStoredBlock(
	ctx context.Context,
	s *gcsBlockStore,
	bucket *storage.BucketHandle,
	blockHash uint64) (longtaillib.Longtail_StoredBlock, int) {

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
		return longtaillib.Longtail_StoredBlock{}, longtaillib.EIO
	}

	storedBlock, err := longtaillib.ReadStoredBlockFromBuffer(storedBlockData)
	if err != nil {
		return longtaillib.Longtail_StoredBlock{}, longtaillib.ENOMEM
	}

	atomic.AddUint64(&s.stats.BlocksGetCount, 1)
	atomic.AddUint64(&s.stats.BytesGetCount, (uint64)(len(storedBlockData)))
	blockIndex := storedBlock.GetBlockIndex()
	atomic.AddUint64(&s.stats.ChunksGetCount, (uint64)(blockIndex.GetChunkCount()))
	return storedBlock, 0
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
		s.workerWaitGroup.Done()
		return errors.Wrap(err, u.String())
	}
	bucketName := u.Host
	bucket := client.Bucket(bucketName)

	run := true
	for run {
		select {
		case putMsg := <-putBlockMessages:
			errno := putStoredBlock(ctx, s, bucket, contentIndexMessages, putMsg.storedBlock)
			putMsg.asyncCompleteAPI.OnComplete(errno)
		case getMsg := <-getBlockMessages:
			storedBlock, errno := getStoredBlock(ctx, s, bucket, getMsg.blockHash)
			getMsg.asyncCompleteAPI.OnComplete(storedBlock, errno)
		case _ = <-stopMessages:
			run = false
		}
	}

	select {
	case putMsg := <-putBlockMessages:
		errno := putStoredBlock(ctx, s, bucket, contentIndexMessages, putMsg.storedBlock)
		putMsg.asyncCompleteAPI.OnComplete(errno)
	default:
	}

	s.workerWaitGroup.Done()
	return nil
}

func updateRemoteContentIndex(
	ctx context.Context,
	bucket *storage.BucketHandle,
	prefix string,
	addedContentIndex longtaillib.Longtail_ContentIndex) error {
	storeBlob, err := longtaillib.WriteContentIndexToBuffer(addedContentIndex)
	if err != nil {
		log.Printf("updateRemoteContentIndex: longtaillib.WriteContentIndexToBuffer(addedContentIndex) with %q", err)
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
				log.Printf("updateRemoteContentIndex: objHandle.If(writeCondition).NewReader(ctx) returned nil, retrying")
				continue
			}
			blob, err := ioutil.ReadAll(reader)
			reader.Close()
			if err != nil {
				log.Printf("updateRemoteContentIndex: ioutil.ReadAll(reader) failed with %q", err)
				return err
			}

			remoteContentIndex, err := longtaillib.ReadContentIndexFromBuffer(blob)
			if err != nil {
				log.Printf("updateRemoteContentIndex: longtaillib.ReadContentIndexFromBuffer(blob) failed with %q", err)
				return err
			}
			defer remoteContentIndex.Dispose()
			mergedContentIndex, err := longtaillib.MergeContentIndex(remoteContentIndex, addedContentIndex)
			if err != nil {
				log.Printf("updateRemoteContentIndex: longtaillib.MergeContentIndex(remoteContentIndex, addedContentIndex) failed with %q", err)
				return err
			}
			defer mergedContentIndex.Dispose()

			storeBlob, err = longtaillib.WriteContentIndexToBuffer(mergedContentIndex)
			if err != nil {
				log.Printf("updateRemoteContentIndex: longtaillib.WriteContentIndexToBuffer(mergedContentIndex) failed with %q", err)
				return err
			}
		}
		writer := objHandle.If(writeCondition).NewWriter(ctx)
		if writer == nil {
			log.Printf("updateRemoteContentIndex: objHandle.If(writeCondition).NewWriter(ctx) returned nil, retrying")
			continue
		}
		_, err = writer.Write(storeBlob)
		if err != nil {
			log.Printf("updateRemoteContentIndex: writer.Write(storeBlob) failed with %q", err)
			writer.CloseWithError(err)
			return err
		}
		writer.Close()
		_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: "application/octet-stream"})
		if err != nil {
			log.Printf("updateRemoteContentIndex: objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: \"application/octet-stream\"}) failed with %q", err)
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
		s.indexWorkerWaitGroup.Done()
		return errors.Wrap(err, u.String())
	}
	bucketName := u.Host
	bucket := client.Bucket(bucketName)

	var contentIndex longtaillib.Longtail_ContentIndex

	key := s.prefix + "store.lci"
	objHandle := bucket.Object(key)
	obj, err := objHandle.NewReader(ctx)
	if err == nil {
		defer obj.Close()
		storedContentIndexData, err := ioutil.ReadAll(obj)
		if err == nil {
			contentIndex, err = longtaillib.ReadContentIndexFromBuffer(storedContentIndexData)
		}
	}

	if err != nil {
		hashAPI := longtaillib.CreateBlake3HashAPI()
		defer hashAPI.Dispose()

		contentIndex, err = longtaillib.CreateContentIndexFromBlocks(
			s.maxBlockSize,
			s.maxChunksPerBlock,
			[]longtaillib.Longtail_BlockIndex{})
		if err != nil {
			s.indexWorkerWaitGroup.Done()
			return err
		}
	}

	// TODO: Might need safer update of these two fields?
	s.maxBlockSize = contentIndex.GetMaxBlockSize()
	s.maxChunksPerBlock = contentIndex.GetMaxChunksPerBlock()

	addedContentIndex, err := longtaillib.CreateContentIndexFromBlocks(
		s.maxBlockSize,
		s.maxChunksPerBlock,
		[]longtaillib.Longtail_BlockIndex{})
	if err != nil {
		s.indexWorkerWaitGroup.Done()
		return err
	}

	defer contentIndex.Dispose()
	defer addedContentIndex.Dispose()

	run := true
	for run {
		select {
		case contentIndexMsg := <-contentIndexMessages:
			newAddedContentIndex, err := longtaillib.AddContentIndex(addedContentIndex, contentIndexMsg.contentIndex)
			if err != nil {
				log.Printf("ERROR: MergeContentIndex returned: %q", err)
				continue
			}
			addedContentIndex.Dispose()
			addedContentIndex = newAddedContentIndex
			contentIndexMsg.contentIndex.Dispose()
		case getIndexMessage := <-getIndexMessages:
			contentIndexCopy, err := longtaillib.MergeContentIndex(contentIndex, addedContentIndex)
			if err != nil {
				log.Printf("ERROR: MergeContentIndex returned: %q", err)
				getIndexMessage.asyncCompleteAPI.OnComplete(contentIndexCopy, longtaillib.ENOMEM)
				continue
			}
			getIndexMessage.asyncCompleteAPI.OnComplete(contentIndexCopy, 0)
			atomic.AddUint64(&s.stats.IndexGetCount, 1)
		case _ = <-stopMessages:
			run = false
		}
	}

	select {
	case contentIndexMsg := <-contentIndexMessages:
		newAddedContentIndex, err := longtaillib.AddContentIndex(addedContentIndex, contentIndexMsg.contentIndex)
		if err != nil {
			log.Printf("ERROR: MergeContentIndex returned: %q", err)
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
	s.indexWorkerWaitGroup.Done()
	return nil
}

// NewGCSBlockStore ...
func NewGCSBlockStore(u *url.URL, maxBlockSize uint32, maxChunksPerBlock uint32, outFinalStats *longtaillib.BlockStoreStats) (longtaillib.BlockStoreAPI, error) {
	if u.Scheme != "gs" {
		return nil, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	ctx := context.Background()
	defaultClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, u.String())
	}

	prefix := u.Path
	if len(u.Path) > 0 {
		prefix = u.Path[1:] // strip initial slash
	}

	if prefix != "" {
		prefix += "/"
	}
	bucketName := u.Host
	defaultBucket := defaultClient.Bucket(bucketName)

	s := &gcsBlockStore{url: u, Location: u.String(), prefix: prefix, maxBlockSize: maxBlockSize, maxChunksPerBlock: maxChunksPerBlock, defaultClient: defaultClient, defaultBucket: defaultBucket, outFinalStats: outFinalStats}
	s.workerCount = runtime.NumCPU()
	s.putBlockChan = make(chan putBlockMessage, s.workerCount*2048)
	s.getBlockChan = make(chan getBlockMessage, s.workerCount*2048)
	s.contentIndexChan = make(chan contentIndexMessage, s.workerCount*2048)
	s.getIndexChan = make(chan getIndexMessage)
	s.workerStopChan = make(chan stopMessage, s.workerCount)
	s.indexStopChan = make(chan stopMessage, 1)

	s.indexWorkerWaitGroup.Add(1)
	go contentIndexWorker(ctx, s, u, s.contentIndexChan, s.getIndexChan, s.indexStopChan)

	s.workerWaitGroup.Add(s.workerCount)
	for i := 0; i < s.workerCount; i++ {
		go gcsWorker(ctx, s, u, s.putBlockChan, s.getBlockChan, s.contentIndexChan, s.workerStopChan)
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
func (s *gcsBlockStore) PutStoredBlock(storedBlock longtaillib.Longtail_StoredBlock, asyncCompleteAPI longtaillib.Longtail_AsyncPutStoredBlockAPI) int {
	s.putBlockChan <- putBlockMessage{storedBlock: storedBlock, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// PreflightGet ...
func (s *gcsBlockStore) PreflightGet(blockCount uint64, hashes []uint64, refCounts []uint32) int {
	return 0
}

// GetStoredBlock ...
func (s *gcsBlockStore) GetStoredBlock(blockHash uint64, asyncCompleteAPI longtaillib.Longtail_AsyncGetStoredBlockAPI) int {
	s.getBlockChan <- getBlockMessage{blockHash: blockHash, asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// GetIndex ...
func (s *gcsBlockStore) GetIndex(asyncCompleteAPI longtaillib.Longtail_AsyncGetIndexAPI) int {
	s.getIndexChan <- getIndexMessage{asyncCompleteAPI: asyncCompleteAPI}
	return 0
}

// GetStats ...
func (s *gcsBlockStore) GetStats() (longtaillib.BlockStoreStats, int) {
	return s.stats, 0
}

// Close ...
func (s *gcsBlockStore) Close() {
	for i := 0; i < s.workerCount; i++ {
		s.workerStopChan <- stopMessage{}
	}
	s.workerWaitGroup.Wait()
	s.indexStopChan <- stopMessage{}
	s.indexWorkerWaitGroup.Wait()
	if s.outFinalStats != nil {
		*s.outFinalStats = s.stats
	}
}
