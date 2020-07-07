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
	"google.golang.org/api/iterator"
)

// RemoteObject
type RemoteObject interface {
	LockState() error
	Read() ([]byte, error)
	Write(data []byte) error
}

// RemoteClient
type RemoteClient interface {
	NewObject(path string) (RemoteObject, error)
}

// RemoteStore
type RemoteStore interface {
	OpenClient(path string, ctx context.Context) (RemoteClient, error)
}

type gcsRemoteObject struct {
	objHandle      *storage.ObjectHandle
	ctx            context.Context
	path           string
	writeCondition *storage.Conditions
}

type gcsRemoteClient struct {
	client     *storage.Client
	ctx        context.Context
	bucketName string
	bucket     *storage.BucketHandle
}

type gcsRemoteStore struct {
}

// NewGCSFileStorage ...
func NewGCSRemoteStore() (RemoteStore, error) {
	s := &gcsRemoteStore{}
	return s, nil
}

func (remoteStore *gcsRemoteStore) OpenClient(bucketPath string, ctx context.Context) (RemoteClient, error) {
	u, err := url.Parse(bucketPath)
	if u.Scheme != "gs" {
		return nil, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, bucketPath)
	}

	bucketName := u.Host
	bucket := client.Bucket(bucketName)
	return &gcsRemoteClient{client: client, ctx: ctx, bucketName: bucketName, bucket: bucket}, nil
}

func (remoteClient *gcsRemoteClient) NewObject(path string) (RemoteObject, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "gs" {
		return nil, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}
	if u.Host != remoteClient.bucketName {
		return nil, fmt.Errorf("invalid bucket '%s', expected '%s'", u.Hostname, remoteClient.bucket)
	}

	objHandle := remoteClient.bucket.Object(u.Path[1:])
	return &gcsRemoteObject{objHandle: objHandle, ctx: remoteClient.ctx, path: u.Path[1:], writeCondition: nil}, nil
}

func (remoteObject *gcsRemoteObject) Read() ([]byte, error) {
	var reader *storage.Reader
	var err error
	if remoteObject.writeCondition == nil {
		reader, err = remoteObject.objHandle.NewReader(remoteObject.ctx)
	} else {
		reader, err = remoteObject.objHandle.If(*remoteObject.writeCondition).NewReader(remoteObject.ctx)
	}
	if err != nil {
		return nil, errors.Wrap(err, remoteObject.path)
	}
	defer reader.Close()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrap(err, remoteObject.path)
	}
	return data, nil
}

func (remoteObject *gcsRemoteObject) LockState() error {
	remoteObject.writeCondition = &storage.Conditions{DoesNotExist: true}
	objAttrs, _ := remoteObject.objHandle.Attrs(remoteObject.ctx)
	if objAttrs != nil {
		remoteObject.writeCondition = &storage.Conditions{GenerationMatch: objAttrs.Generation}
	}
	return nil
}

func (remoteObject *gcsRemoteObject) Write(data []byte) error {
	var writer *storage.Writer
	if remoteObject.writeCondition == nil {
		writer = remoteObject.objHandle.NewWriter(remoteObject.ctx)
	} else {
		writer = remoteObject.objHandle.If(*remoteObject.writeCondition).NewWriter(remoteObject.ctx)
	}
	if writer == nil {
		return fmt.Errorf("Failed to create writer for `%s`", remoteObject.path)
	}
	defer writer.Close()
	_, err := writer.Write(data)
	if err != nil {
		return errors.Wrap(err, remoteObject.path)
	}
	return nil
}

type gcsFileStorage struct {
}

func (fileStorage *gcsFileStorage) ReadFromPath(ctx context.Context, path string) ([]byte, error) {
	u, err := url.Parse(path)
	if u.Scheme != "gs" {
		return nil, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, path)
	}

	bucketName := u.Host
	bucket := client.Bucket(bucketName)

	objHandle := bucket.Object(u.Path[1:])
	objReader, err := objHandle.NewReader(ctx)
	if err != nil {
		return nil, errors.Wrap(err, path)
	}
	defer objReader.Close()

	b, err := ioutil.ReadAll(objReader)
	if err != nil {
		return nil, errors.Wrap(err, path)
	}
	return b, nil
}

func (fileStorage *gcsFileStorage) WriteToPath(ctx context.Context, path string, data []byte) error {
	u, err := url.Parse(path)
	if u.Scheme != "gs" {
		return fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return errors.Wrap(err, path)
	}

	bucketName := u.Host
	bucket := client.Bucket(bucketName)

	objHandle := bucket.Object(u.Path[1:])
	{
		objWriter := objHandle.NewWriter(ctx)
		_, err := objWriter.Write(data)
		objWriter.Close()
		if err != nil {
			return errors.Wrap(err, path)
		}
	}
	_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: "application/octet-stream"})
	if err != nil {
		return errors.Wrap(err, path)
	}
	return nil
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

type retargetContentMessage struct {
	contentIndex     longtaillib.Longtail_ContentIndex
	asyncCompleteAPI longtaillib.Longtail_AsyncRetargetContentAPI
}

type stopMessage struct {
}

type gcsBlockStore struct {
	jobAPI            longtaillib.Longtail_JobAPI
	url               *url.URL
	Location          string
	prefix            string
	maxBlockSize      uint32
	maxChunksPerBlock uint32
	defaultClient     *storage.Client
	defaultBucket     *storage.BucketHandle

	workerCount int

	putBlockChan        chan putBlockMessage
	getBlockChan        chan getBlockMessage
	contentIndexChan    chan contentIndexMessage
	getIndexChan        chan getIndexMessage
	retargetContentChan chan fsRetargetContentMessage
	workerStopChan      chan stopMessage
	indexStopChan       chan stopMessage
	workerErrorChan     chan error

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
		return longtaillib.EIO
	}

	err = objWriter.Close()
	if err != nil {
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
		blob, errno := longtaillib.WriteStoredBlockToBuffer(storedBlock)
		if errno != 0 {
			return errno
		}

		errno = putBlob(ctx, objHandle, blob)
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
	addedContentIndex, errno := longtaillib.CreateContentIndexFromBlocks(s.maxBlockSize, s.maxChunksPerBlock, newBlocks)
	if errno != 0 {
		return errno
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

	storedBlock, errno := longtaillib.ReadStoredBlockFromBuffer(storedBlockData)
	if errno != 0 {
		return longtaillib.Longtail_StoredBlock{}, errno
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

	return nil
}

func updateRemoteContentIndex(
	ctx context.Context,
	bucket *storage.BucketHandle,
	prefix string,
	jobAPI longtaillib.Longtail_JobAPI,
	addedContentIndex longtaillib.Longtail_ContentIndex) error {
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

			remoteContentIndex, errno := longtaillib.ReadContentIndexFromBuffer(blob)
			if errno != 0 {
				return fmt.Errorf("updateRemoteContentIndex: longtaillib.ReadContentIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
			}
			defer remoteContentIndex.Dispose()

			mergedContentIndex, errno := longtaillib.MergeContentIndex(jobAPI, remoteContentIndex, addedContentIndex)
			if errno != 0 {
				return fmt.Errorf("updateRemoteContentIndex: longtaillib.MergeContentIndex() failed with error %s", longtaillib.ErrNoToDescription(errno))
			}
			defer mergedContentIndex.Dispose()

			storeBlob, errno := longtaillib.WriteContentIndexToBuffer(mergedContentIndex)
			if errno != 0 {
				return fmt.Errorf("updateRemoteContentIndex: longtaillib.WriteContentIndexToBuffer() failed with error %s", longtaillib.ErrNoToDescription(errno))
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
			return nil
		}

		storeBlob, errno := longtaillib.WriteContentIndexToBuffer(addedContentIndex)
		if errno != 0 {
			return fmt.Errorf("updateRemoteContentIndex: longtaillib.WriteContentIndexToBuffer() failed with error %s", longtaillib.ErrNoToDescription(errno))
		}

		writer := objHandle.If(writeCondition).NewWriter(ctx)
		if writer == nil {
			log.Printf("updateRemoteContentIndex: objHandle.If(writeCondition).NewWriter(ctx) returned nil, retrying")
			continue
		}

		_, err := writer.Write(storeBlob)
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
		return nil
	}
	return nil
}

func buildContentIndexFromBlocks(
	ctx context.Context,
	s *gcsBlockStore,
	u *url.URL,
	client *storage.Client,
	bucketName string,
	bucket *storage.BucketHandle) (longtaillib.Longtail_ContentIndex, int) {
	var items []string
	it := bucket.Objects(ctx, &storage.Query{
		Prefix: s.prefix,
	})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			break
		}
		if attrs.Size == 0 {
			continue
		}
		if strings.HasSuffix(attrs.Name, ".lsb") {
			items = append(items, attrs.Name)
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
				client, _ := storage.NewClient(ctx)
				bucketName := u.Host
				bucket := client.Bucket(bucketName)
				objHandle := bucket.Object(blockKey)
				storedBlockData, errno := getBlob(ctx, objHandle)
				if errno != 0 {
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
	fsRetargetContentMessages <-chan fsRetargetContentMessage,
	stopMessages <-chan stopMessage) {
	run := true
	for run {
		select {
		case _ = <-contentIndexMessages:
		case getIndexMessage := <-getIndexMessages:
			getIndexMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, longtaillib.EINVAL)
		case retargetContentMessage := <-fsRetargetContentMessages:
			retargetContentMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, longtaillib.EINVAL)
		case _ = <-stopMessages:
			run = false
		}
	}
}

func contentIndexWorker(
	ctx context.Context,
	s *gcsBlockStore,
	u *url.URL,
	contentIndexMessages <-chan contentIndexMessage,
	getIndexMessages <-chan getIndexMessage,
	fsRetargetContentMessages <-chan fsRetargetContentMessage,
	stopMessages <-chan stopMessage) error {

	client, err := storage.NewClient(ctx)
	if err != nil {
		contentIndexWorkerReplyErrorState(contentIndexMessages, getIndexMessages, fsRetargetContentMessages, stopMessages)
		return errors.Wrap(err, u.String())
	}
	bucketName := u.Host
	bucket := client.Bucket(bucketName)

	var errno int
	var contentIndex longtaillib.Longtail_ContentIndex

	key := s.prefix + "store.lci"

	objHandle := bucket.Object(key)

	if !contentIndex.IsValid() {
		_, err = objHandle.Attrs(ctx)
		if err != storage.ErrObjectNotExist {
			storedContentIndexData, errno := getBlob(ctx, objHandle)
			if errno != 0 {
				log.Printf("Retrying getBlob %s", key)
				atomic.AddUint64(&s.stats.IndexGetRetryCount, 1)
				storedContentIndexData, errno = getBlob(ctx, objHandle)
			}
			if errno != 0 {
				log.Printf("Retrying 500 ms delayed getBlob %s", key)
				time.Sleep(500 * time.Millisecond)
				atomic.AddUint64(&s.stats.IndexGetRetryCount, 1)
				storedContentIndexData, errno = getBlob(ctx, objHandle)
			}
			if errno != 0 {
				log.Printf("Retrying 2 s delayed getBlob %s", key)
				time.Sleep(2 * time.Second)
				atomic.AddUint64(&s.stats.IndexGetRetryCount, 1)
				storedContentIndexData, errno = getBlob(ctx, objHandle)
			}
			if errno == 0 {
				contentIndex, errno = longtaillib.ReadContentIndexFromBuffer(storedContentIndexData)
				if errno != 0 {
					contentIndexWorkerReplyErrorState(contentIndexMessages, getIndexMessages, fsRetargetContentMessages, stopMessages)
					return fmt.Errorf("contentIndexWorker: longtaillib.ReadContentIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
				}
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
			contentIndexWorkerReplyErrorState(contentIndexMessages, getIndexMessages, fsRetargetContentMessages, stopMessages)
			return fmt.Errorf("contentIndexWorker: longtaillib.CreateContentIndexFromBlocks() failed with %s", longtaillib.ErrNoToDescription(errno))
		}

		addedContentIndex, errno = buildContentIndexFromBlocks(
			ctx,
			s,
			u,
			client,
			bucketName,
			bucket)

		if errno != 0 {
			addedContentIndex, errno = longtaillib.CreateContentIndexFromBlocks(
				s.maxBlockSize,
				s.maxChunksPerBlock,
				[]longtaillib.Longtail_BlockIndex{})
			if errno != 0 {
				contentIndexWorkerReplyErrorState(contentIndexMessages, getIndexMessages, fsRetargetContentMessages, stopMessages)
				return fmt.Errorf("contentIndexWorker: longtaillib.CreateContentIndexFromBlocks() failed with %s", longtaillib.ErrNoToDescription(errno))
			}
		}
	} else {
		addedContentIndex, errno = longtaillib.CreateContentIndexFromBlocks(
			s.maxBlockSize,
			s.maxChunksPerBlock,
			[]longtaillib.Longtail_BlockIndex{})
		if errno != 0 {
			contentIndexWorkerReplyErrorState(contentIndexMessages, getIndexMessages, fsRetargetContentMessages, stopMessages)
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
				contentIndexWorkerReplyErrorState(contentIndexMessages, getIndexMessages, fsRetargetContentMessages, stopMessages)
				return fmt.Errorf("contentIndexWorker: longtaillib.AddContentIndex() failed with %s", longtaillib.ErrNoToDescription(errno))
			}
			addedContentIndex.Dispose()
			addedContentIndex = newAddedContentIndex
			contentIndexMsg.contentIndex.Dispose()
		case getIndexMessage := <-getIndexMessages:
			contentIndexCopy, errno := longtaillib.MergeContentIndex(s.jobAPI, contentIndex, addedContentIndex)
			if errno != 0 {
				getIndexMessage.asyncCompleteAPI.OnComplete(longtaillib.Longtail_ContentIndex{}, errno)
				continue
			}
			getIndexMessage.asyncCompleteAPI.OnComplete(contentIndexCopy, 0)
			atomic.AddUint64(&s.stats.IndexGetCount, 1)
		case retargetContentMessage := <-fsRetargetContentMessages:
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

	if addedContentIndex.GetBlockCount() > 0 {
		err := updateRemoteContentIndex(ctx, bucket, s.prefix, s.jobAPI, addedContentIndex)
		if err != nil {
			log.Printf("Retrying store index to %s", s.prefix)
			atomic.AddUint64(&s.stats.IndexGetRetryCount, 1)
			err = updateRemoteContentIndex(ctx, bucket, s.prefix, s.jobAPI, addedContentIndex)
		}
		if err != nil {
			log.Printf("Retrying 500 ms delayed store index to %s", s.prefix)
			time.Sleep(500 * time.Millisecond)
			atomic.AddUint64(&s.stats.IndexGetRetryCount, 1)
			err = updateRemoteContentIndex(ctx, bucket, s.prefix, s.jobAPI, addedContentIndex)
		}
		if err != nil {
			log.Printf("Retrying 2 s delayed store index to %s", s.prefix)
			time.Sleep(2 * time.Second)
			atomic.AddUint64(&s.stats.IndexGetRetryCount, 1)
			err = updateRemoteContentIndex(ctx, bucket, s.prefix, s.jobAPI, addedContentIndex)
		}

		if err != nil {
			return fmt.Errorf("WARNING: Failed to write store content index failed with %q", err)
		}
	}
	return nil
}

// NewGCSBlockStore ...
func NewGCSBlockStore(jobAPI longtaillib.Longtail_JobAPI, u *url.URL, maxBlockSize uint32, maxChunksPerBlock uint32, outFinalStats *longtaillib.BlockStoreStats) (longtaillib.BlockStoreAPI, error) {
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

	s := &gcsBlockStore{jobAPI: jobAPI, url: u, Location: u.String(), prefix: prefix, maxBlockSize: maxBlockSize, maxChunksPerBlock: maxChunksPerBlock, defaultClient: defaultClient, defaultBucket: defaultBucket, outFinalStats: outFinalStats}
	s.workerCount = runtime.NumCPU()
	s.putBlockChan = make(chan putBlockMessage, s.workerCount*2048)
	s.getBlockChan = make(chan getBlockMessage, s.workerCount*2048)
	s.contentIndexChan = make(chan contentIndexMessage, s.workerCount*2048)
	s.getIndexChan = make(chan getIndexMessage)
	s.retargetContentChan = make(chan fsRetargetContentMessage, 16)
	s.workerStopChan = make(chan stopMessage, s.workerCount)
	s.indexStopChan = make(chan stopMessage, 1)
	s.workerErrorChan = make(chan error, 1+s.workerCount)

	go func() {
		err := contentIndexWorker(ctx, s, u, s.contentIndexChan, s.getIndexChan, s.retargetContentChan, s.indexStopChan)
		s.workerErrorChan <- err
	}()

	for i := 0; i < s.workerCount; i++ {
		go func() {
			err := gcsWorker(ctx, s, u, s.putBlockChan, s.getBlockChan, s.contentIndexChan, s.workerStopChan)
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

// RetargetContent ...
func (s *gcsBlockStore) RetargetContent(
	contentIndex longtaillib.Longtail_ContentIndex,
	asyncCompleteAPI longtaillib.Longtail_AsyncRetargetContentAPI) int {
	s.retargetContentChan <- fsRetargetContentMessage{contentIndex: contentIndex, asyncCompleteAPI: asyncCompleteAPI}
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
