package longtailutils

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

func MakeError(errno int, description string) error {
	return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), description)
}

type getExistingContentCompletionAPI struct {
	wg         sync.WaitGroup
	storeIndex longtaillib.Longtail_StoreIndex
	err        int
}

func (a *getExistingContentCompletionAPI) OnComplete(storeIndex longtaillib.Longtail_StoreIndex, err int) {
	a.err = err
	a.storeIndex = storeIndex
	a.wg.Done()
}

type pruneBlocksCompletionAPI struct {
	wg               sync.WaitGroup
	prunedBlockCount uint32
	err              int
}

func (a *pruneBlocksCompletionAPI) OnComplete(prunedBlockCount uint32, err int) {
	a.err = err
	a.prunedBlockCount = prunedBlockCount
	a.wg.Done()
}

type flushCompletionAPI struct {
	asyncFlushAPI longtaillib.Longtail_AsyncFlushAPI
	wg            sync.WaitGroup
	err           int
}

func (a *flushCompletionAPI) OnComplete(err int) {
	a.err = err
	a.wg.Done()
}

// GetStoredBlockCompletionAPI ...
type GetStoredBlockCompletionAPI struct {
	Wg          sync.WaitGroup
	StoredBlock longtaillib.Longtail_StoredBlock
	Err         int
}

func (a *GetStoredBlockCompletionAPI) OnComplete(storedBlock longtaillib.Longtail_StoredBlock, err int) {
	a.Err = err
	a.StoredBlock = storedBlock
	a.Wg.Done()
}

// GetExistingStoreIndexSync ...
func GetExistingStoreIndexSync(
	indexStore longtaillib.Longtail_BlockStoreAPI,
	chunkHashes []uint64,
	minBlockUsagePercent uint32) (longtaillib.Longtail_StoreIndex, error) {
	log := logrus.WithFields(logrus.Fields{
		"len(chunkHashes)":     len(chunkHashes),
		"minBlockUsagePercent": minBlockUsagePercent,
	})
	getExistingContentComplete := &getExistingContentCompletionAPI{}
	getExistingContentComplete.wg.Add(1)
	errno := indexStore.GetExistingContent(chunkHashes, minBlockUsagePercent, longtaillib.CreateAsyncGetExistingContentAPI(getExistingContentComplete))
	if errno != 0 {
		getExistingContentComplete.wg.Done()
		getExistingContentComplete.wg.Wait()
		err := MakeError(errno, "indexStore.GetExistingContent failed")
		log.WithError(err).Error("GetExistingStoreIndexSync failed")
		return longtaillib.Longtail_StoreIndex{}, err
	}
	getExistingContentComplete.wg.Wait()
	if getExistingContentComplete.err != 0 {
		err := MakeError(getExistingContentComplete.err, "GetExistingContent completion failed")
		log.WithError(err).Error("GetExistingStoreIndexSync")
		return longtaillib.Longtail_StoreIndex{}, err
	}
	return getExistingContentComplete.storeIndex, nil
}

// PruneBlocksSync ...
func PruneBlocksSync(
	indexStore longtaillib.Longtail_BlockStoreAPI,
	keepBlockHashes []uint64) (uint32, error) {
	log := logrus.WithFields(logrus.Fields{
		"len(keepBlockHashes)": len(keepBlockHashes),
	})
	pruneBlocksComplete := &pruneBlocksCompletionAPI{}
	pruneBlocksComplete.wg.Add(1)
	errno := indexStore.PruneBlocks(keepBlockHashes, longtaillib.CreateAsyncPruneBlocksAPI(pruneBlocksComplete))
	if errno != 0 {
		pruneBlocksComplete.wg.Done()
		pruneBlocksComplete.wg.Wait()
		err := MakeError(errno, "indexStore.PruneBlocks failed")
		log.WithError(err).Error("PruneBlocksSync failed")
		return 0, err
	}
	pruneBlocksComplete.wg.Wait()
	if pruneBlocksComplete.err != 0 {
		err := MakeError(pruneBlocksComplete.err, "PruneBlocks failed")
		log.WithError(err).Error("PruneBlocksSync wait failed")
		return 0, err
	}
	return pruneBlocksComplete.prunedBlockCount, nil
}

// flushStore ...
func FlushStore(store *longtaillib.Longtail_BlockStoreAPI) (*flushCompletionAPI, error) {
	log := logrus.WithField("store", store)
	targetStoreFlushComplete := &flushCompletionAPI{}
	targetStoreFlushComplete.wg.Add(1)
	targetStoreFlushComplete.asyncFlushAPI = longtaillib.CreateAsyncFlushAPI(targetStoreFlushComplete)
	errno := store.Flush(targetStoreFlushComplete.asyncFlushAPI)
	if errno == 0 {
		return targetStoreFlushComplete, nil
	}
	targetStoreFlushComplete.wg.Done()
	err := MakeError(errno, "store.Flush failed")
	log.WithError(err).Error("FlushStore failed")
	return nil, err
}

func (f *flushCompletionAPI) Wait() error {
	f.wg.Wait()
	if f.err != 0 {
		err := MakeError(f.err, "Flush completion failed")
		log.WithError(err).Error("flushCompletionAPI.Wait failed")
		return err
	}
	return nil
}

func FlushStoreSync(store *longtaillib.Longtail_BlockStoreAPI) error {
	f, err := FlushStore(store)
	if err != nil {
		return err
	}
	return f.Wait()
}

type StoreFlush struct {
	flushAPIs []*flushCompletionAPI
}

// FlushStores ...
func FlushStores(stores []longtaillib.Longtail_BlockStoreAPI) (*StoreFlush, error) {
	storeFlush := &StoreFlush{}
	storeFlush.flushAPIs = make([]*flushCompletionAPI, len(stores))
	for i, store := range stores {
		if !store.IsValid() {
			continue
		}
		var err error
		storeFlush.flushAPIs[i], err = FlushStore(&store)
		if err != nil {
			for i > 0 {
				i--
				flushAPI := storeFlush.flushAPIs[i]
				if flushAPI != nil {
					flushAPI.Wait()
				}
			}
			return nil, err
		}
	}
	return storeFlush, nil
}

// Wait
func (s *StoreFlush) Wait() error {
	var err error
	for _, f := range s.flushAPIs {
		if f == nil {
			continue
		}
		f.Wait()
		if f.err != 0 {
			err = MakeError(f.err, "StoreFlush.Wait() failed")
			log.WithError(err).Error("Wait failed")
		}
		f.asyncFlushAPI.Dispose()
	}

	return err
}

func FlushStoresSync(stores []longtaillib.Longtail_BlockStoreAPI) error {
	f, err := FlushStores(stores)
	if err != nil {
		return err
	}
	return f.Wait()
}

func createBlobStoreForURI(uri string) (longtailstorelib.BlobStore, error) {
	blobStoreURL, err := url.Parse(uri)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			return longtailstorelib.NewGCSBlobStore(blobStoreURL, false)
		case "s3":
			return longtailstorelib.NewS3BlobStore(blobStoreURL)
		case "abfs":
			return nil, fmt.Errorf("azure Gen1 storage not yet implemented")
		case "abfss":
			return nil, fmt.Errorf("azure Gen2 storage not yet implemented")
		case "file":
			return longtailstorelib.NewFSBlobStore(blobStoreURL.Path[1:])
		}
	}

	return longtailstorelib.NewFSBlobStore(uri)
}

func splitURI(uri string) (string, string) {
	i := strings.LastIndex(uri, "/")
	if i == -1 {
		i = strings.LastIndex(uri, "\\")
	}
	if i == -1 {
		return "", uri
	}
	return uri[:i], uri[i+1:]
}

// ReadFromURI ...
func ReadFromURI(uri string) ([]byte, error) {
	log := logrus.WithField("uri", uri)
	uriParent, uriName := splitURI(uri)
	blobStore, err := longtailstorelib.CreateBlobStoreForURI(uriParent)
	if err != nil {
		log.WithError(err).Error("createBlobStoreForURI failed")
		return nil, errors.Wrap(err, "ReadFromURI failed")
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		log.WithError(err).Error("blobStore.NewClient failed")
		return nil, errors.Wrap(err, "ReadFromURI failed")
	}
	defer client.Close()
	object, err := client.NewObject(uriName)
	if err != nil {
		log.WithError(err).Error("client.NewObject failed")
		return nil, errors.Wrap(err, "ReadFromURI failed")
	}
	vbuffer, err := object.Read()
	if err != nil {
		log.WithError(err).Error("object.Read failed")
		return nil, errors.Wrap(err, "ReadFromURI failed")
	}
	return vbuffer, nil
}

// ReadFromURI ...
func WriteToURI(uri string, data []byte) error {
	uriParent, uriName := splitURI(uri)
	blobStore, err := createBlobStoreForURI(uriParent)
	if err != nil {
		return err
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		return err
	}
	defer client.Close()
	object, err := client.NewObject(uriName)
	if err != nil {
		return err
	}
	_, err = object.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func readBlobWithRetry(
	ctx context.Context,
	client longtailstorelib.BlobClient,
	key string) ([]byte, int, error) {
	retryCount := 0
	objHandle, err := client.NewObject(key)
	if err != nil {
		return nil, retryCount, err
	}
	exists, err := objHandle.Exists()
	if err != nil {
		return nil, retryCount, err
	}
	if !exists {
		return nil, retryCount, longtaillib.ErrENOENT
	}
	blobData, err := objHandle.Read()
	if err != nil {
		log.Printf("Retrying getBlob %s in store %s\n", key, client.String())
		retryCount++
		blobData, err = objHandle.Read()
	}
	if err != nil {
		log.Printf("Retrying 500 ms delayed getBlob %s in store %s\n", key, client.String())
		time.Sleep(500 * time.Millisecond)
		retryCount++
		blobData, err = objHandle.Read()
	}
	if err != nil {
		log.Printf("Retrying 2 s delayed getBlob %s in store %s\n", key, client.String())
		time.Sleep(2 * time.Second)
		retryCount++
		blobData, err = objHandle.Read()
	}

	if err != nil {
		return nil, retryCount, err
	}

	return blobData, retryCount, nil
}

func GetCompressionTypesForFiles(fileInfos longtaillib.Longtail_FileInfos, compressionType uint32) []uint32 {
	pathCount := fileInfos.GetFileCount()
	compressionTypes := make([]uint32, pathCount)
	for i := uint32(0); i < pathCount; i++ {
		compressionTypes[i] = compressionType
	}
	return compressionTypes
}

var (
	compressionTypeMap = map[string]uint32{
		"none":            NoCompressionType,
		"brotli":          longtaillib.GetBrotliGenericDefaultCompressionType(),
		"brotli_min":      longtaillib.GetBrotliGenericMinCompressionType(),
		"brotli_max":      longtaillib.GetBrotliGenericMaxCompressionType(),
		"brotli_text":     longtaillib.GetBrotliTextDefaultCompressionType(),
		"brotli_text_min": longtaillib.GetBrotliTextMinCompressionType(),
		"brotli_text_max": longtaillib.GetBrotliTextMaxCompressionType(),
		"lz4":             longtaillib.GetLZ4DefaultCompressionType(),
		"zstd":            longtaillib.GetZStdDefaultCompressionType(),
		"zstd_min":        longtaillib.GetZStdMinCompressionType(),
		"zstd_max":        longtaillib.GetZStdMaxCompressionType(),
	}

	hashIdentifierMap = map[string]uint32{
		"meow":   longtaillib.GetMeowHashIdentifier(),
		"blake2": longtaillib.GetBlake2HashIdentifier(),
		"blake3": longtaillib.GetBlake3HashIdentifier(),
	}

	reverseHashIdentifierMap = map[uint32]string{
		longtaillib.GetMeowHashIdentifier():   "meow",
		longtaillib.GetBlake2HashIdentifier(): "blake2",
		longtaillib.GetBlake3HashIdentifier(): "blake3",
	}
)

const NoCompressionType = uint32(0)

func GetCompressionType(compressionAlgorithm string) (uint32, error) {
	if compressionType, exists := compressionTypeMap[compressionAlgorithm]; exists {
		return compressionType, nil
	}
	return 0, fmt.Errorf("unsupported compression algorithm: `%s`", compressionAlgorithm)
}

func GetHashIdentifier(hashAlgorithm string) (uint32, error) {
	if identifier, exists := hashIdentifierMap[hashAlgorithm]; exists {
		return identifier, nil
	}
	return 0, fmt.Errorf("not a supported hash api: `%s`", hashAlgorithm)
}

func HashIdentifierToString(hashIdentifier uint32) string {
	if identifier, exists := reverseHashIdentifierMap[hashIdentifier]; exists {
		return identifier
	}
	return fmt.Sprintf("%d", hashIdentifier)
}
