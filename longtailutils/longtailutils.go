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
	const fname = "getExistingContentCompletionAPI.OnComplete"
	log.Debug(fname)
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
	const fname = "pruneBlocksCompletionAPI.OnComplete"
	log.Debug(fname)
	a.prunedBlockCount = prunedBlockCount
	a.wg.Done()
}

type flushCompletionAPI struct {
	asyncFlushAPI longtaillib.Longtail_AsyncFlushAPI
	wg            sync.WaitGroup
	err           int
}

func (a *flushCompletionAPI) OnComplete(err int) {
	const fname = "flushCompletionAPI.OnComplete"
	log.Debug(fname)
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
	const fname = "GetStoredBlockCompletionAPI.OnComplete"
	log.Debug(fname)
	a.Err = err
	a.StoredBlock = storedBlock
	a.Wg.Done()
}

// GetExistingStoreIndexSync ...
func GetExistingStoreIndexSync(
	indexStore longtaillib.Longtail_BlockStoreAPI,
	chunkHashes []uint64,
	minBlockUsagePercent uint32) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "GetExistingStoreIndexSync"
	log := logrus.WithFields(logrus.Fields{
		"fname":                fname,
		"len(chunkHashes)":     len(chunkHashes),
		"minBlockUsagePercent": minBlockUsagePercent,
	})
	log.Debug(fname)

	getExistingContentComplete := &getExistingContentCompletionAPI{}
	getExistingContentComplete.wg.Add(1)
	errno := indexStore.GetExistingContent(chunkHashes, minBlockUsagePercent, longtaillib.CreateAsyncGetExistingContentAPI(getExistingContentComplete))
	if errno != 0 {
		getExistingContentComplete.wg.Done()
		getExistingContentComplete.wg.Wait()
		err := MakeError(errno, "Failed getting existing content index")
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	getExistingContentComplete.wg.Wait()
	if getExistingContentComplete.err != 0 {
		err := MakeError(getExistingContentComplete.err, "GetExistingStoreIndexSync completion failed")
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	return getExistingContentComplete.storeIndex, nil
}

// PruneBlocksSync ...
func PruneBlocksSync(
	indexStore longtaillib.Longtail_BlockStoreAPI,
	keepBlockHashes []uint64) (uint32, error) {
	const fname = "GetExistingStoreIndexSync"
	log := logrus.WithFields(logrus.Fields{
		"fname":                fname,
		"len(keepBlockHashes)": len(keepBlockHashes),
	})
	log.Debug(fname)

	pruneBlocksComplete := &pruneBlocksCompletionAPI{}
	pruneBlocksComplete.wg.Add(1)
	errno := indexStore.PruneBlocks(keepBlockHashes, longtaillib.CreateAsyncPruneBlocksAPI(pruneBlocksComplete))
	if errno != 0 {
		pruneBlocksComplete.wg.Done()
		pruneBlocksComplete.wg.Wait()
		err := MakeError(errno, "Failed pruning blocks in store")
		return 0, errors.Wrap(err, fname)
	}
	pruneBlocksComplete.wg.Wait()
	if pruneBlocksComplete.err != 0 {
		err := MakeError(pruneBlocksComplete.err, "PruneBlocks completion failed")
		return 0, errors.Wrap(err, fname)
	}
	return pruneBlocksComplete.prunedBlockCount, nil
}

// flushStore ...
func FlushStore(store *longtaillib.Longtail_BlockStoreAPI) (*flushCompletionAPI, error) {
	const fname = "FlushStore"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"store": store,
	})
	log.Debug(fname)

	targetStoreFlushComplete := &flushCompletionAPI{}
	targetStoreFlushComplete.wg.Add(1)
	targetStoreFlushComplete.asyncFlushAPI = longtaillib.CreateAsyncFlushAPI(targetStoreFlushComplete)
	errno := store.Flush(targetStoreFlushComplete.asyncFlushAPI)
	if errno == 0 {
		return targetStoreFlushComplete, nil
	}
	targetStoreFlushComplete.wg.Done()
	err := MakeError(errno, "Failed creating flush callback api")
	return nil, errors.Wrap(err, fname)
}

func (f *flushCompletionAPI) Wait() error {
	const fname = "flushCompletionAPI.Wait"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)
	f.wg.Wait()
	if f.err != 0 {
		err := MakeError(f.err, "Flush completion failed")
		return errors.Wrap(err, fname)
	}
	return nil
}

func FlushStoreSync(store *longtaillib.Longtail_BlockStoreAPI) error {
	const fname = "FlushStoreSync"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"store": store,
	})
	log.Debug(fname)
	f, err := FlushStore(store)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	err = f.Wait()
	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

type StoreFlush struct {
	flushAPIs []*flushCompletionAPI
}

// FlushStores ...
func FlushStores(stores []longtaillib.Longtail_BlockStoreAPI) (*StoreFlush, error) {
	const fname = "FlushStores"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"stores": stores,
	})
	log.Debug(fname)
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
			return nil, errors.Wrap(err, fname)
		}
	}
	return storeFlush, nil
}

// Wait
func (s *StoreFlush) Wait() error {
	const fname = "StoreFlush.Wait"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)
	var err error
	for _, f := range s.flushAPIs {
		if f == nil {
			continue
		}
		f.Wait()
		if f.err != 0 {
			err = MakeError(f.err, "StoreFlush.Wait() failed")
			err = errors.Wrap(err, fname)
			log.WithError(err).Error("Flush failed")
		}
		f.asyncFlushAPI.Dispose()
	}

	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

func FlushStoresSync(stores []longtaillib.Longtail_BlockStoreAPI) error {
	const fname = "FlushStoresSync"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"stores": stores,
	})
	log.Debug(fname)
	f, err := FlushStores(stores)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	err = f.Wait()
	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

func createBlobStoreForURI(uri string) (longtailstorelib.BlobStore, error) {
	const fname = "createBlobStoreForURI"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"uri":   uri,
	})
	log.Debug(fname)
	blobStoreURL, err := url.Parse(uri)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			store, err := longtailstorelib.NewGCSBlobStore(blobStoreURL, false)
			if err != nil {
				return nil, errors.Wrap(err, fname)
			}
			return store, nil
		case "s3":
			store, err := longtailstorelib.NewS3BlobStore(blobStoreURL)
			if err != nil {
				return nil, errors.Wrap(err, fname)
			}
			return store, nil
		case "abfs":
			err := fmt.Errorf("azure Gen1 storage not yet implemented for `%s`", uri)
			return nil, errors.Wrap(err, fname)
		case "abfss":
			err := fmt.Errorf("azure Gen2 storage not yet implemented for `%s`", uri)
			return nil, errors.Wrap(err, fname)
		case "file":
			store, err := longtailstorelib.NewFSBlobStore(blobStoreURL.Path[1:])
			if err != nil {
				return nil, errors.Wrap(err, fname)
			}
			return store, nil
		}
	}

	store, err := longtailstorelib.NewFSBlobStore(uri)
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	return store, nil
}

func splitURI(uri string) (string, string) {
	const fname = "splitURI"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"uri":   uri,
	})
	log.Debug(fname)
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
	const fname = "ReadFromURI"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"uri":   uri,
	})
	log.Debug(fname)
	uriParent, uriName := splitURI(uri)
	blobStore, err := longtailstorelib.CreateBlobStoreForURI(uriParent)
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	defer client.Close()
	object, err := client.NewObject(uriName)
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	vbuffer, err := object.Read()
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	return vbuffer, nil
}

// ReadFromURI ...
func WriteToURI(uri string, data []byte) error {
	const fname = "ReadFromURI"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"uri":   uri,
	})
	log.Debug(fname)
	uriParent, uriName := splitURI(uri)
	blobStore, err := createBlobStoreForURI(uriParent)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		return errors.Wrap(err, fname)
	}
	defer client.Close()
	object, err := client.NewObject(uriName)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	_, err = object.Write(data)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

func ReadBlobWithRetry(
	ctx context.Context,
	client longtailstorelib.BlobClient,
	key string) ([]byte, int, error) {
	const fname = "ReadFromURI"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"client": client,
		"key":    key,
	})
	log.Debug(fname)

	retryCount := 0
	objHandle, err := client.NewObject(key)
	if err != nil {
		return nil, retryCount, errors.Wrap(err, fname)
	}
	exists, err := objHandle.Exists()
	if err != nil {
		return nil, retryCount, errors.Wrap(err, fname)
	}
	if !exists {
		return nil, retryCount, longtaillib.ErrENOENT
	}
	blobData, err := objHandle.Read()
	if err != nil {
		log.Infof("Retrying getBlob %s in store %s\n", key, client.String())
		retryCount++
		blobData, err = objHandle.Read()
	}
	if err != nil {
		log.Infof("Retrying 500 ms delayed getBlob %s in store %s\n", key, client.String())
		time.Sleep(500 * time.Millisecond)
		retryCount++
		blobData, err = objHandle.Read()
	}
	if err != nil {
		log.Infof("Retrying 2 s delayed getBlob %s in store %s\n", key, client.String())
		time.Sleep(2 * time.Second)
		retryCount++
		blobData, err = objHandle.Read()
	}

	if err != nil {
		return nil, retryCount, errors.Wrap(err, fname)
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
	const fname = "GetCompressionType"
	log := logrus.WithFields(logrus.Fields{
		"fname":                fname,
		"compressionAlgorithm": compressionAlgorithm,
	})
	log.Debug(fname)
	if compressionType, exists := compressionTypeMap[compressionAlgorithm]; exists {
		return compressionType, nil
	}
	err := fmt.Errorf("Unsupported compression algorithm: `%s`", compressionAlgorithm)
	return 0, errors.Wrap(err, fname)
}

func GetHashIdentifier(hashAlgorithm string) (uint32, error) {
	const fname = "GetHashIdentifier"
	log := logrus.WithFields(logrus.Fields{
		"fname":         fname,
		"hashAlgorithm": hashAlgorithm,
	})
	log.Debug(fname)
	if identifier, exists := hashIdentifierMap[hashAlgorithm]; exists {
		return identifier, nil
	}
	err := fmt.Errorf("not a supported hash api: `%s`", hashAlgorithm)
	return 0, errors.Wrap(err, fname)
}

func HashIdentifierToString(hashIdentifier uint32) string {
	const fname = "GetCompressionType"
	log := logrus.WithFields(logrus.Fields{
		"fname":          fname,
		"hashIdentifier": hashIdentifier,
	})
	log.Debug(fname)
	if identifier, exists := reverseHashIdentifierMap[hashIdentifier]; exists {
		return identifier
	}
	log.Warnf("Unknown hash type")
	return fmt.Sprintf("%d", hashIdentifier)
}
