package longtailutils

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"
)

type getExistingContentCompletionAPI struct {
	wg         sync.WaitGroup
	storeIndex longtaillib.Longtail_StoreIndex
	err        error
}

func WithS3EndpointResolverURI(endpointURI string) longtailstorelib.BlobStoreOption {
	return func(options interface{}) {
		s3options, ok := options.(*longtailstorelib.S3Options)
		if !ok {
			return
		}
		s3options.EndpointResolverURI = endpointURI
	}
}

func (a *getExistingContentCompletionAPI) OnComplete(storeIndex longtaillib.Longtail_StoreIndex, err error) {
	const fname = "getExistingContentCompletionAPI.OnComplete"
	log := logrus.WithContext(context.Background()).WithFields(logrus.Fields{
		"fname": fname,
		"err":   err,
	})
	log.Debug(fname)
	a.err = err
	a.storeIndex = storeIndex
	a.wg.Done()
}

type pruneBlocksCompletionAPI struct {
	wg               sync.WaitGroup
	prunedBlockCount uint32
	err              error
}

func (a *pruneBlocksCompletionAPI) OnComplete(prunedBlockCount uint32, err error) {
	const fname = "pruneBlocksCompletionAPI.OnComplete"
	log := logrus.WithContext(context.Background()).WithFields(logrus.Fields{
		"fname":            fname,
		"prunedBlockCount": prunedBlockCount,
		"err":              err,
	})
	log.Debug(fname)
	a.prunedBlockCount = prunedBlockCount
	a.wg.Done()
}

type flushCompletionAPI struct {
	asyncFlushAPI longtaillib.Longtail_AsyncFlushAPI
	wg            sync.WaitGroup
	err           error
}

func (a *flushCompletionAPI) OnComplete(err error) {
	const fname = "flushCompletionAPI.OnComplete"
	log := logrus.WithContext(context.Background()).WithFields(logrus.Fields{
		"fname": fname,
		"err":   err,
	})
	log.Debug(fname)
	a.err = err
	a.wg.Done()
}

// GetStoredBlockCompletionAPI ...
type GetStoredBlockCompletionAPI struct {
	Wg          sync.WaitGroup
	StoredBlock longtaillib.Longtail_StoredBlock
	Err         error
}

func (a *GetStoredBlockCompletionAPI) OnComplete(storedBlock longtaillib.Longtail_StoredBlock, err error) {
	const fname = "GetStoredBlockCompletionAPI.OnComplete"
	log := logrus.WithContext(context.Background()).WithFields(logrus.Fields{
		"fname": fname,
		"err":   err,
	})
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
	err := indexStore.GetExistingContent(chunkHashes, minBlockUsagePercent, longtaillib.CreateAsyncGetExistingContentAPI(getExistingContentComplete))
	if err != nil {
		getExistingContentComplete.wg.Done()
		getExistingContentComplete.wg.Wait()
		err := errors.Wrap(err, "Failed getting existing content index")
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	getExistingContentComplete.wg.Wait()
	if getExistingContentComplete.err != nil {
		err := errors.Wrap(getExistingContentComplete.err, "GetExistingStoreIndexSync completion failed")
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	return getExistingContentComplete.storeIndex, nil
}

// PruneBlocksSync ...
func PruneBlocksSync(
	indexStore longtaillib.Longtail_BlockStoreAPI,
	keepBlockHashes []uint64) (uint32, error) {
	const fname = "PruneBlocksSync"
	log := logrus.WithFields(logrus.Fields{
		"fname":                fname,
		"len(keepBlockHashes)": len(keepBlockHashes),
	})
	log.Debug(fname)

	pruneBlocksComplete := &pruneBlocksCompletionAPI{}
	pruneBlocksComplete.wg.Add(1)
	err := indexStore.PruneBlocks(keepBlockHashes, longtaillib.CreateAsyncPruneBlocksAPI(pruneBlocksComplete))
	if err != nil {
		pruneBlocksComplete.wg.Done()
		pruneBlocksComplete.wg.Wait()
		err := errors.Wrap(err, "Failed pruning blocks in store")
		return 0, errors.Wrap(err, fname)
	}
	pruneBlocksComplete.wg.Wait()
	if pruneBlocksComplete.err != nil {
		err := errors.Wrap(pruneBlocksComplete.err, "PruneBlocks completion failed")
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
	err := store.Flush(targetStoreFlushComplete.asyncFlushAPI)
	if err == nil {
		return targetStoreFlushComplete, nil
	}
	targetStoreFlushComplete.wg.Done()
	err = errors.Wrap(err, "Failed creating flush callback api")
	return nil, errors.Wrap(err, fname)
}

func (f *flushCompletionAPI) Wait() error {
	const fname = "flushCompletionAPI.Wait"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)
	f.wg.Wait()
	if f.err != nil {
		err := errors.Wrap(f.err, "Flush completion failed")
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
		if f.err != nil {
			err = errors.Wrap(f.err, "StoreFlush.Wait() failed")
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

func splitURI(uri string) (string, string) {
	const fname = "splitURI"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"uri":   uri,
	})
	log.Debug(fname)
	i := strings.LastIndex(uri, "/")
	backI := strings.LastIndex(uri, "\\")
	if backI > i || i == -1 {
		i = backI
	}
	if i == -1 {
		return "", uri
	}
	parent := uri[:i]
	name := uri[i+1:]
	if parent == "" && (uri[0] == '\\' || uri[0] == '/') {
		parent = "/"
	}
	return parent, name
}

func GetObjectsByURI(uri string, pathPrefix string, pathSuffix string, opts ...longtailstorelib.BlobStoreOption) ([]longtailstorelib.BlobProperties, error) {
	const fname = "DeleteByURI"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"uri":   uri,
	})
	log.Debug(fname)
	uriParent, uriName := splitURI(uri)
	blobStore, err := longtailstorelib.CreateBlobStoreForURI(uriParent, opts...)
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	defer client.Close()
	return client.GetObjects(uriName+"/"+pathPrefix, pathSuffix)
}

// ReadFromURI ...
func ReadFromURI(uri string, opts ...longtailstorelib.BlobStoreOption) ([]byte, error) {
	const fname = "ReadFromURI"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"uri":   uri,
	})
	log.Debug(fname)
	uriParent, uriName := splitURI(uri)
	blobStore, err := longtailstorelib.CreateBlobStoreForURI(uriParent, opts...)
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
	log.Debugf("read %d bytes from `%s`", len(vbuffer), uri)
	return vbuffer, nil
}

// WriteToURI ...
func WriteToURI(uri string, data []byte, opts ...longtailstorelib.BlobStoreOption) error {
	const fname = "WriteToURI"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"uri":   uri,
	})
	log.Debug(fname)
	uriParent, uriName := splitURI(uri)
	blobStore, err := longtailstorelib.CreateBlobStoreForURI(uriParent, opts...)
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
	log.Debugf("wrote %d bytes to `%s`", len(data), uri)
	return nil
}

// DeleteByURI ...
func DeleteByURI(uri string, opts ...longtailstorelib.BlobStoreOption) error {
	const fname = "DeleteByURI"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"uri":   uri,
	})
	log.Debug(fname)
	uriParent, uriName := splitURI(uri)
	blobStore, err := longtailstorelib.CreateBlobStoreForURI(uriParent, opts...)
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
	err = object.Delete()
	if err != nil && !longtaillib.IsNotExist(err) {
		return errors.Wrap(err, fname)
	}
	log.Debugf("deleted `%s`", uri)
	return nil
}

func ReadBlobWithRetry(
	ctx context.Context,
	client longtailstorelib.BlobClient,
	key string) ([]byte, int, error) {
	const fname = "ReadBlobWithRetry"
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
	if err == nil && !exists {
		err = errors.Wrap(longtaillib.NotExistErr(), fmt.Sprintf("%s/%s does not exist", client.String(), key))
		return nil, retryCount, errors.Wrap(err, fname)
	}
	retryDelay := []time.Duration{0, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, 1 * time.Second, 2 * time.Second}
	blobData, err := objHandle.Read()
	for err != nil {
		if longtaillib.IsNotExist(err) {
			return nil, retryCount, errors.Wrap(err, fname)
		}
		if retryCount == len(retryDelay) {
			err = errors.Wrapf(err, "Failed getBlob %s in store %s", key, client.String())
			log.Error(err)
			return nil, retryCount, errors.Wrap(err, fname)
		}
		err = errors.Wrapf(err, "Retrying getBlob %s in store %s with %s delay", key, client.String(), retryDelay[retryCount])
		log.Info(err)

		time.Sleep(retryDelay[retryCount])
		retryCount++
		blobData, err = objHandle.Read()
	}
	if retryCount > 0 {
		log.Infof("read %d bytes from `%s` after %d retries", len(blobData), key, retryCount)
	} else {
		log.Debugf("read %d bytes from `%s`", len(blobData), key)
	}
	return blobData, retryCount, nil
}

func WriteBlobWithRetry(
	ctx context.Context,
	client longtailstorelib.BlobClient,
	key string,
	blob []byte) (int, error) {
	const fname = "WriteBlobWithRetry"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"client": client,
		"key":    key,
	})

	log.Debug(fname)

	retryCount := 0

	objHandle, err := client.NewObject(key)
	if err != nil {
		return retryCount, errors.Wrap(err, fname)
	}
	if exists, err := objHandle.Exists(); err == nil && !exists {
		var sleepTimes = [...]time.Duration{100 * time.Millisecond, 500 * time.Millisecond, 2 * time.Second}
		ok, err := objHandle.Write(blob)
		if err != nil {
			err := errors.Wrap(err, fmt.Sprintf("failed putBlob at `%s` in `%s`", key, client))
			err = errors.Wrap(err, fname)
			log.Error(err)
			return retryCount, err
		}
		if !ok {
			for _, sleepTime := range sleepTimes {
				retryCount++
				log.Warningf("retrying putBlob (sleeping %s)", sleepTime)
				time.Sleep(sleepTime)
				ok, err = objHandle.Write(blob)
				if err != nil {
					err = errors.Wrap(err, fmt.Sprintf("failed putBlob at `%s` in `%s`", key, client))
					err = errors.Wrap(err, fname)
					log.Error(err)
					return retryCount, err
				}
				if ok {
					break
				}
			}
			if !ok {
				err = fmt.Errorf("putBlob at `%s` in `%s` even after retries", key, client)
				err = errors.Wrap(err, fname)
				log.Error(err)
				return retryCount, err
			}
		}
	}
	if retryCount > 0 {
		log.Infof("wrote %d bytes to `%s` after %d retries", len(blob), key, retryCount)
	} else {
		log.Debugf("wrote %d bytes to `%s`", len(blob), key)
	}
	return retryCount, nil
}

func DeleteBlobWithRetry(
	ctx context.Context,
	client longtailstorelib.BlobClient,
	key string) (int, error) {
	const fname = "DeleteBlob"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"client": client,
		"key":    key,
	})
	log.Debug(fname)

	retryCount := 0
	objHandle, err := client.NewObject(key)
	if err != nil {
		return retryCount, errors.Wrap(err, fname)
	}
	exists, err := objHandle.Exists()
	if err == nil && !exists {
		return retryCount, nil
	}
	retryDelay := []time.Duration{0, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, 1 * time.Second, 2 * time.Second}
	err = objHandle.Delete()
	for err != nil {
		if longtaillib.IsNotExist(err) {
			if retryCount > 0 {
				log.Infof("someone else deleted `%s` after %d retries", key, retryCount)
			} else {
				log.Debugf("someone else deleted `%s`", key)
			}
			return retryCount, nil
		}
		if retryCount == len(retryDelay) {
			err = errors.Wrapf(err, "Failed deleteBlob %s in store %s", key, client.String())
			log.Error(err)
			return retryCount, errors.Wrap(err, fname)
		}
		err = errors.Wrapf(err, "Retrying deleteBlob %s in store %s with %s delay", key, client.String(), retryDelay[retryCount])
		log.Info(err)

		time.Sleep(retryDelay[retryCount])
		retryCount++
		err = objHandle.Delete()
	}
	if retryCount > 0 {
		log.Infof("deleted `%s` after %d retries", key, retryCount)
	} else {
		log.Debugf("deleted `%s`", key)
	}
	return retryCount, nil
}

func BlobExists(ctx context.Context,
	client longtailstorelib.BlobClient,
	key string) (bool, error) {
	const fname = "BlobExists"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"client": client,
		"key":    key,
	})
	log.Debug(fname)

	objHandle, err := client.NewObject(key)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}
	return objHandle.Exists()
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
		"zstd_high":       longtaillib.GetZStdMaxCompressionType(),
		"zstd_low":        longtaillib.GetZStdMaxCompressionType(),
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
	err := fmt.Errorf("unsupported compression algorithm: `%s`", compressionAlgorithm)
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
	const fname = "HashIdentifierToString"
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
