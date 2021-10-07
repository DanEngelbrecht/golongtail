package longtailstorelib

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
)

type memBlob struct {
	generation int
	path       string
	data       []byte
}

type memBlobStore struct {
	blobs           map[string]*memBlob
	blobsMutex      sync.RWMutex
	prefix          string
	supportsLocking bool
}

type memBlobClient struct {
	store *memBlobStore
}

type memBlobObject struct {
	client           *memBlobClient
	path             string
	lockedGeneration *int
}

// NewMemBlobStore ...
func NewMemBlobStore(prefix string, supportsLocking bool) (BlobStore, error) {
	s := &memBlobStore{prefix: prefix, blobs: make(map[string]*memBlob), supportsLocking: supportsLocking}
	return s, nil
}

func (blobStore *memBlobStore) NewClient(ctx context.Context) (BlobClient, error) {
	return &memBlobClient{store: blobStore}, nil
}

func (blobStore *memBlobStore) String() string {
	return "memstore"
}

func (blobClient *memBlobClient) NewObject(filepath string) (BlobObject, error) {
	return &memBlobObject{client: blobClient, path: filepath}, nil
}

func (blobClient *memBlobClient) GetObjects(pathPrefix string) ([]BlobProperties, error) {
	blobClient.store.blobsMutex.RLock()
	defer blobClient.store.blobsMutex.RUnlock()
	properties := make([]BlobProperties, 0)
	for key, blob := range blobClient.store.blobs {
		if strings.HasPrefix(key, pathPrefix) {
			properties = append(properties, BlobProperties{Name: key, Size: int64(len(blob.data))})
		}
	}
	return properties, nil
}

func (blobClient *memBlobClient) SupportsLocking() bool {
	return blobClient.store.supportsLocking
}

func (blobClient *memBlobClient) Close() {
}

func (blobClient *memBlobClient) String() string {
	return "memstore"
}

func (blobObject *memBlobObject) Exists() (bool, error) {
	blobObject.client.store.blobsMutex.RLock()
	defer blobObject.client.store.blobsMutex.RUnlock()
	_, exists := blobObject.client.store.blobs[blobObject.path]
	return exists, nil
}

func (blobObject *memBlobObject) Read() ([]byte, error) {
	blobObject.client.store.blobsMutex.RLock()
	defer blobObject.client.store.blobsMutex.RUnlock()
	blob, exists := blobObject.client.store.blobs[blobObject.path]
	if !exists {
		return nil, nil
	}
	return blob.data, nil
}

func (blobObject *memBlobObject) LockWriteVersion() (bool, error) {
	blobObject.client.store.blobsMutex.RLock()
	defer blobObject.client.store.blobsMutex.RUnlock()
	blob, exists := blobObject.client.store.blobs[blobObject.path]
	blobObject.lockedGeneration = new(int)
	if !exists {
		*blobObject.lockedGeneration = -1
		return false, nil
	}
	*blobObject.lockedGeneration = blob.generation
	return true, nil
}

func (blobObject *memBlobObject) Write(data []byte) (bool, error) {
	blobObject.client.store.blobsMutex.Lock()
	defer blobObject.client.store.blobsMutex.Unlock()

	blob, exists := blobObject.client.store.blobs[blobObject.path]

	if blobObject.lockedGeneration != nil {
		if exists {
			if blob.generation != *blobObject.lockedGeneration {
				return false, nil
			}
		} else if (*blobObject.lockedGeneration) != -1 {
			return false, nil
		}
	}

	if !exists {
		blob = &memBlob{generation: 0, path: blobObject.path, data: data}
		blobObject.client.store.blobs[blobObject.path] = blob
		return true, nil
	}

	blob.data = data
	blob.generation++
	return true, nil
}

func (blobObject *memBlobObject) Delete() error {
	blobObject.client.store.blobsMutex.Lock()
	defer blobObject.client.store.blobsMutex.Unlock()

	if blobObject.lockedGeneration != nil {
		blob, exists := blobObject.client.store.blobs[blobObject.path]
		if !exists {
			return longtaillib.ErrENOENT
		}
		if blob.generation != *blobObject.lockedGeneration {
			return fmt.Errorf("memBlobObject: generation lock mismatch %s", blobObject.path)
		}
	}
	delete(blobObject.client.store.blobs, blobObject.path)
	return nil
}
