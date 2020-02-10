package store

import (
	"context"
	"fmt"
	"net/url"

	"cloud.google.com/go/storage"
	"github.com/DanEngelbrecht/golongtail/lib"
	"github.com/pkg/errors"
)

type gcsBlockStore struct {
	url      *url.URL
	Location string
	client   *storage.Client
	bucket   *storage.BucketHandle

	// Fake it:
	backingStorage    *lib.Longtail_StorageAPI
	backingBlockStore *lib.Longtail_BlockStoreAPI
}

// NewGCSBlockStore ...
func NewGCSBlockStore(u *url.URL) (lib.Longtail_BlockStoreAPI, error) {
	var err error
	if u.Scheme != "gs" {
		return lib.Longtail_BlockStoreAPI{}, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return lib.Longtail_BlockStoreAPI{}, errors.Wrap(err, u.String())
	}

	bucketName := u.Host
	bucket := client.Bucket(bucketName)

	backingStorage := lib.CreateFSStorageAPI()
	backingBlockStore := lib.CreateFSBlockStore(backingStorage, "fake_remote_store")

	s, err := lib.CreateBlockStoreAPI(gcsBlockStore{url: u, Location: u.String(), client: client, bucket: bucket, backingStorage: &backingStorage, backingBlockStore: &backingBlockStore})
	if err != nil {
		backingBlockStore.Dispose()
		backingStorage.Dispose()
		return lib.Longtail_BlockStoreAPI{}, fmt.Errorf("unable to create block store api, %q", err)
	}
	return s, nil
}

// PutStoredBlock ...
func (s gcsBlockStore) PutStoredBlock(storedBlock lib.Longtail_StoredBlock) int {
	err := s.backingBlockStore.PutStoredBlock(storedBlock)
	if err == nil {
		return 0
	}
	return 1
}

// GetStoredBlock ...
func (s gcsBlockStore) GetStoredBlock(blockHash uint64) (lib.Longtail_StoredBlock, int) {
	storedBlock, err := s.backingBlockStore.GetStoredBlock(blockHash)
	if err == nil {
		return storedBlock, 0
	}
	return lib.Longtail_StoredBlock{}, 2
}

// GetIndex ...
func (s gcsBlockStore) GetIndex(defaultHashAPIIdentifier uint32, jobAPI lib.Longtail_JobAPI, progress lib.Progress) (lib.Longtail_ContentIndex, int) {
	contentIndex, err := s.backingBlockStore.GetIndex(defaultHashAPIIdentifier, jobAPI, progress)
	if err == nil {
		return contentIndex, 0
	}
	return lib.Longtail_ContentIndex{}, 2
}

// GetStoredBlockPath ...
func (s gcsBlockStore) GetStoredBlockPath(blockHash uint64) (string, int) {
	path, err := s.backingBlockStore.GetStoredBlockPath(blockHash)
	if err == nil {
		return path, 0
	}
	return "", 2
}

// Close ...
func (s gcsBlockStore) Close() {
	// Create and store updated content index
}
