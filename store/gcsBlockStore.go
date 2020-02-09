package store

import (
	"context"
	"fmt"
	"net/url"

	"cloud.google.com/go/storage"
	"github.com/DanEngelbrecht/golongtail/lib"
	"github.com/pkg/errors"
)

type GCSBlockStore struct {
	url      *url.URL
	Location string
	client   *storage.Client
	bucket   *storage.BucketHandle
}

func NewGCSBlockStore(u *url.URL) (*GCSBlockStore, error) {
	var err error
	s := &GCSBlockStore{url: u, Location: u.String()}
	if u.Scheme != "gs" {
		return s, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	ctx := context.Background()
	s.client, err = storage.NewClient(ctx)
	if err != nil {
		return s, errors.Wrap(err, u.String())
	}

	bucketName := u.Host
	s.bucket = s.client.Bucket(bucketName)

	return s, nil
}

func (s GCSBlockStore) PutStoredBlock(storedBlock lib.Longtail_StoredBlock) int {
	// TODO
	return 0
}

func (s GCSBlockStore) GetStoredBlock(blockHash uint64) (lib.Longtail_StoredBlock, int) {
	// TODO
	return lib.Longtail_StoredBlock{}, 0
}

func (s GCSBlockStore) GetIndex(defaultHashAPIIdentifier uint32, progress lib.Progress) (lib.Longtail_ContentIndex, int) {
	// TODO
	return lib.Longtail_ContentIndex{}, 0
}

func (s GCSBlockStore) GetStoredBlockPath(blockHash uint64) (string, int) {
	// TODO
	return "", 0
}

func (s GCSBlockStore) Close() {
	// Create and store updated content index
}
