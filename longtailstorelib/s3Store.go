package longtailstorelib

import (
	"context"
	"fmt"
	"net/url"
)

// TODO: Not yet implemented, shell here to show how what it would require to support S3

type s3BlobStore struct {
}

type s3BlobClient struct {
	ctx   context.Context
	store *s3BlobStore
}

type s3BlobObject struct {
	ctx    context.Context
	client *s3BlobClient
}

// NewS3BlobStore ...
func NewS3BlobStore(u *url.URL) (BlobStore, error) {
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}
	s := &s3BlobStore{}
	return s, nil
}

func (blobStore *s3BlobStore) NewClient(ctx context.Context) (BlobClient, error) {
	return &s3BlobClient{store: blobStore, ctx: ctx}, nil
}

func (blobStore *s3BlobStore) String() string {
	return ""
}

func (blobClient *s3BlobClient) NewObject(path string) (BlobObject, error) {
	return &s3BlobObject{
			ctx:    blobClient.ctx,
			client: blobClient},
		nil
}

func (blobClient *s3BlobClient) GetObjects(pathPrefix string) ([]BlobProperties, error) {
	return nil, fmt.Errorf("S3 storage not yet implemented")
}

func (blobClient *s3BlobClient) Close() {
}

func (blobClient *s3BlobClient) String() string {
	return blobClient.store.String()
}

func (blobObject *s3BlobObject) Read() ([]byte, error) {
	return nil, fmt.Errorf("S3 storage not yet implemented")
}

func (blobObject *s3BlobObject) LockWriteVersion() (bool, error) {
	return false, fmt.Errorf("S3 storage not yet implemented")
}

func (blobObject *s3BlobObject) Exists() (bool, error) {
	return false, fmt.Errorf("S3 storage not yet implemented")
}

func (blobObject *s3BlobObject) Write(data []byte) (bool, error) {
	return false, fmt.Errorf("S3 storage not yet implemented")
}

func (blobObject *s3BlobObject) Delete() error {
	return fmt.Errorf("S3 storage not yet implemented")
}
