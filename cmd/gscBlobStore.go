package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
)

// GCSBlobStore is the base object for all chunk and index stores with GCS backing
type GCSBlobStore struct {
	Location string
	client   *storage.Client
	bucket   *storage.BucketHandle
}

func (s GCSBlobStore) String() string {
	return s.Location
}

// NewGCSBlobStore initializes a base object used for chunk or index stores backed by GCS.
func NewGCSBlobStore(u *url.URL) (*GCSBlobStore, error) {
	var err error
	s := &GCSBlobStore{Location: u.String()}
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

// Close the GCS base store. NOP opertation but needed to implement the store interface.
func (s GCSBlobStore) Close() error { return nil }

// HasObjectBlob ...
func (s GCSBlobStore) HasBlob(ctx context.Context, key string) bool {
	objHandle := s.bucket.Object(key)
	_, err := objHandle.Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return false
	}
	return true
}

// PutObjectBlob ...
func (s GCSBlobStore) PutBlob(ctx context.Context, key string, contentType string, blob []byte) error {
	objHandle := s.bucket.Object(key)
	objWriter := objHandle.NewWriter(ctx)

	_, err := objWriter.Write(blob)
	if err != nil {
		objWriter.Close()
		return errors.Wrap(err, s.String())
	}

	err = objWriter.Close()
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: contentType})
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	return nil
}

// GetObjectBlob ...
func (s GCSBlobStore) GetBlob(ctx context.Context, key string) ([]byte, error) {
	objHandle := s.bucket.Object(key)
	obj, err := objHandle.NewReader(ctx)
	if err != nil {
		return nil, errors.Wrap(err, s.String())
	}
	defer obj.Close()

	b, err := ioutil.ReadAll(obj)

	if err != nil {
		return nil, err
	}

	return b, nil
}
