package longtailstorelib

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
)

type fsBlobStore struct {
	prefix string
}

type fsBlobClient struct {
	store *fsBlobStore
}

type fsBlobObject struct {
	client *fsBlobClient
	path   string
}

// NewFSBlobStore ...
func NewFSBlobStore(prefix string) (BlobStore, error) {
	s := &fsBlobStore{prefix: prefix}
	return s, nil
}

func (blobStore *fsBlobStore) NewClient(ctx context.Context) (BlobClient, error) {
	return &fsBlobClient{store: blobStore}, nil
}

func (blobStore *fsBlobStore) String() string {
	return "fsstore"
}

func (blobClient *fsBlobClient) NewObject(filepath string) (BlobObject, error) {
	fsPath := path.Join(blobClient.store.prefix, filepath)
	return &fsBlobObject{client: blobClient, path: fsPath}, nil
}

func (blobClient *fsBlobClient) GetObjects(pathPrefix string) ([]BlobProperties, error) {
	return make([]BlobProperties, 0), nil
}

func (blobClient *fsBlobClient) SupportsLocking() bool {
	return true
}

func (blobClient *fsBlobClient) Close() {
}

func (blobClient *fsBlobClient) String() string {
	return "fsstore"
}

func (blobObject *fsBlobObject) Exists() (bool, error) {
	const fname = "fsBlobObject.Exists"
	_, err := os.Stat(blobObject.path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Failed to check for existance `%s`", blobObject.path))
		return false, errors.Wrap(err, fname)
	}
	return true, nil
}

func (blobObject *fsBlobObject) Read() ([]byte, error) {
	const fname = "fsBlobObject.Exists"
	data, err := ioutil.ReadFile(blobObject.path)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Failed to read from `%s`", blobObject.path))
		return nil, errors.Wrap(err, fname)
	}
	return data, nil
}

func (blobObject *fsBlobObject) LockWriteVersion() (bool, error) {
	return blobObject.Exists()
}

func (blobObject *fsBlobObject) Write(data []byte) (bool, error) {
	const fname = "fsBlobObject.Write"
	err := os.MkdirAll(filepath.Dir(blobObject.path), os.ModePerm)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}
	err = ioutil.WriteFile(blobObject.path, data, 0644)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Failed to write to `%s`", blobObject.path))
		return false, errors.Wrap(err, fname)
	}
	return true, nil
}

func (blobObject *fsBlobObject) Delete() error {
	const fname = "fsBlobObject.Delete"
	err := os.Remove(blobObject.path)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Failed to delete `%s`", blobObject.path))
		return errors.Wrap(err, fname)
	}
	return nil
}
