package main

import (
	"context"
	"io/ioutil"
	"os"
	"path"
)

// FSBloblStore is the base object for all chunk and index stores with FS backing
type FSBloblStore struct {
	root string
}

func (s FSBloblStore) String() string {
	return s.root
}

// NewFSBlobStore initializes a base object used for chunk or index stores backed by FS.
func NewFSBlobStore(rootPath string) (*FSBloblStore, error) {
	s := &FSBloblStore{root: rootPath}

	return s, nil
}

// Close the FS base store. NOP opertation but needed to implement the store interface.
func (s FSBloblStore) Close() error { return nil }

// HasObjectBlob ...
func (s FSBloblStore) HasBlob(ctx context.Context, key string) bool {
	blobPath := path.Join(s.root, key)
	if _, err := os.Stat(blobPath); os.IsNotExist(err) {
		return false
	}
	return true
}

// PutObjectBlob ...
func (s FSBloblStore) PutBlob(ctx context.Context, key string, contentType string, blob []byte) error {
	blobPath := path.Join(s.root, key)
	blobParent, _ := path.Split(blobPath)
	err := os.MkdirAll(blobParent, os.ModePerm)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(blobPath, blob, 0644)
	if err != nil {
		return err
	}
	return nil
}

// GetObjectBlob ...
func (s FSBloblStore) GetBlob(ctx context.Context, key string) ([]byte, error) {
	blobPath := path.Join(s.root, key)
	return ioutil.ReadFile(blobPath)
}
