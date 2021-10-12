package longtailstorelib

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

// BlobObject
type BlobObject interface {
	// returns false, nil if the object does not exist
	// returns false, err on error
	// returns true, nil if the object exists
	Exists() (bool, error)

	// Locked the version for Write and Delete operations
	// If the underlying file has changed between the LockWriteVersion call
	// and a Write or Delete operation the operation will fail
	LockWriteVersion() (bool, error)

	// returns nil, error on error
	// returns []byte, nil on success
	// returns nil, nil if the underlying file no longer exists
	Read() ([]byte, error)

	// If no write condition is set:
	//   returns true, nil on success
	//   returns false, err on failure
	// If a write condition is set:
	//   returns true, nil if a version locked write succeeded
	//   returns false, nil if the write was prevented due to a version change
	//   returns false, err on error
	Write(data []byte) (bool, error)

	// Will return an error if a version lock is set with LockWriteVersion()
	// and the underlying file has changed
	Delete() error
}

type BlobProperties struct {
	Size int64
	Name string
}

// BlobClient
type BlobClient interface {
	NewObject(path string) (BlobObject, error)
	GetObjects(pathPrefix string) ([]BlobProperties, error)
	SupportsLocking() bool
	String() string
	Close()
}

// BlobStore
type BlobStore interface {
	NewClient(ctx context.Context) (BlobClient, error)
	String() string
}

func CreateBlobStoreForURI(uri string) (BlobStore, error) {
	// Special case since filepaths may not bare nicely as a url
	if strings.HasPrefix(uri, "fsblob://") {
		return NewFSBlobStore(uri[len("fsblob://"):], false)
	}
	blobStoreURL, err := url.Parse(uri)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			return NewGCSBlobStore(blobStoreURL, false)
		case "s3":
			return NewS3BlobStore(blobStoreURL)
		case "abfs":
			return nil, fmt.Errorf("azure Gen1 storage not yet implemented")
		case "abfss":
			return nil, fmt.Errorf("azure Gen2 storage not yet implemented")
		case "file":
			return NewFSBlobStore(blobStoreURL.Host+blobStoreURL.Path, false)
		}
	}

	return NewFSBlobStore(uri, false)
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
