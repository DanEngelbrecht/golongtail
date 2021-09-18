package longtailstorelib

import (
	"context"
)

// BlobObject
type BlobObject interface {
	Exists() (bool, error)
	LockWriteVersion() (bool, error)
	Read() ([]byte, error)
	Write(data []byte) (bool, error)
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
