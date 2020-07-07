package longtailstorelib

import "context"

// BlobObject
type BlobObject interface {
	LockState() error
	Read() ([]byte, error)
	Write(data []byte) error
}

// BlobClient
type BlobClient interface {
	NewObject(path string) (BlobObject, error)
}

// BlobStore
type BlobStore interface {
	OpenClient(path string, ctx context.Context) (BlobClient, error)
}
