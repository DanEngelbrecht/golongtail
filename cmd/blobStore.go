package main

import (
	"io"
	"fmt"
	"context"
)

type BlobStore interface {
	HasBlob(ctx context.Context, key string) bool
	PutBlob(ctx context.Context, key string, contentType string, blob []byte) error
	GetBlob(ctx context.Context, key string) ([]byte, error)
	io.Closer
	fmt.Stringer
}
