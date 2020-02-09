package store

import (
	"context"
	"fmt"
	"io"

	"github.com/DanEngelbrecht/golongtail/lib"
)

type BlobStore interface {
	HasBlob(ctx context.Context, key string) bool
	PutBlob(ctx context.Context, key string, contentType string, blob []byte) error
	GetBlob(ctx context.Context, key string) ([]byte, error)
	PutContent(ctx context.Context, progress lib.Progress, contentIndex lib.Longtail_ContentIndex, fs lib.Longtail_StorageAPI, contentPath string) error
	GetContent(ctx context.Context, progress lib.Progress, contentIndex lib.Longtail_ContentIndex, fs lib.Longtail_StorageAPI, contentPath string) error
	io.Closer
	fmt.Stringer
}
