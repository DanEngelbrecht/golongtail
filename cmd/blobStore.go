package main

import (
	"context"
	"fmt"
	"io"

	"github.com/DanEngelbrecht/golongtail/golongtail"
)

type BlobStore interface {
	HasBlob(ctx context.Context, key string) bool
	PutBlob(ctx context.Context, key string, contentType string, blob []byte) error
	GetBlob(ctx context.Context, key string) ([]byte, error)
	PutContent(ctx context.Context, progressFunc golongtail.ProgressFunc, progressContext interface{}, contentIndex golongtail.Longtail_ContentIndex, fs golongtail.Longtail_StorageAPI, contentPath string) error
	GetContent(ctx context.Context, progressFunc golongtail.ProgressFunc, progressContext interface{}, contentIndex golongtail.Longtail_ContentIndex, fs golongtail.Longtail_StorageAPI, contentPath string) error
	io.Closer
	fmt.Stringer
}
