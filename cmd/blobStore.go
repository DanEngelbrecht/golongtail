package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"path"

	"github.com/DanEngelbrecht/golongtail/longtail"
)

type BlobStore interface {
	HasBlob(ctx context.Context, key string) bool
	PutBlob(ctx context.Context, key string, contentType string, blob []byte) error
	GetBlob(ctx context.Context, key string) ([]byte, error)
	PutContent(ctx context.Context, contentIndex longtail.Longtail_ContentIndex, fs longtail.Longtail_StorageAPI, contentPath string) error
	GetContent(ctx context.Context, contentIndex longtail.Longtail_ContentIndex, fs longtail.Longtail_StorageAPI, contentPath string) error
	io.Closer
	fmt.Stringer
}
