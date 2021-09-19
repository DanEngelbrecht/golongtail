package longtailstorelib

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
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

func createBlobStoreForURI(uri string) (BlobStore, error) {
	blobStoreURL, err := url.Parse(uri)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			return NewGCSBlobStore(blobStoreURL)
		case "s3":
			return NewS3BlobStore(blobStoreURL)
		case "abfs":
			return nil, fmt.Errorf("azure Gen1 storage not yet implemented")
		case "abfss":
			return nil, fmt.Errorf("azure Gen2 storage not yet implemented")
		case "file":
			return NewFSBlobStore(blobStoreURL.Path[1:])
		}
	}

	return NewFSBlobStore(uri)
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

// ReadFromURI ...
func ReadFromURI(uri string) ([]byte, error) {
	uriParent, uriName := splitURI(uri)
	blobStore, err := createBlobStoreForURI(uriParent)
	if err != nil {
		return nil, err
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		return nil, err
	}
	defer client.Close()
	object, err := client.NewObject(uriName)
	if err != nil {
		return nil, err
	}
	vbuffer, err := object.Read()
	if err != nil {
		return nil, err
	}
	return vbuffer, nil
}

// ReadFromURI ...
func WriteToURI(uri string, data []byte) error {
	uriParent, uriName := splitURI(uri)
	blobStore, err := createBlobStoreForURI(uriParent)
	if err != nil {
		return err
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		return err
	}
	defer client.Close()
	object, err := client.NewObject(uriName)
	if err != nil {
		return err
	}
	_, err = object.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func readBlobWithRetry(
	ctx context.Context,
	client BlobClient,
	key string) ([]byte, int, error) {
	retryCount := 0
	objHandle, err := client.NewObject(key)
	if err != nil {
		return nil, retryCount, err
	}
	exists, err := objHandle.Exists()
	if err != nil {
		return nil, retryCount, err
	}
	if !exists {
		return nil, retryCount, longtaillib.ErrENOENT
	}
	blobData, err := objHandle.Read()
	if err != nil {
		log.Printf("Retrying getBlob %s in store %s\n", key, client.String())
		retryCount++
		blobData, err = objHandle.Read()
	}
	if err != nil {
		log.Printf("Retrying 500 ms delayed getBlob %s in store %s\n", key, client.String())
		time.Sleep(500 * time.Millisecond)
		retryCount++
		blobData, err = objHandle.Read()
	}
	if err != nil {
		log.Printf("Retrying 2 s delayed getBlob %s in store %s\n", key, client.String())
		time.Sleep(2 * time.Second)
		retryCount++
		blobData, err = objHandle.Read()
	}

	if err != nil {
		return nil, retryCount, err
	}

	return blobData, retryCount, nil
}
