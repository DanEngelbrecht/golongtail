package longtailstorelib

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	log := logrus.WithField("uri", uri)
	uriParent, uriName := splitURI(uri)
	blobStore, err := CreateBlobStoreForURI(uriParent)
	if err != nil {
		log.WithError(err).Error("createBlobStoreForURI failed")
		return nil, errors.Wrap(err, "ReadFromURI")
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		log.WithError(err).Error("blobStore.NewClient failed")
		return nil, errors.Wrap(err, "ReadFromURI")
	}
	defer client.Close()
	object, err := client.NewObject(uriName)
	if err != nil {
		log.WithError(err).Error("client.NewObject failed")
		return nil, errors.Wrap(err, "ReadFromURI")
	}
	vbuffer, err := object.Read()
	if err != nil {
		log.WithError(err).Error("object.Read failed")
		return nil, errors.Wrap(err, "ReadFromURI")
	}
	return vbuffer, nil
}

// WriteToURI ...
func WriteToURI(uri string, data []byte) error {
	uriParent, uriName := splitURI(uri)
	blobStore, err := CreateBlobStoreForURI(uriParent)
	if err != nil {
		return errors.Wrap(err, "WriteToURI")
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		return errors.Wrap(err, "WriteToURI")
	}
	defer client.Close()
	object, err := client.NewObject(uriName)
	if err != nil {
		return errors.Wrap(err, "WriteToURI")
	}
	_, err = object.Write(data)
	if err != nil {
		return errors.Wrap(err, "WriteToURI")
	}
	return nil
}

func ReadBlobWithRetry(
	ctx context.Context,
	client BlobClient,
	key string) ([]byte, int, error) {
	retryCount := 0
	objHandle, err := client.NewObject(key)
	if err != nil {
		return nil, retryCount, errors.Wrap(err, "ReadBlobWithRetry")
	}
	exists, err := objHandle.Exists()
	if err != nil {
		return nil, retryCount, errors.Wrap(err, "ReadBlobWithRetry")
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
