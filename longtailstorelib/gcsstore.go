package longtailstorelib

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

type gcsBlobStore struct {
	bucketName     string
	prefix         string
	disableLocking bool
}

type gcsBlobClient struct {
	client *storage.Client
	ctx    context.Context
	store  *gcsBlobStore
	bucket *storage.BucketHandle
}

type gcsBlobObject struct {
	objHandle      *storage.ObjectHandle
	ctx            context.Context
	path           string
	writeCondition *storage.Conditions
	client         *gcsBlobClient
}

const (
	// If the meta generation changes between our lock and write/close we get a gcs error with code 409 or 412
	metadataForObjectChanged = 409
	writeConditionFailed     = 412
	rateLimitExceeded        = 429
)

// NewGCSBlobStore ...
func NewGCSBlobStore(u *url.URL, disableLocking bool) (BlobStore, error) {
	if u.Scheme != "gs" {
		return nil, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}
	prefix := u.Path
	if len(u.Path) > 0 {
		prefix = u.Path[1:] // strip initial slash
	}

	if prefix != "" {
		prefix += "/"
	}

	s := &gcsBlobStore{bucketName: u.Host, prefix: prefix, disableLocking: disableLocking}
	return s, nil
}

func (blobStore *gcsBlobStore) NewClient(ctx context.Context) (BlobClient, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, blobStore.bucketName)
	}

	bucket := client.Bucket(blobStore.bucketName)
	return &gcsBlobClient{client: client, ctx: ctx, store: blobStore, bucket: bucket}, nil
}

func (blobStore *gcsBlobStore) String() string {
	return "gs://" + blobStore.bucketName + "/" + blobStore.prefix
}

func (blobClient *gcsBlobClient) NewObject(path string) (BlobObject, error) {
	gcsPath := blobClient.store.prefix + path
	objHandle := blobClient.bucket.Object(gcsPath)
	return &gcsBlobObject{
			objHandle:      objHandle,
			ctx:            blobClient.ctx,
			path:           gcsPath,
			writeCondition: nil,
			client:         blobClient},
		nil
}

func (blobClient *gcsBlobClient) GetObjects(pathPrefix string) ([]BlobProperties, error) {
	var items []BlobProperties
	it := blobClient.bucket.Objects(blobClient.ctx, &storage.Query{
		Prefix: blobClient.store.prefix + pathPrefix,
	})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		itemName := attrs.Name[len(blobClient.store.prefix):]
		items = append(items, BlobProperties{Size: attrs.Size, Name: itemName})
	}
	return items, nil
}

func (blobClient *gcsBlobClient) SupportsLocking() bool {
	return !blobClient.store.disableLocking
}

func (blobClient *gcsBlobClient) Close() {
	blobClient.client.Close()
}

func (blobClient *gcsBlobClient) String() string {
	return blobClient.store.String()
}

func (blobObject *gcsBlobObject) Read() ([]byte, error) {
	reader, err := blobObject.objHandle.NewReader(blobObject.ctx)
	if err == storage.ErrObjectNotExist {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, blobObject.path)
	}
	data, err := ioutil.ReadAll(reader)
	err2 := reader.Close()
	if err == storage.ErrObjectNotExist || err2 == storage.ErrObjectNotExist {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, blobObject.path)
	} else if err2 != nil {
		return nil, err2
	}
	return data, nil
}

func (blobObject *gcsBlobObject) LockWriteVersion() (bool, error) {
	objAttrs, err := blobObject.objHandle.Attrs(blobObject.ctx)
	if err == storage.ErrObjectNotExist {
		blobObject.writeCondition = &storage.Conditions{DoesNotExist: true}
		return false, nil
	} else if err != nil {
		return false, err
	}

	blobObject.writeCondition = &storage.Conditions{GenerationMatch: objAttrs.Generation, DoesNotExist: false}
	return true, nil
}

func (blobObject *gcsBlobObject) Exists() (bool, error) {
	_, err := blobObject.objHandle.Attrs(blobObject.ctx)
	if err == storage.ErrObjectNotExist {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (blobObject *gcsBlobObject) Write(data []byte) (bool, error) {
	var writer *storage.Writer
	if blobObject.writeCondition == nil {
		writer = blobObject.objHandle.NewWriter(blobObject.ctx)
	} else {
		writer = blobObject.objHandle.If(*blobObject.writeCondition).NewWriter(blobObject.ctx)
	}

	_, err := writer.Write(data)
	err2 := writer.Close()
	if err != nil {
		return false, errors.Wrap(err, blobObject.path)
	}
	if e, ok := err2.(*googleapi.Error); ok {
		if e.Code == writeConditionFailed || e.Code == metadataForObjectChanged || e.Code == rateLimitExceeded {
			return false, nil
		}
		return false, err2
	} else if err2 != nil {
		return false, err2
	}

	_, err = blobObject.objHandle.Update(blobObject.ctx, storage.ObjectAttrsToUpdate{ContentType: "application/octet-stream"})
	if err != nil {
		return true, err
	}
	return true, nil
}

func (blobObject *gcsBlobObject) Delete() error {
	_, err := blobObject.objHandle.Attrs(blobObject.ctx)
	if err == storage.ErrObjectNotExist {
		return nil
	}
	if err != nil {
		return err
	}
	if blobObject.writeCondition == nil {
		err = blobObject.objHandle.Delete(blobObject.ctx)
	} else {
		err = blobObject.objHandle.If(*blobObject.writeCondition).Delete(blobObject.ctx)
	}
	return err
}
