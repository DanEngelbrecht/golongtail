package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"

	"github.com/DanEngelbrecht/golongtail/longtail"
)

// GCSStoreBase is the base object for all chunk and index stores with GCS backing
type GCSStoreBase struct {
	Location string
	client   *storage.Client
	bucket   *storage.BucketHandle
	prefix   string
}

func (s GCSStoreBase) String() string {
	return s.Location
}

// NewGCSStoreBase initializes a base object used for chunk or index stores backed by GCS.
func NewGCSStoreBase(u *url.URL) (GCSStoreBase, error) {
	var err error
	s := GCSStoreBase{Location: u.String()}
	if u.Scheme != "gs" {
		return s, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	s.prefix = u.Path[1:] // strip initial slash

	if s.prefix != "" {
		s.prefix += "/"
	}

	ctx := context.Background()
	s.client, err = storage.NewClient(ctx)
	if err != nil {
		return s, errors.Wrap(err, u.String())
	}

	bucketName := u.Host
	s.bucket = s.client.Bucket(bucketName)

	return s, nil
}

// Close the GCS base store. NOP opertation but needed to implement the store interface.
func (s GCSStoreBase) Close() error { return nil }

func (s GCSStoreBase) putObjectBlob(ctx context.Context, key string, contentType string, blob []byte) error {
	objHandle := s.bucket.Object(s.prefix + key)
	objWriter := objHandle.NewWriter(ctx)

	_, err := objWriter.Write(blob)
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	err = objWriter.Close()
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: contentType})
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	return nil
}

func (s GCSStoreBase) getObjectBlob(ctx context.Context, key string) ([]byte, error) {
	objHandle := s.bucket.Object(s.prefix + key)

	obj, err := objHandle.NewReader(ctx)

	if err != nil {
		return nil, errors.Wrap(err, s.String())
	}
	defer obj.Close()

	b, err := ioutil.ReadAll(obj)

	if err != nil {
		return nil, err
	}

	return b, nil
}

func main() {
	fmt.Println("cmd")
	fs := longtail.CreateFSStorageAPI()
	defer fs.Dispose()

	loc, err := url.Parse("gs://ue4-jenkins-artifacts/chunks")
	if err != nil {
		log.Fatal(err)
	}

	chunkStore, err := NewGCSStoreBase(loc)
	if err != nil {
		log.Fatal(err)
	}
	defer chunkStore.Close()

	blob := "0000/000022a7d087269b03956429b1c2278de308e06791a62e39a6b17dbda78ea32a.cacnk"
	fmt.Printf("Fetching blob `%s`\n", blob)
	data, err := chunkStore.getObjectBlob(context.Background(), blob)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Block `%s` is %d bytes\n", blob, len(data))

	testBlob := "xxxx/remove_me.cacnk"
	err = chunkStore.putObjectBlob(context.Background(), testBlob, "application/zstd", data)
	if err != nil {
		log.Fatal(err)
	}

}
