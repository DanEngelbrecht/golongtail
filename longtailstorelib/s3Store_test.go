package longtailstorelib

import (
	"net/url"
	"os"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

func TestS3BlobStore(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	//t.Skip()

	u, err := url.Parse("s3://longtail-test/test-s3-blob-store")
	if err != nil {
		t.Errorf("url.Parse() err == %s", err)
	}
	blobStore, err := NewS3BlobStore(u)
	if err != nil {
		t.Errorf("NewS3BlobStore() err == %s", err)
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		t.Errorf("blobStore.NewClient() err == %s", err)
	}
	defer client.Close()
	{
		// Clean up any old test data
		object, _ := client.NewObject("test.txt")
		object.Delete()
		object, _ = client.NewObject("path/first.txt")
		object.Delete()
		object, _ = client.NewObject("path/second.txt")
		object.Delete()
	}
	object, err := client.NewObject("test.txt")
	if err != nil {
		t.Errorf("client.NewObject() err == %s", err)
	}

	exists, err := object.Exists()
	if err != nil {
		t.Error("object.Exists() err != nil")
	}
	if exists {
		t.Error("object.Exists() true != false")
	}

	data, err := object.Read()
	if data != nil && err != nil {
		t.Errorf("object.Read() nil != %v", err)
	}

	testData := []byte("apa")
	ok, err := object.Write(testData)
	if !ok {
		t.Errorf("object.Write() ok != true")
	}
	if err != nil {
		t.Errorf("object.Write() err == %s", err)
	}

	exists, err = object.Exists()
	if err != nil {
		t.Error("object.Exists() err != nil")
	}
	if !exists {
		t.Error("object.Exists() false != true")
	}

	blobs, err := client.GetObjects("")
	if err != nil {
		t.Errorf("client.GetObjects(\"\") err == %s", err)
	}
	if blobs[0].Name != "test.txt" {
		t.Errorf("blobs[0].Name %s != %s", blobs[0].Name, "test.txt")
	}
	data, err = object.Read()
	if len(data) != 3 {
		t.Errorf("len(data) %d != %d", len(data), 3)
	}
	for i, d := range data {
		if d != testData[i] {
			t.Errorf("%d != testData[%d]", int(d), int(testData[i]))
		}
	}

	object, _ = client.NewObject("path/first.txt")
	object.Delete()
	_, _ = object.Write([]byte("dog"))
	object, _ = client.NewObject("path/second.txt")
	object.Delete()
	_, _ = object.Write([]byte("cat"))

	objects, _ := client.GetObjects("")
	if len(objects) != 3 {
		t.Errorf("len(objects) %d != 3", len(objects))
	}

	objects, _ = client.GetObjects("path/")
	if len(objects) != 2 {
		t.Errorf("len(objects) %d != 2", len(objects))
	}
}

func TestListObjectsInEmptyS3Store(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	//t.Skip()

	u, err := url.Parse("s3://longtail-test/test-s3-blob-store-nonono")
	if err != nil {
		t.Errorf("url.Parse() err == %s", err)
	}
	blobStore, err := NewS3BlobStore(u)
	if err != nil {
		t.Errorf("NewS3BlobStore() err == %s", err)
	}
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	objects, err := client.GetObjects("")
	if err != nil {
		t.Errorf("TestListObjectsInEmptyS3Store() client.GetObjects(\"\")) %s", err)
	}
	if len(objects) != 0 {
		t.Errorf("TestListObjectsInEmptyS3Store() client.GetObjects(\"\")) %d != %d", len(objects), 0)
	}
	obj, _ := client.NewObject("should-not-exist")
	data, err := obj.Read()
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("TestListObjectsInEmptyS3Store() obj.Read()) %s", err)
	}
	if data != nil {
		t.Errorf("TestListObjectsInEmptyS3Store() obj.Read()) %v != %v", nil, data)
	}
}
