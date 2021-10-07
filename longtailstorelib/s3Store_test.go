package longtailstorelib

import (
	"net/url"
	"testing"

	"golang.org/x/net/context"
)

func TestS3BlobStore(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	t.Skip()

	u, err := url.Parse("s3://longtail-test/test-s3-blob-store")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}
	blobStore, err := NewS3BlobStore(u)
	if err != nil {
		t.Errorf("NewS3BlobStore() err == %q", err)
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		t.Errorf("blobStore.NewClient() err == %q", err)
	}
	defer client.Close()
	object, err := client.NewObject("test.txt")
	if err != nil {
		t.Errorf("client.NewObject() err == %q", err)
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
		t.Errorf("object.Write() err == %q", err)
	}
	blobs, err := client.GetObjects("")
	if err != nil {
		t.Errorf("client.GetObjects(\"\") err == %q", err)
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
	_, _ = object.Write([]byte("dog"))

	object, _ = client.NewObject("path/second.txt")
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
