package longtailstorelib

import (
	"os"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

func TestFSBlobStore(t *testing.T) {
	// This test uses hardcoded paths and is disabled
	t.Skip()

	blobStore, err := NewFSBlobStore("C:\\Temp\\fsblobstore")
	if err != nil {
		t.Errorf("NewFSBlobStore() err == %q", err)
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		t.Errorf("blobStore.NewClient() err == %q", err)
	}
	object, err := client.NewObject("test.txt")
	if err != nil {
		t.Errorf("client.NewObject() err == %q", err)
	}
	ok, err := object.Write([]byte("apa"))
	if !ok {
		t.Errorf("object.Write() ok != true")
	}
	if err != nil {
		t.Errorf("object.Write() err == %q", err)
	}
}

func TestListObjectsInEmptyFSStore(t *testing.T) {
	blobStore, err := NewFSBlobStore("C:\\Temp\\fsblobstore-nonono")
	if err != nil {
		t.Errorf("NewFSBlobStore() err == %q", err)
	}
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	objects, err := client.GetObjects("")
	if err != nil {
		t.Errorf("TestListObjectsInEmptyFSStore() client.GetObjects(\"\")) %v != %v", err, nil)
	}
	if len(objects) != 0 {
		t.Errorf("TestListObjectsInEmptyFSStore() client.GetObjects(\"\")) %d != %d", len(objects), 0)
	}
	obj, _ := client.NewObject("should-not-exist")
	data, err := obj.Read()
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("TestListObjectsInEmptyFSStore() obj.Read()) %v != %v", true, errors.Is(err, os.ErrNotExist))
	}
	if data != nil {
		t.Errorf("TestListObjectsInEmptyFSStore() obj.Read()) %v != %v", nil, data)
	}
}
