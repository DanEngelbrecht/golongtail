package longtailstorelib

import (
	"net/url"
	"testing"

	"golang.org/x/net/context"
)

func TestGCSBlobStore(t *testing.T) {
	// This test uses hardcoded paths and is disabled
	return
	u, err := url.Parse("gs://longtail-storage/test-storage/store")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}
	blobStore, err := NewGCSBlobStore(u)
	if err != nil {
		t.Errorf("NewGCSBlobStore() err == %q", err)
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

func TestGCSBlobStoreVersioning(t *testing.T) {
	// This test uses hardcoded paths and is disabled
	return
	u, err := url.Parse("gs://longtail-storage/test-storage/store")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}
	blobStore, err := NewGCSBlobStore(u)
	if err != nil {
		t.Errorf("NewGCSBlobStore() err == %q", err)
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		t.Errorf("blobStore.NewClient() err == %q", err)
	}
	object, err := client.NewObject("test.txt")
	if err != nil {
		t.Errorf("client.NewObject() err == %q", err)
	}
	err = object.Delete()
	exists, err := object.LockWriteVersion()
	if err != nil {
		t.Errorf("object.LockWriteVersion() err == %q", err)
	}
	if exists {
		t.Errorf("object.LockWriteVersion() exists != false")
	}
	ok, err := object.Write([]byte("apa"))
	if !ok {
		t.Errorf("object.Write() ok != true")
	}
	if err != nil {
		t.Errorf("object.Write() err == %q", err)
	}
	ok, err = object.Write([]byte("skapa"))
	if ok {
		t.Errorf("object.Write() ok != false")
	}
	if err != nil {
		t.Errorf("object.Write() err == %q", err)
	}
	exists, err = object.LockWriteVersion()
	if err != nil {
		t.Errorf("object.LockWriteVersion() err == %q", err)
	}
	if !exists {
		t.Errorf("object.LockWriteVersion() exists == false")
	}
	ok, err = object.Write([]byte("skapa"))
	if !ok {
		t.Errorf("object.Write() ok == false")
	}
	if err != nil {
		t.Errorf("object.Write() err == %q", err)
	}
	_, err = object.Read()
	if err != nil {
		t.Errorf("object.Read() err == %q", err)
	}
	err = object.Delete()
	if err != nil {
		t.Errorf("object.Delete() err == %q", err)
	}
}
