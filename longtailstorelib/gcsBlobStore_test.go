package longtailstorelib

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"

	"golang.org/x/net/context"
)

func TestGCSBlobStore(t *testing.T) {
	// This test uses hardcoded paths in gcs and is disabled
	t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-storage/store")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}
	blobStore, err := NewGCSBlobStore(u, false)
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
	// This test uses hardcoded paths in gcs and is disabled
	t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-storage/store")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}
	blobStore, err := NewGCSBlobStore(u, false)
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
	if err == nil {
		t.Error("object.Delete() err != nil")
	}
	exists, err = object.LockWriteVersion()
	if err != nil {
		t.Errorf("object.LockWriteVersion() err == %q", err)
	}
	err = object.Delete()
	if err != nil {
		t.Errorf("object.Delete() err == %q", err)
	}
}

func TestGCSBlobStoreVersioningStressTest(t *testing.T) {
	// This test uses hardcoded paths in gcs and is disabled
	t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-storage/store")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}
	blobStore, err := NewGCSBlobStore(u, false)
	if err != nil {
		t.Errorf("NewGCSBlobStore() err == %q", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(20)
		for n := 0; n < 20; n++ {
			go func(number int, blobStore BlobStore) {
				err := writeANumberWithRetry(number, blobStore)
				if err != nil {
					t.Fatal(err)
				}
				wg.Done()
			}(i*20+n+1, blobStore)
		}
		wg.Wait()
	}

	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	object, err := client.NewObject("test.txt")
	if err != nil {
		t.Fatal(err)
	}
	data, err := object.Read()
	if err != nil {
		t.Fatal(err)
	}
	sliceData := strings.Split(string(data), "\n")
	if len(sliceData) != 10*20 {
		t.Fatal(err)
	}
	for i := 0; i < 10*20; i++ {
		expected := fmt.Sprintf("%05d", i+1)
		if sliceData[i] != expected {
			t.Fatal(err)
		}
	}
}
