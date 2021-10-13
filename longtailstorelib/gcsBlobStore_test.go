package longtailstorelib

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

func TestGCSBlobStore(t *testing.T) {
	// This test uses hardcoded paths in gcs and is disabled
	//t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-storage/store")
	if err != nil {
		t.Errorf("url.Parse() err == %s", err)
	}
	blobStore, err := NewGCSBlobStore(u, false)
	if err != nil {
		t.Errorf("NewGCSBlobStore() err == %s", err)
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

func TestListObjectsInEmptyGCSStore(t *testing.T) {
	// This test uses hardcoded paths in gcs and is disabled
	//t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-storage/store-nonono")
	if err != nil {
		t.Errorf("url.Parse() err == %s", err)
	}
	blobStore, err := NewGCSBlobStore(u, false)
	if err != nil {
		t.Errorf("NewGCSBlobStore() err == %s", err)
	}
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	objects, err := client.GetObjects("")
	if err != nil {
		t.Errorf("TestListObjectsInEmptyGCSStore() client.GetObjects(\"\")) %s", err)
	}
	if len(objects) != 0 {
		t.Errorf("TestListObjectsInEmptyGCSStore() client.GetObjects(\"\")) %d != %d", len(objects), 0)
	}
	obj, _ := client.NewObject("should-not-exist")
	data, err := obj.Read()
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("TestListObjectsInEmptyGCSStore() obj.Read()) %s", err)
	}
	if data != nil {
		t.Errorf("TestListObjectsInEmptyGCSStore() obj.Read()) %v != %v", nil, data)
	}
}

func TestGCSBlobStoreVersioning(t *testing.T) {
	// This test uses hardcoded paths in gcs and is disabled
	//t.Skip()
	u, err := url.Parse("gs://longtail-test-de/test-storage/store")
	if err != nil {
		t.Errorf("url.Parse() err == %s", err)
	}
	blobStore, err := NewGCSBlobStore(u, false)
	if err != nil {
		t.Errorf("NewGCSBlobStore() err == %s", err)
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		t.Errorf("blobStore.NewClient() err == %s", err)
	}
	{
		// Clean up any old test data
		object, _ := client.NewObject("test.txt")
		object.Delete()
	}
	object, err := client.NewObject("test.txt")
	if err != nil {
		t.Errorf("client.NewObject() err == %s", err)
	}
	exists, err := object.LockWriteVersion()
	if err != nil {
		t.Errorf("object.LockWriteVersion() err == %s", err)
	}
	if exists {
		t.Errorf("object.LockWriteVersion() exists != false")
	}
	ok, err := object.Write([]byte("apa"))
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
	ok, err = object.Write([]byte("skapa"))
	if ok {
		t.Errorf("object.Write() ok != false")
	}
	if err != nil {
		t.Errorf("object.Write() err == %s", err)
	}
	exists, err = object.LockWriteVersion()
	if err != nil {
		t.Errorf("object.LockWriteVersion() err == %s", err)
	}
	if !exists {
		t.Errorf("object.LockWriteVersion() exists == false")
	}
	ok, err = object.Write([]byte("skapa"))
	if !ok {
		t.Errorf("object.Write() ok == false")
	}
	if err != nil {
		t.Errorf("object.Write() err == %s", err)
	}
	_, err = object.Read()
	if err != nil {
		t.Errorf("object.Read() err == %s", err)
	}
	err = object.Delete()
	if err == nil {
		t.Error("object.Delete() err != nil")
	}
	exists, err = object.LockWriteVersion()
	if err != nil {
		t.Errorf("object.LockWriteVersion() err == %s", err)
	}
	err = object.Delete()
	if err != nil {
		t.Errorf("object.Delete() err == %s", err)
	}
	exists, err = object.Exists()
	if err != nil {
		t.Error("object.Exists() err != nil")
	}
	if exists {
		t.Error("object.Exists() true != false")
	}
}

func TestGCSBlobStoreVersioningStressTest(t *testing.T) {
	// This test uses hardcoded paths in gcs and is disabled
	//t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-storage/store")
	if err != nil {
		t.Errorf("url.Parse() err == %s", err)
	}
	blobStore, err := NewGCSBlobStore(u, false)
	if err != nil {
		t.Errorf("NewGCSBlobStore() err == %s", err)
	}
	client, _ := blobStore.NewClient(context.Background())
	{
		// Clean up any old test data
		object, _ := client.NewObject("test.txt")
		object.Delete()
	}

	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(5)
		for n := 0; n < 5; n++ {
			go func(number int, blobStore BlobStore) {
				err := writeANumberWithRetry(number, blobStore)
				if err != nil {
					t.Errorf("writeANumberWithRetry() err == %s", err)
				}
				wg.Done()
			}(i*5+n+1, blobStore)
		}
		wg.Wait()
	}

	client, err = blobStore.NewClient(context.Background())
	if err != nil {
		t.Errorf("blobStore.NewClient() err == %s", err)
	}
	defer client.Close()
	object, err := client.NewObject("test.txt")
	if err != nil {
		t.Errorf("client.NewObject() err == %s", err)
	}
	data, err := object.Read()
	if err != nil {
		t.Errorf("object.Read() err == %s", err)
	}
	sliceData := strings.Split(string(data), "\n")
	if len(sliceData) != 3*5 {
		t.Errorf("strings.Split() err == %s", err)
	}
	for i := 0; i < 3*5; i++ {
		expected := fmt.Sprintf("%05d", i+1)
		if sliceData[i] != expected {
			t.Errorf("strings.Split() %q == %q", sliceData[i], expected)
		}
	}
}
