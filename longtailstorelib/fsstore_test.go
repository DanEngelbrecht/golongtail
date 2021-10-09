package longtailstorelib

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

func TestFSBlobStore(t *testing.T) {
	storePath, _ := ioutil.TempDir("", "test")
	blobStore, err := NewFSBlobStore(storePath, true)
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
	storePath, _ := ioutil.TempDir("", "test")
	blobStore, err := NewFSBlobStore(storePath, true)
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

func TestFSBlobStoreVersioning(t *testing.T) {
	storePath, _ := ioutil.TempDir("", "test")
	blobStore, err := NewFSBlobStore(storePath, true)
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
	object.Delete()
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

func TestFSBlobStoreVersioningStressTest(t *testing.T) {
	storePath, _ := ioutil.TempDir("", "test")
	blobStore, err := NewFSBlobStore(storePath, true)
	if err != nil {
		t.Errorf("NewFSBlobStore() err == %q", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(20)
		for n := 0; n < 20; n++ {
			go func(number int, blobStore BlobStore) {
				err := writeANumberWithRetry(number, blobStore)
				if err != nil {
					wg.Done()
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

func TestFSGetObjects(t *testing.T) {
	storePath, _ := ioutil.TempDir("", "test")
	blobStore, err := NewFSBlobStore(storePath, true)
	if err != nil {
		t.Errorf("NewFSBlobStore() err == %q", err)
	}

	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		t.Errorf("blobStore.NewClient() err == %q", err)
	}
	files := []string{"first.txt", "second.txt", "third.txt", "fourth.txt", "nested/first_nested.txt", "nested/second_nested.txt"}
	for _, name := range files {
		object, err := client.NewObject(name)
		if err != nil {
			t.Errorf("client.NewObject() err == %q", err)
		}
		object.Write([]byte(name))
	}

	blobs, err := client.GetObjects("")
	if err != nil {
		t.Errorf("blobStore.GetObjects() err == %q", err)
	}
	if len(blobs) != len(files) {
		t.Errorf("Can't find all written files with client.GetObjects()")
	}

	nestedBlobs, err := client.GetObjects("nest")
	if err != nil {
		t.Errorf("blobStore.GetObjects() err == %q", err)
	}
	if len(nestedBlobs) != 2 {
		t.Errorf("Can't find all written files with client.GetObjects()")
	}
}
