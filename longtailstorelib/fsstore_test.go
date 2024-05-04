package longtailstorelib

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/alecthomas/assert/v2"
)

func TestFSBlobStore(t *testing.T) {
	storePath, _ := os.MkdirTemp("", "test")
	blobStore, err := NewFSBlobStore(storePath, true)
	assert.NoError(t, err)
	assert.NotEqual(t, blobStore, nil)
	client, err := blobStore.NewClient(context.Background())
	assert.NoError(t, err)
	assert.NotEqual(t, client, nil)
	defer client.Close()
	object, err := client.NewObject("test.txt")
	assert.NoError(t, err)
	assert.NotEqual(t, object, nil)
	ok, err := object.Write([]byte("apa"))
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestListObjectsInEmptyFSStore(t *testing.T) {
	storePath, _ := os.MkdirTemp("", "test")
	blobStore, err := NewFSBlobStore(storePath, true)
	assert.NoError(t, err)
	assert.NotEqual(t, blobStore, nil)
	client, _ := blobStore.NewClient(context.Background())
	assert.NoError(t, err)
	assert.NotEqual(t, client, nil)
	defer client.Close()
	objects, err := client.GetObjects("")
	assert.NoError(t, err)
	assert.Equal(t, len(objects), 0)
	obj, err := client.NewObject("should-not-exist")
	assert.NoError(t, err)
	assert.NotEqual(t, obj, nil)
	data, err := obj.Read()
	assert.True(t, longtaillib.IsNotExist(err))
	assert.Equal(t, data, nil)
}

func TestFSBlobStoreVersioning(t *testing.T) {
	storePath, _ := os.MkdirTemp("", "test")
	blobStore, err := NewFSBlobStore(storePath, true)
	assert.NoError(t, err)
	assert.NotEqual(t, blobStore, nil)
	client, err := blobStore.NewClient(context.Background())
	assert.NoError(t, err)
	assert.NotEqual(t, client, nil)
	defer client.Close()
	object, err := client.NewObject("test.txt")
	assert.NoError(t, err)
	assert.NotEqual(t, object, nil)
	object.Delete()
	exists, err := object.LockWriteVersion()
	assert.NoError(t, err)
	assert.False(t, exists)
	ok, err := object.Write([]byte("apa"))
	assert.True(t, ok)
	assert.NoError(t, err)
	ok, err = object.Write([]byte("skapa"))
	assert.False(t, ok)
	assert.NoError(t, err)
	exists, err = object.LockWriteVersion()
	assert.NoError(t, err)
	assert.True(t, exists)
	ok, err = object.Write([]byte("skapa"))
	assert.True(t, ok)
	assert.NoError(t, err)
	_, err = object.Read()
	assert.NoError(t, err)
	err = object.Delete()
	assert.Error(t, err)
	exists, err = object.LockWriteVersion()
	assert.True(t, exists)
	assert.NoError(t, err)
	err = object.Delete()
	assert.NoError(t, err)
}

func TestFSBlobStoreVersioningStressTest(t *testing.T) {
	storePath, _ := os.MkdirTemp("", "test")
	blobStore, err := NewFSBlobStore(storePath, true)
	assert.NoError(t, err)
	assert.NotEqual(t, blobStore, nil)

	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(5)
		for n := 0; n < 5; n++ {
			go func(number int, blobStore BlobStore) {
				err := writeANumberWithRetry(number, blobStore)
				if err != nil {
					wg.Done()
					t.Errorf("writeANumberWithRetry() err == %s", err)
				}
				wg.Done()
			}(i*5+n+1, blobStore)
		}
		wg.Wait()
	}

	client, err := blobStore.NewClient(context.Background())
	assert.NoError(t, err)
	assert.NotEqual(t, client, nil)
	defer client.Close()
	object, err := client.NewObject("test.txt")
	assert.NoError(t, err)
	assert.NotEqual(t, object, nil)
	data, err := object.Read()
	assert.NoError(t, err)
	sliceData := strings.Split(string(data), "\n")
	assert.Equal(t, len(sliceData), 5*5)
	for i := 0; i < 5*5; i++ {
		expected := fmt.Sprintf("%05d", i+1)
		assert.Equal(t, sliceData[i], expected)
	}
}

func TestFSGetObjects(t *testing.T) {
	storePath, _ := os.MkdirTemp("", "test")
	blobStore, err := NewFSBlobStore(storePath, false)
	assert.NoError(t, err)
	assert.NotEqual(t, blobStore, nil)

	client, err := blobStore.NewClient(context.Background())
	assert.NoError(t, err)
	assert.NotEqual(t, client, nil)
	defer client.Close()
	files := []string{"first.txt", "second.txt", "third.txt", "fourth.txt", "nested/first_nested.txt", "nested/second_nested.txt"}
	for _, name := range files {
		object, err := client.NewObject(name)
		if err != nil {
			t.Errorf("client.NewObject() err == %s", err)
		}
		object.Write([]byte(name))
	}

	blobs, err := client.GetObjects("")
	assert.NoError(t, err)
	assert.Equal(t, len(blobs), len(files))

	nestedBlobs, err := client.GetObjects("nest")
	assert.NoError(t, err)
	assert.Equal(t, len(nestedBlobs), 2)
}
