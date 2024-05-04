package longtailstorelib

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/alecthomas/assert/v2"
)

func TestCreateStoreAndClient(t *testing.T) {
	blobStore, err := NewMemBlobStore("the_path", true)
	assert.NoError(t, err, "NewMemBlobStore()")
	client, err := blobStore.NewClient(context.Background())
	assert.NoError(t, err, "blobStore.NewClient(context.Background())")
	defer client.Close()
}

func TestListObjectsInEmptyStore(t *testing.T) {
	blobStore, _ := NewMemBlobStore("the_path", true)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	objects, err := client.GetObjects("")
	assert.NoError(t, err, "client.GetObjects(\"\"))")
	assert.Equal(t, len(objects), 0)
	obj, _ := client.NewObject("should-not-exist")
	data, err := obj.Read()
	assert.True(t, longtaillib.IsNotExist(err))
	assert.Equal(t, data, nil)
}

func TestSingleObjectStore(t *testing.T) {
	blobStore, _ := NewMemBlobStore("the_path", true)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	obj, err := client.NewObject("my-fine-object.txt")
	assert.NoError(t, err, "client.NewObject(\"my-fine-object.txt\")")
	exists, _ := obj.Exists()
	assert.False(t, exists, "obj.Exists()")
	testContent := "the content of the object"
	ok, err := obj.Write([]byte(testContent))
	assert.NoError(t, err, "obj.Write([]byte(testContent))")
	assert.True(t, ok, "obj.Write([]byte(testContent))")
	assert.NoError(t, err, "obj.Write([]byte(testContent)")
	data, err := obj.Read()
	assert.NoError(t, err, "obj.Read()")
	dataString := string(data)
	assert.Equal(t, dataString, testContent)
	err = obj.Delete()
	assert.NoError(t, err, "obj.Delete()")
}

func TestDeleteObject(t *testing.T) {
	blobStore, _ := NewMemBlobStore("the_path", true)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	obj, _ := client.NewObject("my-fine-object.txt")
	testContent := "the content of the object"
	_, _ = obj.Write([]byte(testContent))
	obj.Delete()
	exists, err := obj.Exists()
	assert.NoError(t, err, "obj.Exists()")
	assert.False(t, exists, "obj.Exists()")
}

func TestListObjects(t *testing.T) {
	blobStore, _ := NewMemBlobStore("the_path", true)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	obj, _ := client.NewObject("my-fine-object1.txt")
	obj.Write([]byte("my-fine-object1.txt"))
	obj, _ = client.NewObject("my-fine-object2.txt")
	obj.Write([]byte("my-fine-object2.txt"))
	obj, _ = client.NewObject("my-fine-object3.txt")
	obj.Write([]byte("my-fine-object3.txt"))
	objects, err := client.GetObjects("")
	assert.NoError(t, err, "TestListObjects() client.GetObjects(\"\")")
	assert.Equal(t, len(objects), 3)
	for _, o := range objects {
		readObj, err := client.NewObject(o.Name)
		assert.NoError(t, err)
		assert.NotEqual(t, readObj, nil)
		data, err := readObj.Read()
		assert.NoError(t, err, nil)
		stringData := string(data)
		assert.Equal(t, stringData, o.Name)
	}
}

func TestGenerationWrite(t *testing.T) {
	blobStore, _ := NewMemBlobStore("the_path", true)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	obj, _ := client.NewObject("my-fine-object.txt")
	testContent1 := "the content of the object1"
	testContent2 := "the content of the object2"
	testContent3 := "the content of the object3"
	exists, err := obj.LockWriteVersion()
	assert.False(t, exists)
	assert.NoError(t, err, "obj.LockWriteVersion()")
	ok, err := obj.Write([]byte(testContent1))
	assert.True(t, ok)
	assert.True(t, ok)
	assert.NoError(t, err, "obj.Write([]byte(testContent1)")
	ok, err = obj.Write([]byte(testContent2))
	assert.False(t, ok)
	assert.NoError(t, err, "obj.Write([]byte(testContent2))")
	obj2, _ := client.NewObject("my-fine-object.txt")
	exists, err = obj.LockWriteVersion()
	assert.True(t, exists)
	assert.NoError(t, err, "obj.LockWriteVersion()")
	exists, err = obj2.LockWriteVersion()
	assert.True(t, exists)
	assert.NoError(t, err, "obj2.LockWriteVersion()")
	ok, err = obj.Write([]byte(testContent2))
	assert.True(t, ok)
	assert.NoError(t, err, "obj.Write([]byte(testContent2))")
	ok, err = obj2.Write([]byte(testContent3))
	assert.False(t, ok)
	assert.NoError(t, err, "obj2.Write([]byte(testContent3))")
	err = obj.Delete()
	assert.Error(t, err)
	obj.LockWriteVersion()
	err = obj.Delete()
	assert.NoError(t, err, "obj.Delete()")
}

func writeANumberWithRetry(number int, blobStore BlobStore) error {
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		return err
	}
	defer client.Close()
	object, err := client.NewObject("test.txt")
	if err != nil {
		return err
	}
	retries := 0
	for {
		exists, err := object.LockWriteVersion()
		if err != nil {
			return err
		}
		var sliceData []string
		if exists {
			data, err := object.Read()
			if err != nil {
				return err
			}
			time.Sleep(30 * time.Millisecond)
			sliceData = strings.Split(string(data), "\n")
		}
		sliceData = append(sliceData, fmt.Sprintf("%05d", number))
		sort.Strings(sliceData)
		newData := strings.Join(sliceData, "\n")

		ok, err := object.Write([]byte(newData))
		if err != nil {
			return err
		}
		if ok {
			if retries > 0 {
				log.Printf("Wrote %d (after %d retries)\n", number, retries)
			}
			return nil
		}
		retries++
		log.Printf("Retrying %d (failed %d times)\n", number, retries)
	}
}

func TestCreateFSBlobStoreFromURI(t *testing.T) {
	blobStore, err := CreateBlobStoreForURI("fsblob://my-blob-store")
	assert.NoError(t, err, "TestCreateStores() CreateBlobStoreForURI()")
	name := blobStore.String()
	assert.True(t, strings.Contains(name, "my-blob-store"))
}

func TestCreateGCSBlobStoreFromURI(t *testing.T) {
	blobStore, err := CreateBlobStoreForURI("gs://my-blob-store")
	assert.NoError(t, err, "TestCreateStores() CreateBlobStoreForURI()")
	name := blobStore.String()
	assert.True(t, strings.Contains(name, "my-blob-store"))
}

func TestCreateS3BlobStoreFromURI(t *testing.T) {
	blobStore, err := CreateBlobStoreForURI("s3://my-blob-store")
	assert.NoError(t, err, "TestCreateStores() CreateBlobStoreForURI()")
	name := blobStore.String()
	assert.True(t, strings.Contains(name, "my-blob-store"))
}

func TestCreateFileBlobStoreFromURI(t *testing.T) {
	blobStore, err := CreateBlobStoreForURI("file://my-blob-store")
	assert.NoError(t, err, "TestCreateStores() CreateBlobStoreForURI()")
	name := blobStore.String()
	assert.True(t, strings.Contains(name, "my-blob-store"))
}

func TestCreateFileBlobStoreFromPath(t *testing.T) {
	blobStore, err := CreateBlobStoreForURI("c:\\temp\\my-blob-store")
	assert.NoError(t, err, "TestCreateStores() CreateBlobStoreForURI()")
	name := blobStore.String()
	assert.True(t, strings.Contains(name, "my-blob-store"))
}

func TestCreateAzureGen1BlobStoreFromPath(t *testing.T) {
	_, err := CreateBlobStoreForURI("abfs://my-blob-store")
	assert.Error(t, err)
}

func TestCreateAzureGen2BlobStoreFromPath(t *testing.T) {
	_, err := CreateBlobStoreForURI("abfss://my-blob-store")
	assert.Error(t, err)
}
