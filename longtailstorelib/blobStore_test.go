package longtailstorelib

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
)

func TestCreateStoreAndClient(t *testing.T) {
	blobStore, err := NewMemBlobStore("the_path", true)
	if err != nil {
		t.Errorf("TestCreateStoreAndClient() NewMemBlobStore() %s", err)
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		t.Errorf("TestCreateStoreAndClient() blobStore.NewClient(context.Background()) %s", err)
	}
	defer client.Close()
}

func TestListObjectsInEmptyStore(t *testing.T) {
	blobStore, _ := NewMemBlobStore("the_path", true)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	objects, err := client.GetObjects("")
	if err != nil {
		t.Errorf("TestListObjectsInEmptyStore() client.GetObjects(\"\")) %s", err)
	}
	if len(objects) != 0 {
		t.Errorf("TestListObjectsInEmptyStore() client.GetObjects(\"\")) %d != %d", len(objects), 0)
	}
	obj, _ := client.NewObject("should-not-exist")
	data, err := obj.Read()
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("TestListObjectsInEmptyStore() obj.Read()) %s", err)
	}
	if data != nil {
		t.Errorf("TestListObjectsInEmptyStore() obj.Read()) %v != %v", nil, data)
	}
}

func TestSingleObjectStore(t *testing.T) {
	blobStore, _ := NewMemBlobStore("the_path", true)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	obj, err := client.NewObject("my-fine-object.txt")
	if err != nil {
		t.Errorf("TestSingleObjectStore() client.NewObject(\"my-fine-object.txt\")) %s", err)
	}
	if exists, _ := obj.Exists(); exists {
		t.Errorf("TestSingleObjectStore() obj.Exists()) %t != %t", exists, false)
	}
	testContent := "the content of the object"
	ok, err := obj.Write([]byte(testContent))
	if !ok {
		t.Errorf("TestSingleObjectStore() obj.Write([]byte(testContent)) %t != %t", ok, true)
	}
	if err != nil {
		t.Errorf("TestSingleObjectStore() obj.Write([]byte(testContent)) %s", err)
	}
	data, err := obj.Read()
	if err != nil {
		t.Errorf("TestSingleObjectStore() obj.Read()) %s", err)
	}
	dataString := string(data)
	if dataString != testContent {
		t.Errorf("TestSingleObjectStore() string(data)) %s != %s", dataString, testContent)
	}
	err = obj.Delete()
	if err != nil {
		t.Errorf("TestSingleObjectStore() obj.Delete()) %s", err)
	}
}

func TestDeleteObject(t *testing.T) {
	blobStore, _ := NewMemBlobStore("the_path", true)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	obj, _ := client.NewObject("my-fine-object.txt")
	testContent := "the content of the object"
	_, _ = obj.Write([]byte(testContent))
	obj.Delete()
	if exists, _ := obj.Exists(); exists {
		t.Errorf("TestSingleObjectStore() obj.Exists()) %t != %t", exists, false)
	}
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
	if err != nil {
		t.Errorf("TestListObjects() client.GetObjects(\"\")) %s", err)
	}
	if len(objects) != 3 {
		t.Errorf("TestListObjects() client.GetObjects(\"\")) %d != %d", len(objects), 3)
	}
	for _, o := range objects {
		readObj, err := client.NewObject(o.Name)
		if err != nil {
			t.Errorf("TestListObjects() o.client.NewObject(o.Name)) %d != %d", len(objects), 3)
		}
		if readObj == nil {
			t.Errorf("TestListObjects() o.client.NewObject(o.Name)) %v == %v", readObj, nil)
		}
		data, err := readObj.Read()
		if err != nil {
			t.Errorf("TestListObjects() readObj.Read()) %s", err)
		}
		stringData := string(data)
		if stringData != o.Name {
			t.Errorf("TestListObjects() string(data) != o.Name) %s != %s", stringData, o.Name)
		}
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
	if exists {
		t.Errorf("TestGenerationWrite() obj.LockWriteVersion()) %t != %t", exists, false)
	}
	if err != nil {
		t.Errorf("TestGenerationWrite() obj.LockWriteVersion()) %s", err)
	}
	ok, err := obj.Write([]byte(testContent1))
	if !ok {
		t.Errorf("TestGenerationWrite() obj.Write([]byte(testContent1)) %t != %t", ok, true)
	}
	if err != nil {
		t.Errorf("TestGenerationWrite() obj.Write([]byte(testContent1)) %s", err)
	}
	ok, err = obj.Write([]byte(testContent2))
	if ok {
		t.Errorf("TestGenerationWrite() obj.Write([]byte(testContent2))) %t != %t", ok, false)
	}
	if err != nil {
		t.Errorf("TestGenerationWrite() obj.Write([]byte(testContent2))) %s", err)
	}
	obj2, _ := client.NewObject("my-fine-object.txt")
	exists, err = obj.LockWriteVersion()
	if !exists {
		t.Errorf("TestGenerationWrite() obj.LockWriteVersion()) %t != %t", exists, true)
	}
	if err != nil {
		t.Errorf("TestGenerationWrite() obj.LockWriteVersion()) %s", err)
	}
	exists, err = obj2.LockWriteVersion()
	if !exists {
		t.Errorf("TestGenerationWrite() obj2.LockWriteVersion()) %t != %t", exists, true)
	}
	if err != nil {
		t.Errorf("TestGenerationWrite() obj2.LockWriteVersion()) %s", err)
	}
	ok, err = obj.Write([]byte(testContent2))
	if !ok {
		t.Errorf("TestGenerationWrite() obj.Write([]byte(testContent2))) %t != %t", ok, true)
	}
	if err != nil {
		t.Errorf("TestGenerationWrite() obj.Write([]byte(testContent2))) %s", err)
	}
	ok, err = obj2.Write([]byte(testContent3))
	if ok {
		t.Errorf("TestGenerationWrite() obj2.Write([]byte(testContent3))) %t != %t", ok, false)
	}
	if err != nil {
		t.Errorf("TestGenerationWrite() obj2.Write([]byte(testContent3))) %s", err)
	}
	err = obj.Delete()
	if err == nil {
		t.Errorf("TestGenerationWrite() obj.Delete()) %s", err)
	}
	obj.LockWriteVersion()
	err = obj.Delete()
	if err != nil {
		t.Errorf("TestGenerationWrite() obj.Delete()) %s", err)
	}
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
	if err != nil {
		t.Errorf("TestCreateStores() CreateBlobStoreForURI()) %s", err)
	}
	name := blobStore.String()
	if !strings.Contains(name, "my-blob-store") {
		t.Errorf("TestCreateStores() blobStore.String()) %s", name)
	}
}

func TestCreateGCSBlobStoreFromURI(t *testing.T) {
	blobStore, err := CreateBlobStoreForURI("gs://my-blob-store")
	if err != nil {
		t.Errorf("TestCreateStores() CreateBlobStoreForURI()) %s", err)
	}
	name := blobStore.String()
	if !strings.Contains(name, "my-blob-store") {
		t.Errorf("TestCreateStores() blobStore.String()) %s", name)
	}
}

func TestCreateS3BlobStoreFromURI(t *testing.T) {
	blobStore, err := CreateBlobStoreForURI("s3://my-blob-store")
	if err != nil {
		t.Errorf("TestCreateStores() CreateBlobStoreForURI()) %s", err)
	}
	name := blobStore.String()
	if !strings.Contains(name, "my-blob-store") {
		t.Errorf("TestCreateStores() blobStore.String()) %s", name)
	}
}

func TestCreateFileBlobStoreFromURI(t *testing.T) {
	blobStore, err := CreateBlobStoreForURI("file://my-blob-store")
	if err != nil {
		t.Errorf("TestCreateStores() CreateBlobStoreForURI()) %s", err)
	}
	name := blobStore.String()
	if !strings.Contains(name, "my-blob-store") {
		t.Errorf("TestCreateStores() blobStore.String()) %s", name)
	}
}

func TestCreateFileBlobStoreFromPath(t *testing.T) {
	blobStore, err := CreateBlobStoreForURI("c:\\temp\\my-blob-store")
	if err != nil {
		t.Errorf("TestCreateStores() CreateBlobStoreForURI()) %s", err)
	}
	name := blobStore.String()
	if !strings.Contains(name, "my-blob-store") {
		t.Errorf("TestCreateStores() blobStore.String()) %s", name)
	}
}

func TestCreateAzureGen1BlobStoreFromPath(t *testing.T) {
	_, err := CreateBlobStoreForURI("abfs://my-blob-store")
	if err == nil {
		t.Errorf("TestCreateStores() CreateBlobStoreForURI()) %s", "should fail")
	}
}

func TestCreateAzureGen2BlobStoreFromPath(t *testing.T) {
	_, err := CreateBlobStoreForURI("abfss://my-blob-store")
	if err == nil {
		t.Errorf("TestCreateStores() CreateBlobStoreForURI()) %s", "should fail")
	}
}
