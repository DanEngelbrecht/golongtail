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
		t.Errorf("TestCreateStoreAndClient() NewMemBlobStore() %v != %v", err, nil)
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		t.Errorf("TestCreateStoreAndClient() blobStore.NewClient(context.Background()) %v != %v", err, nil)
	}
	defer client.Close()
}

func TestListObjectsInEmptyStore(t *testing.T) {
	blobStore, _ := NewMemBlobStore("the_path", true)
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	objects, err := client.GetObjects("")
	if err != nil {
		t.Errorf("TestListObjectsInEmptyStore() client.GetObjects(\"\")) %v != %v", err, nil)
	}
	if len(objects) != 0 {
		t.Errorf("TestListObjectsInEmptyStore() client.GetObjects(\"\")) %d != %d", len(objects), 0)
	}
	obj, _ := client.NewObject("should-not-exist")
	data, err := obj.Read()
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("TestListObjectsInEmptyStore() obj.Read()) %v != %v", true, errors.Is(err, os.ErrNotExist))
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
		t.Errorf("TestSingleObjectStore() client.NewObject(\"my-fine-object.txt\")) %v != %v", err, nil)
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
		t.Errorf("TestSingleObjectStore() obj.Write([]byte(testContent)) %v != %v", err, nil)
	}
	data, err := obj.Read()
	if err != nil {
		t.Errorf("TestSingleObjectStore() obj.Read()) %v != %v", err, nil)
	}
	dataString := string(data)
	if dataString != testContent {
		t.Errorf("TestSingleObjectStore() string(data)) %s != %s", dataString, testContent)
	}
	err = obj.Delete()
	if err != nil {
		t.Errorf("TestSingleObjectStore() obj.Delete()) %v != %v", err, nil)
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
		t.Errorf("TestListObjects() client.GetObjects(\"\")) %v != %v", err, nil)
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
			t.Errorf("TestListObjects() readObj.Read()) %v != %v", err, nil)
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
		t.Errorf("TestGenerationWrite() obj.LockWriteVersion()) %v != %v", err, nil)
	}
	ok, err := obj.Write([]byte(testContent1))
	if !ok {
		t.Errorf("TestGenerationWrite() obj.Write([]byte(testContent1)) %t != %t", ok, true)
	}
	if err != nil {
		t.Errorf("TestGenerationWrite() obj.Write([]byte(testContent1)) %v != %v", err, nil)
	}
	ok, err = obj.Write([]byte(testContent2))
	if ok {
		t.Errorf("TestGenerationWrite() obj.Write([]byte(testContent2))) %t != %t", ok, false)
	}
	if err != nil {
		t.Errorf("TestGenerationWrite() obj.Write([]byte(testContent2))) %v != %v", err, nil)
	}
	obj2, _ := client.NewObject("my-fine-object.txt")
	exists, err = obj.LockWriteVersion()
	if !exists {
		t.Errorf("TestGenerationWrite() obj.LockWriteVersion()) %t != %t", exists, true)
	}
	if err != nil {
		t.Errorf("TestGenerationWrite() obj.LockWriteVersion()) %v != %v", err, nil)
	}
	exists, err = obj2.LockWriteVersion()
	if !exists {
		t.Errorf("TestGenerationWrite() obj2.LockWriteVersion()) %t != %t", exists, true)
	}
	if err != nil {
		t.Errorf("TestGenerationWrite() obj2.LockWriteVersion()) %v != %v", err, nil)
	}
	ok, err = obj.Write([]byte(testContent2))
	if !ok {
		t.Errorf("TestGenerationWrite() obj.Write([]byte(testContent2))) %t != %t", ok, true)
	}
	if err != nil {
		t.Errorf("TestGenerationWrite() obj.Write([]byte(testContent2))) %v != %v", err, nil)
	}
	ok, err = obj2.Write([]byte(testContent3))
	if ok {
		t.Errorf("TestGenerationWrite() obj2.Write([]byte(testContent3))) %t != %t", ok, false)
	}
	if err != nil {
		t.Errorf("TestGenerationWrite() obj2.Write([]byte(testContent3))) %v != %v", err, nil)
	}
	err = obj.Delete()
	if err == nil {
		t.Errorf("TestGenerationWrite() obj.Delete()) %v == %v", err, nil)
	}
	obj.LockWriteVersion()
	err = obj.Delete()
	if err != nil {
		t.Errorf("TestGenerationWrite() obj.Delete()) %v != %v", err, nil)
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
			log.Printf("Wrote %d\n", number)
			return nil
		}
		log.Printf("Retrying %d\n", number)
	}
}
