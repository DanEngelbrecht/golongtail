package longtailstorelib

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
)

type testBlob struct {
	generation int
	path       string
	data       []byte
}

type testBlobStore struct {
	blobs      map[string]*testBlob
	blobsMutex sync.RWMutex
	prefix     string
}

type testBlobClient struct {
	store *testBlobStore
}

type testBlobObject struct {
	client           *testBlobClient
	path             string
	lockedGeneration *int
}

// NewTestBlobStore ...
func NewTestBlobStore(prefix string) (BlobStore, error) {
	s := &testBlobStore{prefix: prefix, blobs: make(map[string]*testBlob)}
	return s, nil
}

func (blobStore *testBlobStore) NewClient(ctx context.Context) (BlobClient, error) {
	return &testBlobClient{store: blobStore}, nil
}

func (blobStore *testBlobStore) String() string {
	return "teststore"
}

func (blobClient *testBlobClient) NewObject(filepath string) (BlobObject, error) {
	return &testBlobObject{client: blobClient, path: filepath}, nil
}

func (blobClient *testBlobClient) GetObjects(pathPrefix string) ([]BlobProperties, error) {
	blobClient.store.blobsMutex.RLock()
	defer blobClient.store.blobsMutex.RUnlock()
	properties := make([]BlobProperties, len(blobClient.store.blobs))
	i := 0
	for key, blob := range blobClient.store.blobs {
		if strings.HasPrefix(key, pathPrefix)) {
			properties[i] = BlobProperties{Name: key, Size: int64(len(blob.data))}
		}
		i++
	}
	return properties, nil
}

func (blobClient *testBlobClient) Close() {
}

func (blobClient *testBlobClient) String() string {
	return "teststore"
}

func (blobObject *testBlobObject) Exists() (bool, error) {
	blobObject.client.store.blobsMutex.RLock()
	defer blobObject.client.store.blobsMutex.RUnlock()
	_, exists := blobObject.client.store.blobs[blobObject.path]
	return exists, nil
}

func (blobObject *testBlobObject) Read() ([]byte, error) {
	blobObject.client.store.blobsMutex.RLock()
	defer blobObject.client.store.blobsMutex.RUnlock()
	blob, exists := blobObject.client.store.blobs[blobObject.path]
	if !exists {
		return nil, fmt.Errorf("testBlobObject object does not exist: %s", blobObject.path)
	}
	return blob.data, nil
}

func (blobObject *testBlobObject) LockWriteVersion() (bool, error) {
	blobObject.client.store.blobsMutex.RLock()
	defer blobObject.client.store.blobsMutex.RUnlock()
	blob, exists := blobObject.client.store.blobs[blobObject.path]
	blobObject.lockedGeneration = new(int)
	if !exists {
		*blobObject.lockedGeneration = -1
		return false, nil
	}
	*blobObject.lockedGeneration = blob.generation
	return true, nil
}

func (blobObject *testBlobObject) Write(data []byte) (bool, error) {
	blobObject.client.store.blobsMutex.Lock()
	defer blobObject.client.store.blobsMutex.Unlock()

	blob, exists := blobObject.client.store.blobs[blobObject.path]

	if blobObject.lockedGeneration != nil {
		if exists {
			if blob.generation != *blobObject.lockedGeneration {
				return false, nil
			}
		} else if (*blobObject.lockedGeneration) != -1 {
			return false, nil
		}
	}

	if !exists {
		blob = &testBlob{generation: 0, path: blobObject.path, data: data}
		blobObject.client.store.blobs[blobObject.path] = blob
		return true, nil
	}

	blob.data = data
	blob.generation++
	return true, nil
}

func (blobObject *testBlobObject) Delete() error {
	blobObject.client.store.blobsMutex.Lock()
	defer blobObject.client.store.blobsMutex.Unlock()

	if blobObject.lockedGeneration != nil {
		blob, exists := blobObject.client.store.blobs[blobObject.path]
		if !exists {
			return storage.ErrObjectNotExist
		}
		if blob.generation != *blobObject.lockedGeneration {
			return fmt.Errorf("testBlobObject: generation lock mismatch %s", blobObject.path)
		}
	}
	delete(blobObject.client.store.blobs, blobObject.path)
	return nil
}

func TestCreateStoreAndClient(t *testing.T) {
	blobStore, err := NewTestBlobStore("the_path")
	if err != nil {
		t.Errorf("TestCreateStoreAndClient() NewTestBlobStore() %v != %v", err, nil)
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		t.Errorf("TestCreateStoreAndClient() blobStore.NewClient(context.Background()) %v != %v", err, nil)
	}
	defer client.Close()
}

func TestListObjectsInEmptyStore(t *testing.T) {
	blobStore, _ := NewTestBlobStore("the_path")
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	objects, err := client.GetObjects("")
	if err != nil {
		t.Errorf("TestListObjectsInEmptyStore() client.GetObjects()) %v != %v", err, nil)
	}
	if len(objects) != 0 {
		t.Errorf("TestListObjectsInEmptyStore() client.GetObjects()) %d != %d", len(objects), 0)
	}
	obj, _ := client.NewObject("should-not-exist")
	data, err := obj.Read()
	if err == nil {
		t.Errorf("TestListObjectsInEmptyStore() obj.Read()) %v != %v", fmt.Errorf("testBlobObject object does not exist: should-not-exist"), err)
	}
	if data != nil {
		t.Errorf("TestListObjectsInEmptyStore() obj.Read()) %v != %v", nil, data)
	}
}

func TestSingleObjectStore(t *testing.T) {
	blobStore, _ := NewTestBlobStore("the_path")
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

func TestListObjects(t *testing.T) {
	blobStore, _ := NewTestBlobStore("the_path")
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
		t.Errorf("TestListObjects() client.GetObjects()) %v != %v", err, nil)
	}
	if len(objects) != 3 {
		t.Errorf("TestListObjects() client.GetObjects()) %d != %d", len(objects), 3)
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
	blobStore, _ := NewTestBlobStore("the_path")
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
