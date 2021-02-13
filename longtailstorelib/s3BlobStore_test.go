package longtailstorelib

import (
	"net/url"
	"testing"

	"golang.org/x/net/context"
)

func TestS3BlobStore(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	//	t.Skip()

	u, err := url.Parse("s3://longtail-test/test-storage/store")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}
	blobStore, err := NewS3BlobStore(u)
	if err != nil {
		t.Errorf("NewS3BlobStore() err == %q", err)
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		t.Errorf("blobStore.NewClient() err == %q", err)
	}
	defer client.Close()
	object, err := client.NewObject("test.txt")
	if err != nil {
		t.Errorf("client.NewObject() err == %q", err)
	}
	testData := []byte("apa")
	ok, err := object.Write(testData)
	if !ok {
		t.Errorf("object.Write() ok != true")
	}
	if err != nil {
		t.Errorf("object.Write() err == %q", err)
	}
	blobs, err := client.GetObjects("")
	if err != nil {
		t.Errorf("client.GetObjects(\"\") err == %q", err)
	}
	if blobs[0].Name != "test.txt" {
		t.Errorf("blobs[0].Name %s != %s", blobs[0].Name, "test.txt")
	}
	data, err := object.Read()
	if len(data) != 3 {
		t.Errorf("len(data) %d != %d", len(data), 3)
	}
	for i, d := range data {
		if d != testData[i] {
			t.Errorf("%d != testData[%d]", int(d), int(testData[i]))
		}
	}
}

func TestS3BlobStoreVersioning(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	t.Skip()
	/*
	   	u, err := url.Parse("s3://longtail-test/test-storage/store")
	   	if err != nil {
	   		t.Errorf("url.Parse() err == %q", err)
	   	}
	   	blobStore, err := NewS3BlobStore(u)
	   	if err != nil {
	   		t.Errorf("NewS3BlobStore() err == %q", err)
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

	   func s3WriteANumberWithRetry(number int, blobStore BlobStore) error {
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
	   			time.Sleep(300 * time.Millisecond)
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

	   func TestS3BlobStoreVersioningStressTest(t *testing.T) {
	   	// This test uses hardcoded paths in S3 and is disabled
	   	t.Skip()

	   	u, err := url.Parse("s3://longtail-test/test-storage/store")
	   	if err != nil {
	   		t.Errorf("url.Parse() err == %q", err)
	   	}
	   	blobStore, err := NewS3BlobStore(u)
	   	if err != nil {
	   		t.Errorf("NewS3BlobStore() err == %q", err)
	   	}

	   	var wg sync.WaitGroup

	   	for i := 0; i < 10; i++ {
	   		wg.Add(20)
	   		for n := 0; n < 20; n++ {
	   			go func(number int, blobStore BlobStore) {
	   				err := s3WriteANumberWithRetry(number, blobStore)
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
	*/
}
