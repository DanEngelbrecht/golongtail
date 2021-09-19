package longtailstorelib

import (
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestGCSStoreIndexSyncWithLocking(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-gcs-blob-store-sync")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}

	blobStore, err := NewGCSBlobStore(u, false)
	if err != nil {
		log.Fatalf("%v", err)
	}
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	object, _ := client.NewObject("store.lsi")
	object.Delete()

	testStoreIndexSync(blobStore, t)
}

func TestGCSStoreIndexSyncWithoutLocking(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-gcs-blob-store-sync")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}

	blobStore, err := NewGCSBlobStore(u, true)
	if err != nil {
		log.Fatalf("%v", err)
	}
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	object, _ := client.NewObject("store.lsi")
	object.Delete()

	testStoreIndexSync(blobStore, t)
}
