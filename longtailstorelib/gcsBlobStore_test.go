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

	"github.com/DanEngelbrecht/golongtail/longtaillib"
)

func TestGCSBlobStore(t *testing.T) {
	// This test uses hardcoded paths in gcs and is disabled
	t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-storage/store")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}
	blobStore, err := NewGCSBlobStore(u)
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
	blobStore, err := NewGCSBlobStore(u)
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
	blobStore, err := NewGCSBlobStore(u)
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

func TestGCSStoreIndexSync(t *testing.T) {
	// This test uses hardcoded paths in S3 and is disabled
	t.Skip()

	u, err := url.Parse("gs://longtail-test-de/test-gcs-blob-store-sync")
	if err != nil {
		t.Errorf("url.Parse() err == %q", err)
	}

	blobStore, err := NewGCSBlobStore(u)
	if err != nil {
		log.Fatalf("%v", err)
	}

	blockGenerateCount := 4
	workerCount := 21

	generatedBlockHashes := make(chan uint64, blockGenerateCount*workerCount)

	var wg sync.WaitGroup
	for n := 0; n < workerCount; n++ {
		wg.Add(1)
		seedBase := blockGenerateCount * n
		go func(blockGenerateCount int, seedBase int) {
			client, _ := blobStore.NewClient(context.Background())
			defer client.Close()

			{
				blocks := []longtaillib.Longtail_BlockIndex{}
				for i := 0; i < blockGenerateCount-1; i++ {
					block, _ := generateUniqueStoredBlock(t, uint8(seedBase+i))
					blocks = append(blocks, block.GetBlockIndex())
				}

				newBlocksIndex, errno := longtaillib.CreateStoreIndexFromBlocks(blocks)
				if errno != 0 {
					t.Errorf("longtaillib.CreateStoreIndexFromBlocks() failed with %q", longtaillib.ErrnoToError(errno, longtaillib.ErrEIO))
				}
				newStoreIndex, err := addToRemoteStoreIndex(context.Background(), client, newBlocksIndex)
				newBlocksIndex.Dispose()
				if err != nil {
					t.Errorf("addToRemoteStoreIndex() failed with %q", err)
				}
				newStoreIndex.Dispose()
			}

			readStoreIndex, err := readStoreStoreIndex(context.Background(), client)
			if err != nil {
				t.Errorf("readStoreStoreIndex() failed with %q", err)
			}
			readStoreIndex.Dispose()

			{
				blocks := []longtaillib.Longtail_BlockIndex{}
				for i := blockGenerateCount - 1; i < blockGenerateCount; i++ {
					block, _ := generateUniqueStoredBlock(t, uint8(seedBase+i))
					blocks = append(blocks, block.GetBlockIndex())
				}
				newBlocksIndex, errno := longtaillib.CreateStoreIndexFromBlocks(blocks)
				if errno != 0 {
					t.Errorf("longtaillib.CreateStoreIndexFromBlocks() failed with %q", longtaillib.ErrnoToError(errno, longtaillib.ErrEIO))
				}
				newStoreIndex, err := addToRemoteStoreIndex(context.Background(), client, newBlocksIndex)
				newBlocksIndex.Dispose()
				if err != nil {
					t.Errorf("addToRemoteStoreIndex() failed with %q", err)
				}
				newStoreIndex.Dispose()
			}

			storeIndex, err := readStoreStoreIndex(context.Background(), client)
			if err != nil {
				t.Errorf("readStoreStoreIndex() failed with %q", err)
			}
			lookup := map[uint64]bool{}
			for _, h := range storeIndex.GetBlockHashes() {
				lookup[h] = true
			}

			blockHashes := storeIndex.GetBlockHashes()
			for n := 0; n < blockGenerateCount; n++ {
				h := blockHashes[n]
				generatedBlockHashes <- h
				_, exists := lookup[h]
				if !exists {
					storeIndex.Dispose()
					t.Errorf("Missing direct block %d", h)
				}
			}
			storeIndex.Dispose()

			wg.Done()
		}(blockGenerateCount, seedBase)
	}
	wg.Wait()
	client, _ := blobStore.NewClient(context.Background())
	defer client.Close()
	storeIndex, err := readStoreStoreIndex(context.Background(), client)
	if err != nil {
		t.Errorf("readStoreStoreIndex() failed with %q", err)
	}
	defer storeIndex.Dispose()
	if len(storeIndex.GetBlockHashes()) != blockGenerateCount*workerCount {
		t.Errorf("Not all blockes were stored in index, expected %d, got %d", blockGenerateCount*workerCount, len(storeIndex.GetBlockHashes()))
	}
	lookup := map[uint64]bool{}
	for _, h := range storeIndex.GetBlockHashes() {
		lookup[h] = true
	}
	for n := 0; n < workerCount*blockGenerateCount; n++ {
		h := <-generatedBlockHashes
		_, exists := lookup[h]
		if !exists {
			t.Errorf("Missing block %d", h)
		}
	}
}
