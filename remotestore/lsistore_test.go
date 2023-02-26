package remotestore

import (
	"context"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
)

func getLSIs(client longtailstorelib.BlobClient) []string {
	items, err := client.GetObjects("store")
	if err != nil {
		return nil
	}
	names := make([]string, len(items))
	for i := 0; i < len(items); i++ {
		names[i] = items[i].Name
	}
	return names
}

func GenerateStoreIndex(t *testing.T, blockCount uint8, seed uint8) (longtaillib.Longtail_StoreIndex, error) {
	blocks := make([]longtaillib.Longtail_StoredBlock, 0)
	defer func(blocks []longtaillib.Longtail_StoredBlock) {
		for _, b := range blocks {
			b.Dispose()
		}
	}(blocks)
	blockIndexes := make([]longtaillib.Longtail_BlockIndex, 0)

	for block := uint8(0); block < blockCount; block++ {
		storedBlock, err := generateStoredBlock(t, seed+block)
		if err != nil {
			t.Errorf("TestCleanPut() generateStoredBlock(t, 77)) %s", err)
			return longtaillib.Longtail_StoreIndex{}, err
		}
		blocks = append(blocks, storedBlock)
		blockIndexes = append(blockIndexes, storedBlock.GetBlockIndex())
	}

	storeIndex, err := longtaillib.CreateStoreIndexFromBlocks(blockIndexes)
	if err != nil {
		t.Errorf("TestCleanPut() CreateStoreIndexFromBlocks()) %s", err)
		return longtaillib.Longtail_StoreIndex{}, err
	}
	return storeIndex, nil
}

func TestPutGet(t *testing.T) {
	storeIndex, err := GenerateStoreIndex(t, 1, uint8(77))
	if err != nil {
		t.Errorf("TestCleanPut() GenerateStoreIndex(t, 1, uint8(77)) %s", err)
		return
	}
	defer storeIndex.Dispose()

	remoteBlobStore, err := longtailstorelib.NewMemBlobStore("remote", true)
	if err != nil {
		t.Errorf("TestCleanPut() longtailstorelib.NewMemBlobStore(\"remote\", true)) %s", err)
		return
	}
	remoteClient, _ := remoteBlobStore.NewClient(context.Background())

	localBlobStore, err := longtailstorelib.NewMemBlobStore("local", true)
	if err != nil {
		t.Errorf("TestCleanPut() longtailstorelib.NewMemBlobStore(\"local\", true)) %s", err)
		return
	}
	localClient, _ := localBlobStore.NewClient(context.Background())

	err = PutStoreLSI(context.Background(), remoteClient, storeIndex)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}

	remoteStoreIndex, err := GetStoreLSI(context.Background(), remoteClient, localClient)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndex.Dispose()

	LocalNames := getLSIs(localClient)
	if len(LocalNames) != 1 {
		t.Errorf("TestCleanPut() len(LocalNames) == %d, expected 1) %s", len(LocalNames), err)
	}
	remoteStoreIndexCached, err := GetStoreLSI(context.Background(), remoteClient, localClient)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndexCached.Dispose()

	err = PutStoreLSI(context.Background(), remoteClient, storeIndex)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}
	RemoteNames1 := getLSIs(remoteClient)
	if len(RemoteNames1) != 1 {
		t.Errorf("TestCleanPut() len(RemoteNames) == %d, expected 1) %s", len(RemoteNames1), err)
	}

	storeIndex2, err := GenerateStoreIndex(t, 4, uint8(33))
	if err != nil {
		t.Errorf("TestCleanPut() GenerateStoreIndex(t, 1, uint8(33)) %s", err)
		return
	}
	defer storeIndex2.Dispose()

	err = PutStoreLSI(context.Background(), remoteClient, storeIndex2)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}

	RemoteNames2 := getLSIs(remoteClient)
	if len(RemoteNames2) != 2 {
		t.Errorf("TestCleanPut() len(RemoteNames) == %d, expected 2) %s", len(RemoteNames2), err)
	}

	LocalNames = getLSIs(localClient)
	if len(LocalNames) != 1 {
		t.Errorf("TestCleanPut() len(LocalNames) == %d, expected 1) %s", len(LocalNames), err)
	}

	remoteStoreIndexMerged, err := GetStoreLSI(context.Background(), remoteClient, localClient)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndexMerged.Dispose()
	if remoteStoreIndexMerged.GetBlockCount() != 5 {
		t.Errorf("TestCleanPut() remoteStoreIndexMerged.GetBlockCount() == %d, expected 5) %s", remoteStoreIndexMerged.GetBlockCount(), err)
		return
	}

	LocalNames = getLSIs(localClient)
	if len(LocalNames) != 2 {
		t.Errorf("TestCleanPut() len(LocalNames) == %d, expected 2) %s", len(LocalNames), err)
	}

	obj, err := remoteClient.NewObject(RemoteNames1[0])
	if err != nil {
		t.Errorf("TestCleanPut() remoteClient.NewObject(\"%s\") %s", RemoteNames1[0], err)
	}
	err = obj.Delete()
	if err != nil {
		t.Errorf("TestCleanPut() obj.Delete()) %s", err)
	}

	remoteStoreIndexPruned, err := GetStoreLSI(context.Background(), remoteClient, localClient)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndexPruned.Dispose()
	if remoteStoreIndexPruned.GetBlockCount() != 4 {
		t.Errorf("TestCleanPut() remoteStoreIndexMerged.GetBlockCount() == %d, expected 1) %s", remoteStoreIndexPruned.GetBlockCount(), err)
		return
	}
	//	remoteStoreNoCache, err := GetStoreLSI(context.Background(), remoteClient, nil) //localClient)
	//	if err != nil {
	//		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
	//		return
	//	}
	//	defer remoteStoreNoCache.Dispose()
}
