package remotestore

import (
	"context"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
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

func generateStoreIndex(t *testing.T, blockCount uint8, seed uint8) (longtaillib.Longtail_StoreIndex, error) {
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
	storeIndex, _ := generateStoreIndex(t, 1, uint8(77))
	defer storeIndex.Dispose()

	remoteStore, _ := longtailstorelib.NewMemBlobStore("remote", true)
	remoteClient, _ := remoteStore.NewClient(context.Background())

	localBlobStore, _ := longtailstorelib.NewMemBlobStore("local", true)
	localClient, _ := localBlobStore.NewClient(context.Background())

	err := PutStoreLSI(context.Background(), remoteStore, storeIndex, 0)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}

	remoteStoreIndex, err := GetStoreLSI(context.Background(), remoteStore, &localBlobStore)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndex.Dispose()

	LocalNames := getLSIs(localClient)
	if len(LocalNames) != 1 {
		t.Errorf("TestCleanPut() len(LocalNames) == %d, expected 1) %s", len(LocalNames), err)
	}
	remoteStoreIndexCached, err := GetStoreLSI(context.Background(), remoteStore, &localBlobStore)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndexCached.Dispose()

	err = PutStoreLSI(context.Background(), remoteStore, storeIndex, 0)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}
	RemoteNames1 := getLSIs(remoteClient)
	if len(RemoteNames1) != 1 {
		t.Errorf("TestCleanPut() len(RemoteNames) == %d, expected 1) %s", len(RemoteNames1), err)
	}

	storeIndex2, err := generateStoreIndex(t, 4, uint8(33))
	if err != nil {
		t.Errorf("TestCleanPut() generateStoreIndex(t, 1, uint8(33)) %s", err)
		return
	}
	defer storeIndex2.Dispose()

	err = PutStoreLSI(context.Background(), remoteStore, storeIndex2, 0)
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

	remoteStoreIndexMerged, err := GetStoreLSI(context.Background(), remoteStore, &localBlobStore)
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

	err = longtailutils.DeleteBlob(context.Background(), remoteClient, RemoteNames1[0])
	if err != nil {
		t.Errorf("TestCleanPut() obj.Delete()) %s", err)
	}

	remoteStoreIndexPruned, err := GetStoreLSI(context.Background(), remoteStore, &localBlobStore)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndexPruned.Dispose()
	if remoteStoreIndexPruned.GetBlockCount() != 4 {
		t.Errorf("TestCleanPut() remoteStoreIndexMerged.GetBlockCount() == %d, expected 1) %s", remoteStoreIndexPruned.GetBlockCount(), err)
		return
	}
}

func TestdMergeAtPut(t *testing.T) {
	remoteStore, _ := longtailstorelib.NewMemBlobStore("remote", true)
	remoteClient, _ := remoteStore.NewClient(context.Background())

	localBlobStore, _ := longtailstorelib.NewMemBlobStore("local", true)

	storeIndex1, _ := generateStoreIndex(t, 1, uint8(77))
	defer storeIndex1.Dispose()

	err := PutStoreLSI(context.Background(), remoteStore, storeIndex1, 0)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}

	storeIndex2, _ := generateStoreIndex(t, 3, uint8(33))
	defer storeIndex2.Dispose()

	err = PutStoreLSI(context.Background(), remoteStore, storeIndex2, 0)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}

	storeIndex3, _ := generateStoreIndex(t, 2, uint8(66))
	defer storeIndex3.Dispose()

	err = PutStoreLSI(context.Background(), remoteStore, storeIndex3, 1024*1024*1024)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}

	lsis := getLSIs(remoteClient)
	if len(lsis) != 2 {
		t.Errorf("TestForcedMerge() len(lsis) == %d, expected 2) %s", len(lsis), err)
	}

	resultStoreIndex, err := GetStoreLSI(context.Background(), remoteStore, &localBlobStore)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer resultStoreIndex.Dispose()
	if resultStoreIndex.GetBlockCount() != 6 {
		t.Errorf("TestCleanPut() resultStoreIndex.GetBlockCount() == %d, expected 6)", resultStoreIndex.GetBlockCount())
		return
	}

	noCacheStoreIndex, err := GetStoreLSI(context.Background(), remoteStore, nil)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer noCacheStoreIndex.Dispose()
	if noCacheStoreIndex.GetBlockCount() != 6 {
		t.Errorf("TestCleanPut() noCacheStoreIndex.GetBlockCount() == %d, expected 6)", noCacheStoreIndex.GetBlockCount())
		return
	}
}
