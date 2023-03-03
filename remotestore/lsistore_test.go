package remotestore

import (
	"context"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
)

func getLSIs(store longtailstorelib.BlobStore) []string {
	client, _ := store.NewClient(context.Background())
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
	localStore, _ := longtailstorelib.NewMemBlobStore("local", true)

	emptyStoreIndex, err := GetStoreLSI(context.Background(), remoteStore, &localStore)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer emptyStoreIndex.Dispose()
	if emptyStoreIndex.GetBlockCount() != 0 {
		t.Errorf("TestCleanPut() emptyStoreIndex.GetBlockCount() == %d, expected 0) %s", emptyStoreIndex.GetBlockCount(), err)
		return
	}

	LSI1, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex, 0)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}
	defer LSI1.Dispose()
	if LSI1.GetBlockCount() != 1 {
		t.Errorf("TestCleanPut() LSI1.GetBlockCount() == %d, expected 1) %s", LSI1.GetBlockCount(), err)
		return
	}

	remoteStoreIndex, err := GetStoreLSI(context.Background(), remoteStore, &localStore)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndex.Dispose()

	LocalNames := getLSIs(localStore)
	if len(LocalNames) != 1 {
		t.Errorf("TestCleanPut() len(LocalNames) == %d, expected 1) %s", len(LocalNames), err)
	}
	remoteStoreIndexCached, err := GetStoreLSI(context.Background(), remoteStore, &localStore)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndexCached.Dispose()

	LSI2, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex, 0)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}
	defer LSI2.Dispose()
	if LSI2.GetBlockCount() != 1 {
		t.Errorf("TestCleanPut() LSI2.GetBlockCount() == %d, expected 1) %s", LSI2.GetBlockCount(), err)
		return
	}

	RemoteNames1 := getLSIs(remoteStore)
	if len(RemoteNames1) != 1 {
		t.Errorf("TestCleanPut() len(RemoteNames) == %d, expected 1) %s", len(RemoteNames1), err)
	}

	storeIndex2, err := generateStoreIndex(t, 4, uint8(33))
	if err != nil {
		t.Errorf("TestCleanPut() generateStoreIndex(t, 1, uint8(33)) %s", err)
		return
	}
	defer storeIndex2.Dispose()

	LSI3, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex2, 0)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}
	defer LSI3.Dispose()
	if LSI3.GetBlockCount() != 5 {
		t.Errorf("TestCleanPut() LSI3.GetBlockCount() == %d, expected 5) %s", LSI3.GetBlockCount(), err)
		return
	}

	RemoteNames2 := getLSIs(remoteStore)
	if len(RemoteNames2) != 2 {
		t.Errorf("TestCleanPut() len(RemoteNames) == %d, expected 2) %s", len(RemoteNames2), err)
	}

	LocalNames = getLSIs(localStore)
	if len(LocalNames) != 1 {
		t.Errorf("TestCleanPut() len(LocalNames) == %d, expected 1) %s", len(LocalNames), err)
	}

	remoteStoreIndexMerged, err := GetStoreLSI(context.Background(), remoteStore, &localStore)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndexMerged.Dispose()
	if remoteStoreIndexMerged.GetBlockCount() != 5 {
		t.Errorf("TestCleanPut() remoteStoreIndexMerged.GetBlockCount() == %d, expected 5) %s", remoteStoreIndexMerged.GetBlockCount(), err)
		return
	}

	LocalNames = getLSIs(localStore)
	if len(LocalNames) != 2 {
		t.Errorf("TestCleanPut() len(LocalNames) == %d, expected 2) %s", len(LocalNames), err)
	}

	remoteClient, _ := remoteStore.NewClient(context.Background())
	err = longtailutils.DeleteBlob(context.Background(), remoteClient, RemoteNames1[0])
	if err != nil {
		t.Errorf("TestCleanPut() obj.Delete()) %s", err)
	}

	remoteStoreIndexPruned, err := GetStoreLSI(context.Background(), remoteStore, &localStore)
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

func TestMergeAtPut(t *testing.T) {
	remoteStore, _ := longtailstorelib.NewMemBlobStore("remote", true)
	localStore, _ := longtailstorelib.NewMemBlobStore("local", true)

	storeIndex1, _ := generateStoreIndex(t, 1, uint8(11))
	defer storeIndex1.Dispose()

	LSI1, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex1, 0)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}
	defer LSI1.Dispose()
	if LSI1.GetBlockCount() != 1 {
		t.Errorf("TestCleanPut() LSI1.GetBlockCount() == %d, expected 1)", LSI1.GetBlockCount())
		return
	}

	storeIndex2, _ := generateStoreIndex(t, 2, uint8(22))
	defer storeIndex2.Dispose()

	LSI2, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex2, 0)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}
	defer LSI2.Dispose()
	if LSI2.GetBlockCount() != 3 {
		t.Errorf("TestCleanPut() LSI2.GetBlockCount() == %d, expected 3)", LSI2.GetBlockCount())
		return
	}

	storeIndex3, _ := generateStoreIndex(t, 3, uint8(33))
	defer storeIndex3.Dispose()

	LSI3, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex3, 0)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}
	defer LSI3.Dispose()
	if LSI3.GetBlockCount() != 6 {
		t.Errorf("TestCleanPut() LSI3.GetBlockCount() == %d, expected 6)", LSI3.GetBlockCount())
		return
	}

	storeIndex4, _ := generateStoreIndex(t, 4, uint8(44))
	defer storeIndex4.Dispose()

	LSI4, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex4, 530)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}
	defer LSI4.Dispose()
	if LSI4.GetBlockCount() != 10 {
		t.Errorf("TestCleanPut() LSI4.GetBlockCount() == %d, expected 10)", LSI4.GetBlockCount())
		return
	}

	lsis := getLSIs(remoteStore)
	if len(lsis) != 2 {
		t.Errorf("TestForcedMerge() len(lsis) == %d, expected 2) %s", len(lsis), err)
	}

	resultStoreIndex, err := GetStoreLSI(context.Background(), remoteStore, &localStore)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer resultStoreIndex.Dispose()
	if resultStoreIndex.GetBlockCount() != 10 {
		t.Errorf("TestCleanPut() resultStoreIndex.GetBlockCount() == %d, expected 10)", resultStoreIndex.GetBlockCount())
		return
	}

	noCacheStoreIndex, err := GetStoreLSI(context.Background(), remoteStore, nil)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer noCacheStoreIndex.Dispose()
	if noCacheStoreIndex.GetBlockCount() != 10 {
		t.Errorf("TestCleanPut() noCacheStoreIndex.GetBlockCount() == %d, expected 10)", noCacheStoreIndex.GetBlockCount())
		return
	}
}
