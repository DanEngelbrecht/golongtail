package remotestore

import (
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
)

type testLSIStore struct {
	lsis map[string][]byte
}

func (store *testLSIStore) LSIStore_ListLSIs() ([]string, []int64, error) {
	count := len(store.lsis)
	names := make([]string, (count))
	sizes := make([]int64, (count))
	index := 0
	for k, v := range store.lsis {
		names[index] = k
		sizes[index] = int64(len(v))
		index++
	}
	return names, sizes, nil
}

func (store *testLSIStore) LSIStore_Read(Name string) ([]byte, error) {
	return store.lsis[Name], nil
}

func (store *testLSIStore) LSIStore_Write(Name string, buffer []byte) error {
	store.lsis[Name] = buffer
	return nil
}
func (store *testLSIStore) LSIStore_Delete(Name string) error {
	delete(store.lsis, Name)
	return nil
}

func NewTestLSIStore() (LSIStore, error) {
	s := &testLSIStore{}
	s.lsis = make(map[string][]byte)
	return s, nil
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

	remoteLSIStore, err := NewTestLSIStore()
	if err != nil {
		t.Errorf("TestCleanPut() NewTestLSIStore()) %s", err)
		return
	}
	err = PutStoreLSI(&remoteLSIStore, storeIndex)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}
	localLSIStore, err := NewTestLSIStore()
	if err != nil {
		t.Errorf("TestCleanPut() NewTestLSIStore()) %s", err)
		return
	}
	remoteStoreIndex, err := GetStoreLSI(&remoteLSIStore, &localLSIStore)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndex.Dispose()
	LocalNames, _, err := localLSIStore.LSIStore_ListLSIs()
	if err != nil {
		t.Errorf("TestCleanPut() LSIStore_ListLSIs()) %s", err)
		return
	}
	if len(LocalNames) != 1 {
		t.Errorf("TestCleanPut() len(LocalNames) == %d, expected 1) %s", len(LocalNames), err)
	}
	remoteStoreIndexCached, err := GetStoreLSI(&remoteLSIStore, &localLSIStore)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndexCached.Dispose()

	err = PutStoreLSI(&remoteLSIStore, storeIndex)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}
	RemoteNames1, _, err := remoteLSIStore.LSIStore_ListLSIs()
	if err != nil {
		t.Errorf("TestCleanPut() LSIStore_ListLSIs()) %s", err)
		return
	}
	if len(RemoteNames1) != 1 {
		t.Errorf("TestCleanPut() len(RemoteNames) == %d, expected 1) %s", len(RemoteNames1), err)
	}

	storeIndex2, err := GenerateStoreIndex(t, 4, uint8(33))
	if err != nil {
		t.Errorf("TestCleanPut() GenerateStoreIndex(t, 1, uint8(33)) %s", err)
		return
	}
	defer storeIndex2.Dispose()

	err = PutStoreLSI(&remoteLSIStore, storeIndex2)
	if err != nil {
		t.Errorf("TestCleanPut() PutStoreLSI()) %s", err)
		return
	}

	RemoteNames2, _, err := remoteLSIStore.LSIStore_ListLSIs()
	if err != nil {
		t.Errorf("TestCleanPut() LSIStore_ListLSIs()) %s", err)
		return
	}
	if len(RemoteNames2) != 2 {
		t.Errorf("TestCleanPut() len(RemoteNames) == %d, expected 2) %s", len(RemoteNames2), err)
	}

	LocalNames, _, err = localLSIStore.LSIStore_ListLSIs()
	if err != nil {
		t.Errorf("TestCleanPut() LSIStore_ListLSIs()) %s", err)
		return
	}
	if len(LocalNames) != 1 {
		t.Errorf("TestCleanPut() len(LocalNames) == %d, expected 1) %s", len(LocalNames), err)
	}

	remoteStoreIndexMerged, err := GetStoreLSI(&remoteLSIStore, &localLSIStore)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndexMerged.Dispose()
	if remoteStoreIndexMerged.GetBlockCount() != 5 {
		t.Errorf("TestCleanPut() remoteStoreIndexMerged.GetBlockCount() == %d, expected 5) %s", remoteStoreIndexMerged.GetBlockCount(), err)
		return
	}

	LocalNames, _, err = localLSIStore.LSIStore_ListLSIs()
	if err != nil {
		t.Errorf("TestCleanPut() LSIStore_ListLSIs()) %s", err)
		return
	}
	if len(LocalNames) != 2 {
		t.Errorf("TestCleanPut() len(LocalNames) == %d, expected 2) %s", len(LocalNames), err)
	}

	err = remoteLSIStore.LSIStore_Delete(RemoteNames1[0])
	if err != nil {
		t.Errorf("TestCleanPut() LSIStore_Delete(%s)) %s", RemoteNames1[0], err)
		return
	}

	remoteStoreIndexPruned, err := GetStoreLSI(&remoteLSIStore, &localLSIStore)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreIndexPruned.Dispose()
	if remoteStoreIndexPruned.GetBlockCount() != 4 {
		t.Errorf("TestCleanPut() remoteStoreIndexMerged.GetBlockCount() == %d, expected 1) %s", remoteStoreIndexPruned.GetBlockCount(), err)
		return
	}
	remoteStoreNoCache, err := GetStoreLSI(&remoteLSIStore, nil)
	if err != nil {
		t.Errorf("TestCleanPut() GetStoreLSI()) %s", err)
		return
	}
	defer remoteStoreNoCache.Dispose()
}
