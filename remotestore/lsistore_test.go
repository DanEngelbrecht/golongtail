package remotestore

import (
	"context"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/stretchr/testify/assert"
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

func generateStoreIndex(blockCount uint8, seed uint8) (longtaillib.Longtail_StoreIndex, error) {
	blocks := make([]longtaillib.Longtail_StoredBlock, 0)
	defer func(blocks []longtaillib.Longtail_StoredBlock) {
		for _, b := range blocks {
			b.Dispose()
		}
	}(blocks)
	blockIndexes := make([]longtaillib.Longtail_BlockIndex, 0)

	for block := uint8(0); block < blockCount; block++ {
		storedBlock, err := generateStoredBlock(seed + block)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, err
		}
		blocks = append(blocks, storedBlock)
		blockIndexes = append(blockIndexes, storedBlock.GetBlockIndex())
	}

	storeIndex, err := longtaillib.CreateStoreIndexFromBlocks(blockIndexes)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, err
	}
	return storeIndex, nil
}

func addStoreIndex(remoteStore longtailstorelib.BlobStore, seed uint8) error {
	storeIndex, _ := generateStoreIndex(seed/10, uint8(seed))
	defer storeIndex.Dispose()
	resultStoreIndex, err := PutStoreLSI(context.Background(), remoteStore, nil, storeIndex, 0)
	if err != nil {
		return err
	}
	defer resultStoreIndex.Dispose()
	return nil
}

func TestOverwrite(t *testing.T) {
	remoteStore, _ := longtailstorelib.NewMemBlobStore("remote", true)
	addStoreIndex(remoteStore, 22)
	addStoreIndex(remoteStore, 33)
	addStoreIndex(remoteStore, 44)
	addStoreIndex(remoteStore, 55)
	unprunedStoreIndex, err := GetStoreLSI(context.Background(), remoteStore, nil)
	assert.Equal(t, err, nil, "failed getting LSI")
	defer unprunedStoreIndex.Dispose()
	assert.Equal(t, unprunedStoreIndex.GetBlockCount(), uint32(14), "unpruned store index mismatch")

	storeIndex, _ := generateStoreIndex(7, uint8(77))
	err = OverwriteStoreLSI(context.Background(), remoteStore, storeIndex)
	assert.Equal(t, err, nil, "failed overwriting LSI")
	prunedStoreIndex, err := GetStoreLSI(context.Background(), remoteStore, nil)
	assert.Equal(t, err, nil, "failed getting pruned LSI")
	defer prunedStoreIndex.Dispose()
	assert.Equal(t, prunedStoreIndex.GetBlockCount(), uint32(7), "pruned store index mismatch")
}

func TestPutGet(t *testing.T) {
	storeIndex, _ := generateStoreIndex(1, uint8(77))
	defer storeIndex.Dispose()

	remoteStore, _ := longtailstorelib.NewMemBlobStore("remote", true)
	localStore, _ := longtailstorelib.NewMemBlobStore("local", true)

	emptyStoreIndex, err := GetStoreLSI(context.Background(), remoteStore, &localStore)
	assert.Equal(t, err, nil, "failed getting empty LSI")
	defer emptyStoreIndex.Dispose()
	assert.Equal(t, emptyStoreIndex.GetBlockCount(), uint32(0), "empty LSI is not empty")

	LSI1, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex, 0)
	assert.Equal(t, err, nil, "failed putting LSI1")
	defer LSI1.Dispose()
	assert.Equal(t, LSI1.GetBlockCount(), uint32(1), "Put LSI1 did not result in expected store index")

	remoteStoreIndex, err := GetStoreLSI(context.Background(), remoteStore, &localStore)
	assert.Equal(t, err, nil, "failed getting LSI")
	defer remoteStoreIndex.Dispose()

	LocalNames := getLSIs(localStore)
	assert.Equal(t, 1, len(LocalNames), "unexpeced number of indexes in local store")

	remoteStoreIndexCached, err := GetStoreLSI(context.Background(), remoteStore, &localStore)
	assert.Equal(t, err, nil, "failed getting cached LSI")
	defer remoteStoreIndexCached.Dispose()

	LSI2, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex, 0)
	assert.Equal(t, err, nil, "failed putting LSI2")
	defer LSI2.Dispose()
	assert.Equal(t, LSI2.GetBlockCount(), uint32(1), "Put LSI2 did not result in expected store index")

	RemoteNames1 := getLSIs(remoteStore)
	assert.Equal(t, 1, len(RemoteNames1), "unexpeced number of indexes in remote store")

	storeIndex2, err := generateStoreIndex(4, uint8(33))
	assert.Equal(t, err, nil, "failed generating storeIndex2")
	defer storeIndex2.Dispose()

	LSI3, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex2, 0)
	assert.Equal(t, err, nil, "failed putting LSI3")
	defer LSI3.Dispose()
	assert.Equal(t, LSI3.GetBlockCount(), uint32(5), "Put LSI3 did not result in expected store index")

	RemoteNames2 := getLSIs(remoteStore)
	assert.Equal(t, 2, len(RemoteNames2), "unexpeced number of indexes in remote store")

	LocalNames = getLSIs(localStore)
	assert.Equal(t, 1, len(LocalNames), "unexpeced number of indexes in local store")

	remoteStoreIndexMerged, err := GetStoreLSI(context.Background(), remoteStore, &localStore)
	assert.Equal(t, err, nil, "failed getting remoteStoreIndexMerged")
	defer remoteStoreIndexMerged.Dispose()
	assert.Equal(t, remoteStoreIndexMerged.GetBlockCount(), uint32(5), "Get remoteStoreIndexMerged did not result in expected store index")

	LocalNames = getLSIs(localStore)
	assert.Equal(t, 2, len(LocalNames), "unexpeced number of indexes in local store")

	remoteClient, _ := remoteStore.NewClient(context.Background())
	_, err = longtailutils.DeleteBlobWithRetry(context.Background(), remoteClient, RemoteNames1[0])
	assert.Equal(t, err, nil, "failed deleting remote blob")

	remoteStoreIndexPruned, err := GetStoreLSI(context.Background(), remoteStore, &localStore)
	assert.Equal(t, err, nil, "failed getting remoteStoreIndexPruned")
	defer remoteStoreIndexPruned.Dispose()
	assert.Equal(t, remoteStoreIndexPruned.GetBlockCount(), uint32(4), "Get remoteStoreIndexPruned did not result in expected store index")
}

func TestMergeAtPut(t *testing.T) {
	remoteStore, _ := longtailstorelib.NewMemBlobStore("remote", true)
	localStore, _ := longtailstorelib.NewMemBlobStore("local", true)

	storeIndex1, _ := generateStoreIndex(1, uint8(11))
	defer storeIndex1.Dispose()

	LSI1, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex1, 0)
	assert.Equal(t, err, nil, "failed putting LSI1")
	defer LSI1.Dispose()
	assert.Equal(t, LSI1.GetBlockCount(), uint32(1), "Put LSI1 did not result in expected store index")

	storeIndex2, _ := generateStoreIndex(2, uint8(22))
	defer storeIndex2.Dispose()

	LSI2, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex2, 0)
	assert.Equal(t, err, nil, "failed putting LSI2")
	defer LSI2.Dispose()
	assert.Equal(t, LSI2.GetBlockCount(), uint32(3), "Put LSI2 did not result in expected store index")

	storeIndex3, _ := generateStoreIndex(3, uint8(33))
	defer storeIndex3.Dispose()

	LSI3, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex3, 0)
	assert.Equal(t, err, nil, "failed putting LSI3")
	defer LSI3.Dispose()
	assert.Equal(t, LSI3.GetBlockCount(), uint32(6), "Put LSI3 did not result in expected store index")

	storeIndex4, _ := generateStoreIndex(4, uint8(44))
	defer storeIndex4.Dispose()

	LSI4, err := PutStoreLSI(context.Background(), remoteStore, &localStore, storeIndex4, 530)
	assert.Equal(t, err, nil, "failed putting LSI4")
	defer LSI4.Dispose()
	assert.Equal(t, LSI4.GetBlockCount(), uint32(10), "Put LSI4 did not result in expected store index")

	lsis := getLSIs(remoteStore)
	assert.Equal(t, 2, len(lsis), "Unexpeced number of store indexes in remote store")

	resultStoreIndex, err := GetStoreLSI(context.Background(), remoteStore, &localStore)
	assert.Equal(t, err, nil, "failed getting resultStoreIndex")
	defer resultStoreIndex.Dispose()
	assert.Equal(t, resultStoreIndex.GetBlockCount(), uint32(10), "Gut resultStoreIndex did not result in expected store index")

	noCacheStoreIndex, err := GetStoreLSI(context.Background(), remoteStore, nil)
	assert.Equal(t, err, nil, "failed getting noCacheStoreIndex")
	defer noCacheStoreIndex.Dispose()
	assert.Equal(t, noCacheStoreIndex.GetBlockCount(), uint32(10), "Gut noCacheStoreIndex did not result in expected store index")
}
