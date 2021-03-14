package longtaillib

import (
	"reflect"
	"testing"
)

func TestSerializeStoreIndex(t *testing.T) {
	storeIndex := &StoreIndex{}
	storeIndex.Version = CurrentStoreIndexVersion
	storeIndex.HashIdentifier = 4711
	storeIndex.BlockHashes = []uint64{17, 27, 37, 47}
	storeIndex.ChunkHashes = []uint64{171, 172, 173, 271, 272, 273, 274, 371, 471, 472}
	storeIndex.BlockChunksOffsets = []uint32{0, 3, 7, 8}
	storeIndex.BlockChunkCounts = []uint32{3, 4, 1, 2}
	storeIndex.BlockTags = []uint32{7, 7, 7, 7}
	storeIndex.ChunkSizes = []uint32{17100, 17200, 17300, 2710, 2720, 2730, 2740, 3710, 471, 472}

	storeIndexBytes := DirectWriteStoreIndex(storeIndex)
	storeIndexCopy := DirectReadStoreIndex(storeIndexBytes)
	if !reflect.DeepEqual(storeIndex, storeIndexCopy) {
		t.Errorf("TestSerializeStoreIndex() %v != %v", storeIndex, storeIndexCopy)
	}
}

func TestDirectGetExistingStoreIndex(t *testing.T) {
	storeIndex := &StoreIndex{}
	storeIndex.Version = CurrentStoreIndexVersion
	storeIndex.HashIdentifier = 4711
	storeIndex.BlockHashes = []uint64{17, 27, 37, 47}
	storeIndex.ChunkHashes = []uint64{171, 172, 173, 271, 272, 273, 274, 371, 471, 472}
	storeIndex.BlockChunksOffsets = []uint32{0, 3, 7, 8}
	storeIndex.BlockChunkCounts = []uint32{3, 4, 1, 2}
	storeIndex.BlockTags = []uint32{7, 7, 7, 7}
	storeIndex.ChunkSizes = []uint32{17100, 17200, 17300, 2710, 2720, 2730, 2740, 3710, 471, 472}

	chunkHashes := []uint64{171, 172, 173, 271, 272, 273, 274, 371, 471, 472}

	resultStoreIndex := DirectGetExistingStoreIndex(storeIndex, chunkHashes, 0)
	if !reflect.DeepEqual(storeIndex, resultStoreIndex) {
		t.Errorf("TestSerializeStoreIndex() %v != %v", storeIndex, resultStoreIndex)
	}

	resultStoreIndex = DirectGetExistingStoreIndex(storeIndex, []uint64{371}, 0)
	if len(resultStoreIndex.BlockHashes) != 1 {
		t.Errorf("TestSerializeStoreIndex() %d != %d", len(resultStoreIndex.BlockHashes), 1)
	}
	if resultStoreIndex.BlockHashes[0] != storeIndex.BlockHashes[2] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.BlockHashes[0], storeIndex.BlockHashes[2])
	}
	if len(resultStoreIndex.ChunkHashes) != 1 {
		t.Errorf("TestSerializeStoreIndex() %d != %d", len(resultStoreIndex.ChunkHashes), 2)
	}
	if resultStoreIndex.ChunkHashes[0] != storeIndex.ChunkHashes[7] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.ChunkHashes[0], storeIndex.ChunkHashes[7])
	}

	resultStoreIndex = DirectGetExistingStoreIndex(storeIndex, []uint64{471}, 0)
	if len(resultStoreIndex.BlockHashes) != 1 {
		t.Errorf("TestSerializeStoreIndex() %d != %d", len(resultStoreIndex.BlockHashes), 1)
	}
	if resultStoreIndex.BlockHashes[0] != storeIndex.BlockHashes[3] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.BlockHashes[0], storeIndex.BlockHashes[2])
	}
	if len(resultStoreIndex.ChunkHashes) != 2 {
		t.Errorf("TestSerializeStoreIndex() %d != %d", len(resultStoreIndex.ChunkHashes), 2)
	}
	if resultStoreIndex.ChunkHashes[0] != storeIndex.ChunkHashes[8] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.ChunkHashes[0], storeIndex.ChunkHashes[8])
	}
	if resultStoreIndex.ChunkHashes[1] != storeIndex.ChunkHashes[9] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.ChunkHashes[0], storeIndex.ChunkHashes[9])
	}

	resultStoreIndex = DirectGetExistingStoreIndex(storeIndex, []uint64{171, 172, 272, 273, 371, 472}, 60)
	if len(resultStoreIndex.BlockHashes) != 2 {
		t.Errorf("TestSerializeStoreIndex() %d != %d", len(resultStoreIndex.BlockHashes), 1)
	}
	if resultStoreIndex.BlockHashes[0] != storeIndex.BlockHashes[0] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.BlockHashes[0], storeIndex.BlockHashes[2])
	}
	if resultStoreIndex.BlockHashes[1] != storeIndex.BlockHashes[2] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.BlockHashes[0], storeIndex.BlockHashes[2])
	}
	if len(resultStoreIndex.ChunkHashes) != 4 {
		t.Errorf("TestSerializeStoreIndex() %d != %d", len(resultStoreIndex.ChunkHashes), 2)
	}
	if resultStoreIndex.ChunkHashes[0] != storeIndex.ChunkHashes[0] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.ChunkHashes[0], storeIndex.ChunkHashes[8])
	}
	if resultStoreIndex.ChunkHashes[1] != storeIndex.ChunkHashes[1] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.ChunkHashes[1], storeIndex.ChunkHashes[1])
	}
	if resultStoreIndex.ChunkHashes[2] != storeIndex.ChunkHashes[2] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.ChunkHashes[2], storeIndex.ChunkHashes[2])
	}
	if resultStoreIndex.ChunkHashes[3] != storeIndex.ChunkHashes[3] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.ChunkHashes[3], storeIndex.ChunkHashes[3])
	}

	resultStoreIndex = DirectGetExistingStoreIndex(storeIndex, []uint64{271, 371}, 0)
	if len(resultStoreIndex.BlockHashes) != 2 {
		t.Errorf("TestSerializeStoreIndex() %d != %d", len(resultStoreIndex.BlockHashes), 1)
	}
	if resultStoreIndex.BlockHashes[0] != storeIndex.BlockHashes[2] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.BlockHashes[0], storeIndex.BlockHashes[2])
	}
	if resultStoreIndex.BlockHashes[1] != storeIndex.BlockHashes[1] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.BlockHashes[0], storeIndex.BlockHashes[2])
	}
	if len(resultStoreIndex.ChunkHashes) != 5 {
		t.Errorf("TestSerializeStoreIndex() %d != %d", len(resultStoreIndex.ChunkHashes), 2)
	}
	if resultStoreIndex.ChunkHashes[0] != storeIndex.ChunkHashes[7] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.ChunkHashes[0], storeIndex.ChunkHashes[7])
	}
	if resultStoreIndex.ChunkHashes[1] != storeIndex.ChunkHashes[3] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.ChunkHashes[1], storeIndex.ChunkHashes[3])
	}
	if resultStoreIndex.ChunkHashes[2] != storeIndex.ChunkHashes[4] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.ChunkHashes[2], storeIndex.ChunkHashes[4])
	}
	if resultStoreIndex.ChunkHashes[3] != storeIndex.ChunkHashes[5] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.ChunkHashes[3], storeIndex.ChunkHashes[5])
	}
	if resultStoreIndex.ChunkHashes[4] != storeIndex.ChunkHashes[6] {
		t.Errorf("TestSerializeStoreIndex() %d != %d", resultStoreIndex.ChunkHashes[4], storeIndex.ChunkHashes[6])
	}
}
