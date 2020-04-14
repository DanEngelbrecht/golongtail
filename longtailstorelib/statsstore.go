package longtailstorelib

import (
	"fmt"
	"sync"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
)

type statsBlockStore struct {
	backingBlockStore longtaillib.Longtail_BlockStoreAPI
	getBlockToCount   map[uint64]uint64
	putBlockToCount   map[uint64]uint64
	mux               sync.Mutex
	stats             longtaillib.BlockStoreStats
}

func NewStatsBlockStore(backingBlockStore longtaillib.Longtail_BlockStoreAPI) longtaillib.BlockStoreAPI {
	s := &statsBlockStore{backingBlockStore: backingBlockStore, getBlockToCount: make(map[uint64]uint64), putBlockToCount: make(map[uint64]uint64)}
	return s
}

// PutStoredBlock ...
func (s *statsBlockStore) PutStoredBlock(storedBlock longtaillib.Longtail_StoredBlock, asyncCompleteAPI longtaillib.Longtail_AsyncPutStoredBlockAPI) int {
	blockIndex := storedBlock.GetBlockIndex()
	blockHash := blockIndex.GetBlockHash()
	s.mux.Lock()
	count := s.putBlockToCount[blockHash]
	s.putBlockToCount[blockHash] = count + 1
	s.mux.Unlock()
	return s.backingBlockStore.PutStoredBlock(storedBlock, asyncCompleteAPI)
}

// GetStoredBlock ...
func (s *statsBlockStore) GetStoredBlock(blockHash uint64, asyncCompleteAPI longtaillib.Longtail_AsyncGetStoredBlockAPI) int {
	s.mux.Lock()
	count := s.getBlockToCount[blockHash]
	s.getBlockToCount[blockHash] = count + 1
	s.mux.Unlock()
	return s.backingBlockStore.GetStoredBlock(blockHash, asyncCompleteAPI)
}

// GetIndex ...
func (s *statsBlockStore) GetIndex(defaultHashAPIIdentifier uint32, asyncCompleteAPI longtaillib.Longtail_AsyncGetIndexAPI) int {
	return s.backingBlockStore.GetIndex(defaultHashAPIIdentifier, asyncCompleteAPI)
}

// GetStats ...
func (s *statsBlockStore) GetStats() (longtaillib.BlockStoreStats, int) {
	return s.stats, 0
}

func (s *statsBlockStore) Close() {
	fmt.Printf("Put Blocks\n---------\n")
	for k := range s.putBlockToCount {
		if s.putBlockToCount[k] > 1 {
			fmt.Printf("%d -> %d\n", k, s.putBlockToCount[k])
		}
	}
	fmt.Printf("Get Blocks\n---------\n")
	for k := range s.getBlockToCount {
		if s.getBlockToCount[k] > 1 {
			fmt.Printf("%d -> %d\n", k, s.getBlockToCount[k])
		}
	}
}
