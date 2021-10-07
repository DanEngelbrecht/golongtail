package longtailutils

import (
	"sync"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
)

type getExistingContentCompletionAPI struct {
	wg         sync.WaitGroup
	storeIndex longtaillib.Longtail_StoreIndex
	err        int
}

func (a *getExistingContentCompletionAPI) OnComplete(storeIndex longtaillib.Longtail_StoreIndex, err int) {
	a.err = err
	a.storeIndex = storeIndex
	a.wg.Done()
}

type pruneBlocksCompletionAPI struct {
	wg               sync.WaitGroup
	prunedBlockCount uint32
	err              int
}

func (a *pruneBlocksCompletionAPI) OnComplete(prunedBlockCount uint32, err int) {
	a.err = err
	a.prunedBlockCount = prunedBlockCount
	a.wg.Done()
}

type flushCompletionAPI struct {
	asyncFlushAPI longtaillib.Longtail_AsyncFlushAPI
	wg            sync.WaitGroup
	err           int
}

func (a *flushCompletionAPI) OnComplete(err int) {
	a.err = err
	a.wg.Done()
}

// GetStoredBlockCompletionAPI ...
type GetStoredBlockCompletionAPI struct {
	Wg          sync.WaitGroup
	StoredBlock longtaillib.Longtail_StoredBlock
	Err         int
}

func (a *GetStoredBlockCompletionAPI) OnComplete(storedBlock longtaillib.Longtail_StoredBlock, err int) {
	a.Err = err
	a.StoredBlock = storedBlock
	a.Wg.Done()
}

// GetExistingStoreIndexSync ...
func GetExistingStoreIndexSync(indexStore longtaillib.Longtail_BlockStoreAPI, chunkHashes []uint64, minBlockUsagePercent uint32) (longtaillib.Longtail_StoreIndex, int) {
	getExistingContentComplete := &getExistingContentCompletionAPI{}
	getExistingContentComplete.wg.Add(1)
	errno := indexStore.GetExistingContent(chunkHashes, minBlockUsagePercent, longtaillib.CreateAsyncGetExistingContentAPI(getExistingContentComplete))
	if errno != 0 {
		getExistingContentComplete.wg.Done()
		getExistingContentComplete.wg.Wait()
		return longtaillib.Longtail_StoreIndex{}, errno
	}
	getExistingContentComplete.wg.Wait()
	return getExistingContentComplete.storeIndex, getExistingContentComplete.err
}

// PruneBlocksSync ...
func PruneBlocksSync(indexStore longtaillib.Longtail_BlockStoreAPI, keepBlockHashes []uint64) (uint32, int) {
	pruneBlocksComplete := &pruneBlocksCompletionAPI{}
	pruneBlocksComplete.wg.Add(1)
	errno := indexStore.PruneBlocks(keepBlockHashes, longtaillib.CreateAsyncPruneBlocksAPI(pruneBlocksComplete))
	if errno != 0 {
		pruneBlocksComplete.wg.Done()
		pruneBlocksComplete.wg.Wait()
		return 0, errno
	}
	pruneBlocksComplete.wg.Wait()
	return pruneBlocksComplete.prunedBlockCount, pruneBlocksComplete.err
}

// flushStore ...
func FlushStore(store *longtaillib.Longtail_BlockStoreAPI) (*flushCompletionAPI, int) {
	targetStoreFlushComplete := &flushCompletionAPI{}
	targetStoreFlushComplete.wg.Add(1)
	targetStoreFlushComplete.asyncFlushAPI = longtaillib.CreateAsyncFlushAPI(targetStoreFlushComplete)
	errno := store.Flush(targetStoreFlushComplete.asyncFlushAPI)
	if errno == 0 {
		return targetStoreFlushComplete, 0
	}
	targetStoreFlushComplete.wg.Done()
	return nil, errno
}

func (f *flushCompletionAPI) Wait() int {
	f.wg.Wait()
	return f.err
}

func FlushStoreSync(store *longtaillib.Longtail_BlockStoreAPI) int {
	f, errno := FlushStore(store)
	if errno != 0 {
		return errno
	}
	errno = f.Wait()
	return errno
}

type StoreFlush struct {
	flushAPIs []*flushCompletionAPI
}

// FlushStores ...
func FlushStores(stores []longtaillib.Longtail_BlockStoreAPI) (*StoreFlush, int) {
	storeFlush := &StoreFlush{}
	storeFlush.flushAPIs = make([]*flushCompletionAPI, len(stores))
	for i, store := range stores {
		if !store.IsValid() {
			continue
		}
		errno := 0
		storeFlush.flushAPIs[i], errno = FlushStore(&store)
		if errno != 0 {
			for i > 0 {
				i--
				flushAPI := storeFlush.flushAPIs[i]
				if flushAPI != nil {
					flushAPI.Wait()
				}
			}
			return nil, errno
		}
	}
	return storeFlush, 0
}

// Wait
func (s *StoreFlush) Wait() int {
	result := 0
	for _, flushAPI := range s.flushAPIs {
		if flushAPI == nil {
			continue
		}
		flushAPI.Wait()
		if flushAPI.err != 0 {
			result = flushAPI.err
		}
		flushAPI.asyncFlushAPI.Dispose()
	}

	return result
}

func FlushStoresSync(stores []longtaillib.Longtail_BlockStoreAPI) int {
	f, errno := FlushStores(stores)
	if errno != 0 {
		return errno
	}
	errno = f.Wait()
	return errno
}
