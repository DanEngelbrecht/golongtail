package commands

import (
	"fmt"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/DanEngelbrecht/golongtail/remotestore"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func printVersionUsage(
	numWorkerCount int,
	blobStoreURI string,
	versionIndexPath string,
	localCachePath string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "printVersionUsage"
	log := logrus.WithFields(logrus.Fields{
		"fname":            fname,
		"numWorkerCount":   numWorkerCount,
		"blobStoreURI":     blobStoreURI,
		"versionIndexPath": versionIndexPath,
		"localCachePath":   localCachePath,
	})
	log.Debug(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	var indexStore longtaillib.Longtail_BlockStoreAPI

	remoteIndexStore, err := remotestore.CreateBlockStoreForURI(blobStoreURI, "", jobs, numWorkerCount, 8388608, 1024, remotestore.ReadOnly, false)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer remoteIndexStore.Dispose()

	var localFS longtaillib.Longtail_StorageAPI

	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI

	if localCachePath == "" {
		indexStore = remoteIndexStore
	} else {
		localFS = longtaillib.CreateFSStorageAPI()
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, longtailutils.NormalizePath(localCachePath), false)

		cacheBlockStore = longtaillib.CreateCacheBlockStore(jobs, localIndexStore, remoteIndexStore)

		indexStore = cacheBlockStore
	}

	defer localFS.Dispose()
	defer localIndexStore.Dispose()
	defer cacheBlockStore.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	readSourceStartTime := time.Now()
	vbuffer, err := longtailutils.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	versionIndex, err := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if err != nil {
		err = errors.Wrapf(err, "Cant parse version index from `%s`", versionIndexPath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	getExistingContentStartTime := time.Now()
	existingStoreIndex, err := longtailutils.GetExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(), 0)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer existingStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get store index", getExistingContentTime})

	blockLookup := make(map[uint64]uint64)

	blockChunkCount := uint32(0)

	fetchingBlocksStartTime := time.Now()

	progress := longtailutils.CreateProgress("Fetching blocks           ", 1)
	defer progress.Dispose()

	blockHashes := existingStoreIndex.GetBlockHashes()
	maxBatchSize := int(numWorkerCount)
	for i := 0; i < len(blockHashes); {
		batchSize := len(blockHashes) - i
		if batchSize > maxBatchSize {
			batchSize = maxBatchSize
		}
		completions := make([]longtailutils.GetStoredBlockCompletionAPI, batchSize)
		for offset := 0; offset < batchSize; offset++ {
			completions[offset].Wg.Add(1)
			go func(startIndex int, offset int) {
				blockHash := blockHashes[startIndex+offset]
				indexStore.GetStoredBlock(blockHash, longtaillib.CreateAsyncGetStoredBlockAPI(&completions[offset]))
			}(i, offset)
		}

		for offset := 0; offset < batchSize; offset++ {
			completions[offset].Wg.Wait()
			if completions[offset].Err != nil {
				return storeStats, timeStats, errors.Wrapf(err, "stats: remoteStoreIndex.GetStoredBlock() failed")
			}
			blockIndex := completions[offset].StoredBlock.GetBlockIndex()
			for _, chunkHash := range blockIndex.GetChunkHashes() {
				blockLookup[chunkHash] = blockHashes[i+offset]
			}
			blockChunkCount += uint32(len(blockIndex.GetChunkHashes()))
		}

		i += batchSize
		progress.OnProgress(uint32(len(blockHashes)), uint32(i))
	}

	fetchingBlocksTime := time.Since(fetchingBlocksStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Fetching blocks", fetchingBlocksTime})

	blockUsage := uint32(100)
	if blockChunkCount > 0 {
		blockUsage = uint32((100 * existingStoreIndex.GetChunkCount()) / blockChunkCount)
	}

	var assetFragmentCount uint64
	chunkHashes := versionIndex.GetChunkHashes()
	assetChunkCounts := versionIndex.GetAssetChunkCounts()
	assetChunkIndexStarts := versionIndex.GetAssetChunkIndexStarts()
	assetChunkIndexes := versionIndex.GetAssetChunkIndexes()
	assetCount := 0
	for a := uint32(0); a < versionIndex.GetAssetCount(); a++ {
		uniqueBlockCount := uint64(0)
		chunkCount := assetChunkCounts[a]
		chunkIndexOffset := assetChunkIndexStarts[a]
		lastBlockIndex := ^uint64(0)
		if chunkCount > 0 {
			assetCount++
		}
		for c := chunkIndexOffset; c < chunkIndexOffset+chunkCount; c++ {
			chunkIndex := assetChunkIndexes[c]
			chunkHash := chunkHashes[chunkIndex]
			blockIndex := blockLookup[chunkHash]
			if blockIndex != lastBlockIndex {
				uniqueBlockCount++
				lastBlockIndex = blockIndex
				assetFragmentCount++
			}
		}
	}
	assetFragmentation := uint32(0)
	if assetCount > 0 {
		assetFragmentation = uint32((100*assetFragmentCount)/uint64(assetCount) - 100)
	}

	fmt.Printf("Block Usage:          %d%%\n", blockUsage)
	fmt.Printf("Asset Fragmentation:  %d%%\n", assetFragmentation)

	flushStartTime := time.Now()

	stores := []longtaillib.Longtail_BlockStoreAPI{
		cacheBlockStore,
		localIndexStore,
		remoteIndexStore,
	}
	err = longtailutils.FlushStoresSync(stores)
	if err != nil {
		log.WithError(err).Error("longtailutils.FlushStoresSync failed")
		return storeStats, timeStats, err
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Flush", flushTime})

	cacheStoreStats, err := cacheBlockStore.GetStats()
	if err == nil {
		storeStats = append(storeStats, longtailutils.StoreStat{"Cache", cacheStoreStats})
	}
	localStoreStats, err := localIndexStore.GetStats()
	if err == nil {
		storeStats = append(storeStats, longtailutils.StoreStat{"Local", localStoreStats})
	}
	remoteStoreStats, err := remoteIndexStore.GetStats()
	if err == nil {
		storeStats = append(storeStats, longtailutils.StoreStat{"Remote", remoteStoreStats})
	}
	return storeStats, timeStats, nil
}

type PrintVersionUsageCmd struct {
	StorageURIOption
	VersionIndexPathOption
	CachePathOption
}

func (r *PrintVersionUsageCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := printVersionUsage(
		ctx.NumWorkerCount,
		r.StorageURI,
		r.VersionIndexPath,
		r.CachePath)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
