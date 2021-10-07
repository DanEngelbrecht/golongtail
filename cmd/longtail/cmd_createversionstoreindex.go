package main

import (
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
)

func createVersionStoreIndex(
	numWorkerCount int,
	blobStoreURI string,
	sourceFilePath string,
	versionLocalStoreIndexPath string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	indexStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, numWorkerCount, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer indexStore.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(sourceFilePath)
	if err != nil {
		return storeStats, timeStats, err
	}
	sourceVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "createVersionStoreIndex: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer sourceVersionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	getExistingContentStartTime := time.Now()
	chunkHashes := sourceVersionIndex.GetChunkHashes()

	retargettedVersionStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(indexStore, chunkHashes, 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "createVersionStoreIndex: longtailutils.GetExistingStoreIndexSync(indexStore, chunkHashes) failed")
	}
	defer retargettedVersionStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get content index", getExistingContentTime})

	writeVersionLocalStoreIndexStartTime := time.Now()
	versionLocalStoreIndexBuffer, errno := longtaillib.WriteStoreIndexToBuffer(retargettedVersionStoreIndex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "createVersionStoreIndex: longtaillib.WriteStoreIndexToBuffer() failed")
	}
	err = longtailstorelib.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, "createVersionStoreIndex: longtaillib.longtailstorelib.WriteToURL() failed")
	}
	writeVersionLocalStoreIndexTime := time.Since(writeVersionLocalStoreIndexStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Write version store index", writeVersionLocalStoreIndexTime})

	return storeStats, timeStats, nil
}

type CreateVersionStoreIndexCmd struct {
	StorageURIOption
	SourceUriOption
	VersionLocalStoreIndexPathOption
}

func (r *CreateVersionStoreIndexCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := createVersionStoreIndex(
		ctx.NumWorkerCount,
		r.StorageURI,
		r.SourcePath,
		r.VersionLocalStoreIndexPath)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
