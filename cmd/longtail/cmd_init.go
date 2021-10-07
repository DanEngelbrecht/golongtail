package main

import (
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
)

func init(
	numWorkerCount int,
	blobStoreURI string,
	hashAlgorithm string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, numWorkerCount, 8388608, 1024, longtailstorelib.Init)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer remoteIndexStore.Dispose()
	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	getExistingContentStartTime := time.Now()
	retargetStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(remoteIndexStore, []uint64{}, 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "init: longtailutils.GetExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(): Failed for `%s` failed", blobStoreURI)
	}
	defer retargetStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get store index", getExistingContentTime})

	flushStartTime := time.Now()

	f, errno := longtailutils.FlushStore(&remoteIndexStore)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStore: Failed for `%v`", blobStoreURI)
	}
	errno = f.Wait()
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStore: Failed for `%v`", blobStoreURI)
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Flush", flushTime})

	remoteStoreStats, errno := remoteIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Remote", remoteStoreStats})
	}

	return storeStats, timeStats, nil
}

type InitCmd struct {
	StorageURIOption
	HashingOption
}

func (r *InitCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := init(
		ctx.NumWorkerCount,
		r.StorageURI,
		r.Hashing)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
