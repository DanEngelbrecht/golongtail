package commands

import (
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/DanEngelbrecht/golongtail/remotestore"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func createVersionStoreIndex(
	numWorkerCount int,
	blobStoreURI string,
	sourceFilePath string,
	versionLocalStoreIndexPath string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "createVersionStoreIndex"
	log := logrus.WithFields(logrus.Fields{
		"fname":                      fname,
		"numWorkerCount":             numWorkerCount,
		"blobStoreURI":               blobStoreURI,
		"sourceFilePath":             sourceFilePath,
		"versionLocalStoreIndexPath": versionLocalStoreIndexPath,
	})
	log.Debug(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	indexStore, err := remotestore.CreateBlockStoreForURI(blobStoreURI, "", jobs, numWorkerCount, 8388608, 1024, remotestore.ReadOnly, false)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer indexStore.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	readSourceStartTime := time.Now()
	vbuffer, err := longtailutils.ReadFromURI(sourceFilePath)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	sourceVersionIndex, err := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if err != nil {
		err = errors.Wrapf(err, "Cant parse version index from `%s`", sourceFilePath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer sourceVersionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	getExistingContentStartTime := time.Now()
	chunkHashes := sourceVersionIndex.GetChunkHashes()

	retargettedVersionStoreIndex, err := longtailutils.GetExistingStoreIndexSync(indexStore, chunkHashes, 0)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer retargettedVersionStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get content index", getExistingContentTime})

	writeVersionLocalStoreIndexStartTime := time.Now()
	versionLocalStoreIndexBuffer, err := longtaillib.WriteStoreIndexToBuffer(retargettedVersionStoreIndex)
	if err != nil {
		err = errors.Wrapf(err, "Cant serialize store index for `%s`", sourceFilePath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	err = longtailutils.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
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
