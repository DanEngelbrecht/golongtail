package commands

import (
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
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
	_ = logrus.WithFields(logrus.Fields{
		"numWorkerCount":             numWorkerCount,
		"blobStoreURI":               blobStoreURI,
		"sourceFilePath":             sourceFilePath,
		"versionLocalStoreIndexPath": versionLocalStoreIndexPath,
	})

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	indexStore, err := remotestore.CreateBlockStoreForURI(blobStoreURI, "", jobs, numWorkerCount, 8388608, 1024, remotestore.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer indexStore.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(sourceFilePath)
	if err != nil {
		err = errors.Wrapf(err, "File does not exist: %s", sourceFilePath)
		return storeStats, timeStats, err
	}
	if vbuffer == nil {
		err = errors.Wrapf(longtaillib.ErrENOENT, "File does not exist: %s", sourceFilePath)
		return storeStats, timeStats, longtaillib.ErrENOENT
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

	retargettedVersionStoreIndex, err := longtailutils.GetExistingStoreIndexSync(indexStore, chunkHashes, 0)
	if err != nil {
		err = errors.Wrapf(err, "Failed getting store index for version")
		return storeStats, timeStats, err
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
