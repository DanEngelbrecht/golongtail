package commands

import (
	"context"
	"os"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/DanEngelbrecht/golongtail/remotestore"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func cpVersionIndex(
	numWorkerCount int,
	blobStoreURI string,
	versionIndexPath string,
	localCachePath string,
	sourcePath string,
	targetPath string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	log := logrus.WithContext(context.Background()).WithFields(logrus.Fields{
		"numWorkerCount":   numWorkerCount,
		"blobStoreURI":     blobStoreURI,
		"versionIndexPath": versionIndexPath,
		"localCachePath":   localCachePath,
		"sourcePath":       sourcePath,
		"targetPath":       targetPath,
	})

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()
	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	remoteIndexStore, err := remotestore.CreateBlockStoreForURI(blobStoreURI, "", jobs, numWorkerCount, 8388608, 1024, remotestore.ReadOnly)
	if err != nil {
		log.WithError(err).Error("Failed creating block store")
		return storeStats, timeStats, err
	}
	defer remoteIndexStore.Dispose()

	localFS := longtaillib.CreateFSStorageAPI()
	defer localFS.Dispose()

	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI
	var compressBlockStore longtaillib.Longtail_BlockStoreAPI

	if localCachePath == "" {
		compressBlockStore = longtaillib.CreateCompressBlockStore(remoteIndexStore, creg)
	} else {
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, longtailutils.NormalizePath(localCachePath))

		cacheBlockStore = longtaillib.CreateCacheBlockStore(jobs, localIndexStore, remoteIndexStore)

		compressBlockStore = longtaillib.CreateCompressBlockStore(cacheBlockStore, creg)
	}

	defer cacheBlockStore.Dispose()
	defer localIndexStore.Dispose()
	defer compressBlockStore.Dispose()

	lruBlockStore := longtaillib.CreateLRUBlockStoreAPI(compressBlockStore, 32)
	defer lruBlockStore.Dispose()
	indexStore := longtaillib.CreateShareBlockStore(lruBlockStore)
	defer indexStore.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		err = errors.Wrapf(err, "File does not exist: %s", versionIndexPath)
		return storeStats, timeStats, err
	}
	if vbuffer == nil {
		err = errors.Wrapf(longtaillib.ErrENOENT, "File does not exist: %s", versionIndexPath)
		return storeStats, timeStats, longtaillib.ErrENOENT
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cp: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	hashIdentifier := versionIndex.GetHashIdentifier()

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cp: hashRegistry.GetHashAPI() failed")
	}

	getExistingContentStartTime := time.Now()
	storeIndex, err := longtailutils.GetExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(), 0)
	if err != nil {
		err = errors.Wrapf(err, "Failed getting store index for version")
		return storeStats, timeStats, err
	}
	defer storeIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get store index", getExistingContentTime})

	createBlockStoreFSStartTime := time.Now()
	blockStoreFS := longtaillib.CreateBlockStoreStorageAPI(
		hash,
		jobs,
		indexStore,
		storeIndex,
		versionIndex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cp: hashRegistry.CreateBlockStoreStorageAPI() failed")
	}
	defer blockStoreFS.Dispose()
	createBlockStoreFSTime := time.Since(createBlockStoreFSStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Create Blockstore FS", createBlockStoreFSTime})

	copyFileStartTime := time.Now()
	// Only support writing to regular file path for now
	outFile, err := os.Create(targetPath)
	if err != nil {
		log.WithError(err).Error("Failed creating target file")
		return storeStats, timeStats, err
	}
	defer outFile.Close()

	inFile, errno := blockStoreFS.OpenReadFile(sourcePath)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cp: hashRegistry.OpenReadFile() failed")
	}
	defer blockStoreFS.CloseFile(inFile)

	size, errno := blockStoreFS.GetSize(inFile)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cp: blockStoreFS.GetSize() failed")
	}

	offset := uint64(0)
	for offset < size {
		left := size - offset
		if left > 128*1024*1024 {
			left = 128 * 1024 * 1024
		}
		data, errno := blockStoreFS.Read(inFile, offset, left)
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cp: hashRegistry.Read() failed")
		}
		outFile.Write(data)
		offset += left
	}
	copyFileTime := time.Since(copyFileStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Copy file", copyFileTime})

	flushStartTime := time.Now()

	stores := []longtaillib.Longtail_BlockStoreAPI{
		indexStore,
		lruBlockStore,
		compressBlockStore,
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

	shareStoreStats, errno := indexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Share", shareStoreStats})
	}
	lruStoreStats, errno := lruBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"LRU", lruStoreStats})
	}
	compressStoreStats, errno := compressBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Compress", compressStoreStats})
	}
	cacheStoreStats, errno := cacheBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Cache", cacheStoreStats})
	}
	localStoreStats, errno := localIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Local", localStoreStats})
	}
	remoteStoreStats, errno := remoteIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Remote", remoteStoreStats})
	}

	return storeStats, timeStats, nil
}

type CpCmd struct {
	StorageURIOption
	VersionIndexPathOption
	CachePathOption
	SourcePath string `name:"source path" arg:"" help:"Source path inside the version index to copy"`
	TargetPath string `name:"target path" arg:"" help:"Target uri path"`
}

func (r *CpCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := cpVersionIndex(
		ctx.NumWorkerCount,
		r.StorageURI,
		r.VersionIndexPath,
		r.CachePath,
		r.SourcePath,
		r.TargetPath)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
