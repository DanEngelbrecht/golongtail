package commands

import (
	"context"
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
	s3EndpointResolverURI string,
	versionIndexPath string,
	localCachePath string,
	sourcePath string,
	targetPath string,
	enableFileMapping bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "cpVersionIndex"
	log := logrus.WithContext(context.Background()).WithFields(logrus.Fields{
		"fname":                 fname,
		"numWorkerCount":        numWorkerCount,
		"blobStoreURI":          blobStoreURI,
		"s3EndpointResolverURI": s3EndpointResolverURI,
		"versionIndexPath":      versionIndexPath,
		"localCachePath":        localCachePath,
		"sourcePath":            sourcePath,
		"targetPath":            targetPath,
		"enableFileMapping":     enableFileMapping,
	})
	log.Info(fname)

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
	remoteIndexStore, err := remotestore.CreateBlockStoreForURI(blobStoreURI, "", jobs, numWorkerCount, 8388608, 1024, remotestore.ReadOnly, enableFileMapping, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
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
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, longtailstorelib.NormalizeFileSystemPath(localCachePath), "", enableFileMapping)

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
	vbuffer, err := longtailutils.ReadFromURI(versionIndexPath, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
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

	hashIdentifier := versionIndex.GetHashIdentifier()

	hash, err := hashRegistry.GetHashAPI(hashIdentifier)
	if err != nil {
		err = errors.Wrapf(err, "Unsupported hash identifier `%d`", hashIdentifier)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	getExistingContentStartTime := time.Now()
	storeIndex, err := longtailutils.GetExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(), 0)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
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
	if err != nil {
		err = errors.Wrapf(err, "Cant create block store for `%s` using `%s`", versionIndexPath, blobStoreURI)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer blockStoreFS.Dispose()
	createBlockStoreFSTime := time.Since(createBlockStoreFSStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Create Blockstore FS", createBlockStoreFSTime})

	copyFileStartTime := time.Now()

	inFile, err := blockStoreFS.OpenReadFile(sourcePath)
	if err != nil {
		err = errors.Wrapf(err, "Longtail_StorageAPI.OpenReadFile failed for `%s`", sourcePath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer blockStoreFS.CloseFile(inFile)

	size, err := blockStoreFS.GetSize(inFile)
	if err != nil {
		err = errors.Wrapf(err, "Longtail_StorageAPI.GetSize failed for `%s`", sourcePath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	offset := uint64(0)
	for offset < size {
		left := size - offset
		if left > 128*1024*1024 {
			left = 128 * 1024 * 1024
		}
		data, err := blockStoreFS.Read(inFile, offset, left)
		if err != nil {
			err = errors.Wrapf(err, "Longtail_StorageAPI.Read failed for `%s`", sourcePath)
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
		longtailutils.WriteToURI(targetPath, data, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
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
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Flush", flushTime})

	shareStoreStats, err := indexStore.GetStats()
	if err == nil {
		storeStats = append(storeStats, longtailutils.StoreStat{"Share", shareStoreStats})
	}
	lruStoreStats, err := lruBlockStore.GetStats()
	if err == nil {
		storeStats = append(storeStats, longtailutils.StoreStat{"LRU", lruStoreStats})
	}
	compressStoreStats, err := compressBlockStore.GetStats()
	if err == nil {
		storeStats = append(storeStats, longtailutils.StoreStat{"Compress", compressStoreStats})
	}
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

type CpCmd struct {
	StorageURIOption
	S3EndpointResolverURLOption
	VersionIndexPathOption
	CachePathOption
	SourcePath string `name:"source path" arg:"" help:"Source path inside the version index to copy"`
	TargetPath string `name:"target path" arg:"" help:"Target uri path"`
	EnableFileMappingOption
}

func (r *CpCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := cpVersionIndex(
		ctx.NumWorkerCount,
		r.StorageURI,
		r.S3EndpointResolverURL,
		r.VersionIndexPath,
		r.CachePath,
		r.SourcePath,
		r.TargetPath,
		r.EnableFileMapping)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
