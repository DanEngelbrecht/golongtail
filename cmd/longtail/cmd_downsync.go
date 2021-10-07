package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
)

func downsync(
	numWorkerCount int,
	blobStoreURI string,
	sourceFilePath string,
	targetFolderPath string,
	targetIndexPath string,
	localCachePath string,
	retainPermissions bool,
	validate bool,
	versionLocalStoreIndexPath string,
	includeFilterRegEx string,
	excludeFilterRegEx string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	pathFilter, err := longtailutils.MakeRegexPathFilter(includeFilterRegEx, excludeFilterRegEx)
	if err != nil {
		return storeStats, timeStats, err
	}

	resolvedTargetFolderPath := ""
	if targetFolderPath == "" {
		urlSplit := strings.Split(normalizePath(sourceFilePath), "/")
		sourceName := urlSplit[len(urlSplit)-1]
		sourceNameSplit := strings.Split(sourceName, ".")
		resolvedTargetFolderPath = sourceNameSplit[0]
		if resolvedTargetFolderPath == "" {
			return storeStats, timeStats, fmt.Errorf("downsync: unable to resolve target path using `%s` as base", sourceFilePath)
		}
	} else {
		resolvedTargetFolderPath = targetFolderPath
	}

	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()

	targetFolderScanner := asyncFolderScanner{}
	if targetIndexPath == "" {
		targetFolderScanner.scan(resolvedTargetFolderPath, pathFilter, fs)
	}

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	readSourceStartTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(sourceFilePath)
	if err != nil {
		return storeStats, timeStats, err
	}
	sourceVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downsync: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer sourceVersionIndex.Dispose()

	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	hashIdentifier := sourceVersionIndex.GetHashIdentifier()
	targetChunkSize := sourceVersionIndex.GetTargetChunkSize()

	targetIndexReader := asyncVersionIndexReader{}
	targetIndexReader.read(resolvedTargetFolderPath,
		targetIndexPath,
		targetChunkSize,
		noCompressionType,
		hashIdentifier,
		pathFilter,
		fs,
		jobs,
		hashRegistry,
		&targetFolderScanner)

	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	localFS := longtaillib.CreateFSStorageAPI()
	defer localFS.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, versionLocalStoreIndexPath, jobs, numWorkerCount, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer remoteIndexStore.Dispose()

	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI
	var compressBlockStore longtaillib.Longtail_BlockStoreAPI

	if localCachePath == "" {
		compressBlockStore = longtaillib.CreateCompressBlockStore(remoteIndexStore, creg)
	} else {
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, normalizePath(localCachePath))

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

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downsync: longtaillib.GetHashAPI() failed")
	}

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	targetVersionIndex, hash, readTargetIndexTime, err := targetIndexReader.get()
	if err != nil {
		return storeStats, timeStats, err
	}
	defer targetVersionIndex.Dispose()
	timeStats = append(timeStats, longtailutils.TimeStat{"Read target index", readTargetIndexTime})

	getExistingContentStartTime := time.Now()
	versionDiff, errno := longtaillib.CreateVersionDiff(
		hash,
		targetVersionIndex,
		sourceVersionIndex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downsync: longtaillib.CreateVersionDiff() failed")
	}
	defer versionDiff.Dispose()

	chunkHashes, errno := longtaillib.GetRequiredChunkHashes(
		sourceVersionIndex,
		versionDiff)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.GetRequiredChunkHashes() failed")
	}

	retargettedVersionStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(indexStore, chunkHashes, 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downsync: longtailutils.GetExistingStoreIndexSync(indexStore, chunkHashes) failed")
	}
	defer retargettedVersionStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get content index", getExistingContentTime})

	changeVersionStartTime := time.Now()
	changeVersionProgress := longtailutils.CreateProgress("Updating version")
	defer changeVersionProgress.Dispose()
	errno = longtaillib.ChangeVersion(
		indexStore,
		fs,
		hash,
		jobs,
		&changeVersionProgress,
		retargettedVersionStoreIndex,
		targetVersionIndex,
		sourceVersionIndex,
		versionDiff,
		normalizePath(resolvedTargetFolderPath),
		retainPermissions)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downsync: longtaillib.ChangeVersion() failed")
	}

	changeVersionTime := time.Since(changeVersionStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Change version", changeVersionTime})

	flushStartTime := time.Now()

	stores := []longtaillib.Longtail_BlockStoreAPI{
		indexStore,
		lruBlockStore,
		compressBlockStore,
		cacheBlockStore,
		localIndexStore,
		remoteIndexStore,
	}
	errno = longtailutils.FlushStoresSync(stores)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStoresSync: Failed for `%v`", stores)
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

	if validate {
		validateStartTime := time.Now()
		validateFileInfos, errno := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			normalizePath(resolvedTargetFolderPath))
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downsync: longtaillib.GetFilesRecursively() failed")
		}
		defer validateFileInfos.Dispose()

		chunker := longtaillib.CreateHPCDCChunkerAPI()
		defer chunker.Dispose()

		createVersionIndexProgress := longtailutils.CreateProgress("Validating version")
		defer createVersionIndexProgress.Dispose()
		validateVersionIndex, errno := longtaillib.CreateVersionIndex(
			fs,
			hash,
			chunker,
			jobs,
			&createVersionIndexProgress,
			normalizePath(resolvedTargetFolderPath),
			validateFileInfos,
			nil,
			targetChunkSize)
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downsync: longtaillib.CreateVersionIndex() failed")
		}
		defer validateVersionIndex.Dispose()
		if validateVersionIndex.GetAssetCount() != sourceVersionIndex.GetAssetCount() {
			return storeStats, timeStats, fmt.Errorf("downsync: failed validation: asset count mismatch")
		}
		validateAssetSizes := validateVersionIndex.GetAssetSizes()
		validateAssetHashes := validateVersionIndex.GetAssetHashes()

		sourceAssetSizes := sourceVersionIndex.GetAssetSizes()
		sourceAssetHashes := sourceVersionIndex.GetAssetHashes()

		assetSizeLookup := map[string]uint64{}
		assetHashLookup := map[string]uint64{}
		assetPermissionLookup := map[string]uint16{}

		for i, s := range sourceAssetSizes {
			path := sourceVersionIndex.GetAssetPath(uint32(i))
			assetSizeLookup[path] = s
			assetHashLookup[path] = sourceAssetHashes[i]
			assetPermissionLookup[path] = sourceVersionIndex.GetAssetPermissions(uint32(i))
		}
		for i, validateSize := range validateAssetSizes {
			validatePath := validateVersionIndex.GetAssetPath(uint32(i))
			validateHash := validateAssetHashes[i]
			size, exists := assetSizeLookup[validatePath]
			hash := assetHashLookup[validatePath]
			if !exists {
				return storeStats, timeStats, fmt.Errorf("downsync: failed validation: invalid path %s", validatePath)
			}
			if size != validateSize {
				return storeStats, timeStats, fmt.Errorf("downsync: failed validation: asset %d size mismatch", i)
			}
			if hash != validateHash {
				return storeStats, timeStats, fmt.Errorf("downsync: failed validation: asset %d hash mismatch", i)
			}
			if retainPermissions {
				validatePermissions := validateVersionIndex.GetAssetPermissions(uint32(i))
				permissions := assetPermissionLookup[validatePath]
				if permissions != validatePermissions {
					return storeStats, timeStats, fmt.Errorf("downsync: failed validation: asset %d permission mismatch", i)
				}
			}
		}
		validateTime := time.Since(validateStartTime)
		timeStats = append(timeStats, longtailutils.TimeStat{"Validate", validateTime})
	}

	return storeStats, timeStats, nil
}

type DownsyncCmd struct {
	StorageURIOption
	SourceUriOption
	TargetPathOption
	TargetIndexUriOption
	CachePathOption
	RetainPermissionsOption
	ValidateTargetOption
	VersionLocalStoreIndexPathOption
	DownsyncIncludeRegExOption
	DownsyncExcludeRegExOption
}

func (r *DownsyncCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := downsync(
		ctx.NumWorkerCount,
		r.StorageURI,
		r.SourcePath,
		r.TargetPath,
		r.TargetIndexPath,
		r.CachePath,
		r.RetainPermissions,
		r.Validate,
		r.VersionLocalStoreIndexPath,
		r.IncludeFilterRegEx,
		r.ExcludeFilterRegEx)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
