package commands

import (
	"fmt"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/DanEngelbrecht/golongtail/remotestore"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	const fname = "downsync"
	log := logrus.WithFields(logrus.Fields{
		"fname":                      fname,
		"numWorkerCount":             numWorkerCount,
		"blobStoreURI":               blobStoreURI,
		"sourceFilePath":             sourceFilePath,
		"targetFolderPath":           targetFolderPath,
		"targetIndexPath":            targetIndexPath,
		"localCachePath":             localCachePath,
		"retainPermissions":          retainPermissions,
		"validate":                   validate,
		"versionLocalStoreIndexPath": versionLocalStoreIndexPath,
		"includeFilterRegEx":         includeFilterRegEx,
		"excludeFilterRegEx":         excludeFilterRegEx,
	})
	log.Debug(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	pathFilter, err := longtailutils.MakeRegexPathFilter(includeFilterRegEx, excludeFilterRegEx)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	resolvedTargetFolderPath := ""
	if targetFolderPath == "" {
		urlSplit := strings.Split(longtailutils.NormalizePath(sourceFilePath), "/")
		sourceName := urlSplit[len(urlSplit)-1]
		sourceNameSplit := strings.Split(sourceName, ".")
		resolvedTargetFolderPath = sourceNameSplit[0]
		if resolvedTargetFolderPath == "" {
			err = fmt.Errorf("Unable to resolve target path using `%s` as base", sourceFilePath)
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
	} else {
		resolvedTargetFolderPath = targetFolderPath
	}

	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()

	targetFolderScanner := longtailutils.AsyncFolderScanner{}
	if targetIndexPath == "" {
		targetFolderScanner.Scan(resolvedTargetFolderPath, pathFilter, fs)
	}

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	readSourceStartTime := time.Now()

	vbuffer, err := longtailutils.ReadFromURI(sourceFilePath)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	sourceVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Cant parse version index from `%s`", sourceFilePath))
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer sourceVersionIndex.Dispose()

	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	hashIdentifier := sourceVersionIndex.GetHashIdentifier()
	targetChunkSize := sourceVersionIndex.GetTargetChunkSize()

	targetIndexReader := longtailutils.AsyncVersionIndexReader{}
	targetIndexReader.Read(resolvedTargetFolderPath,
		targetIndexPath,
		targetChunkSize,
		longtailutils.NoCompressionType,
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
	remoteIndexStore, err := remotestore.CreateBlockStoreForURI(blobStoreURI, versionLocalStoreIndexPath, jobs, numWorkerCount, 8388608, 1024, remotestore.ReadOnly)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer remoteIndexStore.Dispose()

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

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Unsupported hash identifier `%d`", hashIdentifier))
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	targetVersionIndex, hash, readTargetIndexTime, err := targetIndexReader.Get()
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer targetVersionIndex.Dispose()
	timeStats = append(timeStats, longtailutils.TimeStat{"Read target index", readTargetIndexTime})

	getExistingContentStartTime := time.Now()
	versionDiff, errno := longtaillib.CreateVersionDiff(
		hash,
		targetVersionIndex,
		sourceVersionIndex)
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Failed to create version diff. `%s` -> `%s`", targetFolderPath, sourceFilePath))
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer versionDiff.Dispose()

	chunkHashes, errno := longtaillib.GetRequiredChunkHashes(
		sourceVersionIndex,
		versionDiff)
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Failed to get required chunk hashes. `%s` -> `%s`", targetFolderPath, sourceFilePath))
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	retargettedVersionStoreIndex, err := longtailutils.GetExistingStoreIndexSync(indexStore, chunkHashes, 0)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
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
		longtailutils.NormalizePath(resolvedTargetFolderPath),
		retainPermissions)
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Failed writing version to `%s`", targetFolderPath))
		return storeStats, timeStats, errors.Wrap(err, fname)
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
	err = longtailutils.FlushStoresSync(stores)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
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
			longtailutils.NormalizePath(resolvedTargetFolderPath))
		if errno != 0 {
			err = longtailutils.MakeError(errno, fmt.Sprintf("Failed to scan `%s`", resolvedTargetFolderPath))
			return storeStats, timeStats, errors.Wrap(err, fname)
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
			longtailutils.NormalizePath(resolvedTargetFolderPath),
			validateFileInfos,
			nil,
			targetChunkSize)
		if errno != 0 {
			err = longtailutils.MakeError(errno, fmt.Sprintf("Failed to create version index for `%s`", resolvedTargetFolderPath))
			return storeStats, timeStats, errors.Wrap(err, fname)
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
