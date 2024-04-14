package commands

import (
	"fmt"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/DanEngelbrecht/golongtail/remotestore"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func readVersionIndex(sourceFilePath string, opts ...longtailstorelib.BlobStoreOption) (longtaillib.Longtail_VersionIndex, error) {
	const fname = "readVersionIndex"
	vbuffer, err := longtailutils.ReadFromURI(sourceFilePath, opts...)
	if err != nil {
		return longtaillib.Longtail_VersionIndex{}, errors.Wrap(err, fname)
	}
	sourceVersionIndex, err := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if err != nil {
		err = errors.Wrapf(err, "Cant parse version index from `%s`", sourceFilePath)
		return longtaillib.Longtail_VersionIndex{}, errors.Wrap(err, fname)
	}
	return sourceVersionIndex, nil
}

func downsync(
	numWorkerCount int,
	blobStoreURI string,
	s3EndpointResolverURI string,
	sourceFilePath string,
	sourceFilePaths []string,
	targetFolderPath string,
	targetIndexPath string,
	localCachePath string,
	retainPermissions bool,
	validate bool,
	versionLocalStoreIndexPath string,
	versionLocalStoreIndexPaths []string,
	includeFilterRegEx string,
	excludeFilterRegEx string,
	scanTarget bool,
	cacheTargetIndex bool,
	enableFileMapping bool,
	useLegacyWrite bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "downsync"
	log := logrus.WithFields(logrus.Fields{
		"fname":                       fname,
		"numWorkerCount":              numWorkerCount,
		"blobStoreURI":                blobStoreURI,
		"s3EndpointResolverURI":       s3EndpointResolverURI,
		"sourceFilePath":              sourceFilePath,
		"sourceFilePaths":             sourceFilePaths,
		"targetFolderPath":            targetFolderPath,
		"targetIndexPath":             targetIndexPath,
		"localCachePath":              localCachePath,
		"retainPermissions":           retainPermissions,
		"validate":                    validate,
		"versionLocalStoreIndexPath":  versionLocalStoreIndexPath,
		"versionLocalStoreIndexPaths": versionLocalStoreIndexPaths,
		"includeFilterRegEx":          includeFilterRegEx,
		"excludeFilterRegEx":          excludeFilterRegEx,
		"scanTarget":                  scanTarget,
		"cacheTargetIndex":            cacheTargetIndex,
		"enableFileMapping":           enableFileMapping,
		"useLegacyWrite":              useLegacyWrite,
	})
	log.Info(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	if sourceFilePath != "" {
		sourceFilePaths = []string{sourceFilePath}
	}

	if len(sourceFilePaths) < 1 {
		err := fmt.Errorf("please provide at least one source path uri")
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	if sourceFilePaths[0] == "" {
		err := fmt.Errorf("please provide at least one source path uri")
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	pathFilter, err := longtailutils.MakeRegexPathFilter(includeFilterRegEx, excludeFilterRegEx)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	resolvedTargetFolderPath := ""
	if targetFolderPath == "" {
		normalizedSourceFilePath := longtailstorelib.NormalizeFileSystemPath(sourceFilePaths[0])
		normalizedSourceFilePath = strings.ReplaceAll(normalizedSourceFilePath, "\\", "/")
		urlSplit := strings.Split(normalizedSourceFilePath, "/")
		sourceName := urlSplit[len(urlSplit)-1]
		sourceNameSplit := strings.Split(sourceName, ".")
		resolvedTargetFolderPath = sourceNameSplit[0]
		if resolvedTargetFolderPath == "" {
			err = fmt.Errorf("unable to resolve target path using `%s` as base", sourceFilePaths[0])
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
	} else {
		resolvedTargetFolderPath = targetFolderPath
	}

	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()

	if targetIndexPath != "" {
		cacheTargetIndex = false
	}

	cacheTargetIndexPath := longtailstorelib.NormalizeFileSystemPath(resolvedTargetFolderPath + "/.longtail.index.cache.lvi")

	if cacheTargetIndex {
		if longtaillib.FileExists(fs, cacheTargetIndexPath) {
			targetIndexPath = cacheTargetIndexPath
		}
	}

	targetFolderScanner := longtailutils.AsyncFolderScanner{}
	if scanTarget && targetIndexPath == "" {
		targetFolderScanner.Scan(resolvedTargetFolderPath, pathFilter, fs, jobs)
	}

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	readSourceStartTime := time.Now()

	var sourceVersionIndex longtaillib.Longtail_VersionIndex
	for index, sourceFilePath := range sourceFilePaths {
		oneVersionIndex, err := readVersionIndex(sourceFilePath, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
		if err != nil {
			err = errors.Wrapf(err, "Cant read version index from `%s`", sourceFilePath)
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
		if index == 0 {
			sourceVersionIndex = oneVersionIndex
			continue
		}
		mergedVersionIndex, err := longtaillib.MergeVersionIndex(sourceVersionIndex, oneVersionIndex)
		if err != nil {
			sourceVersionIndex.Dispose()
			oneVersionIndex.Dispose()
			err = errors.Wrapf(err, "Cant mnerge version index from `%s`", sourceFilePath)
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
		sourceVersionIndex.Dispose()
		oneVersionIndex.Dispose()
		sourceVersionIndex = mergedVersionIndex
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
		enableFileMapping,
		&targetFolderScanner)

	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	localFS := longtaillib.CreateFSStorageAPI()
	defer localFS.Dispose()

	if versionLocalStoreIndexPath != "" {
		versionLocalStoreIndexPaths = append([]string{versionLocalStoreIndexPath}, versionLocalStoreIndexPaths...)
	}

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	remoteIndexStore, err := remotestore.CreateBlockStoreForURI(blobStoreURI, versionLocalStoreIndexPaths, jobs, numWorkerCount, 8388608, 1024, remotestore.ReadOnly, enableFileMapping, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
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
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, longtailstorelib.NormalizeFileSystemPath(localCachePath), "", enableFileMapping)

		cacheBlockStore = longtaillib.CreateCacheBlockStore(jobs, localIndexStore, remoteIndexStore)

		compressBlockStore = longtaillib.CreateCompressBlockStore(cacheBlockStore, creg)
	}

	defer cacheBlockStore.Dispose()
	defer localIndexStore.Dispose()
	defer compressBlockStore.Dispose()

	var indexStore longtaillib.Longtail_BlockStoreAPI
	var lruBlockStore longtaillib.Longtail_BlockStoreAPI
	if useLegacyWrite {
		lruBlockStore = longtaillib.CreateLRUBlockStoreAPI(compressBlockStore, 32)
		indexStore = longtaillib.CreateShareBlockStore(lruBlockStore)
	} else {
		indexStore = longtaillib.CreateShareBlockStore(compressBlockStore)
	}
	defer lruBlockStore.Dispose()
	defer indexStore.Dispose()

	hash, err := hashRegistry.GetHashAPI(hashIdentifier)
	if err != nil {
		err = errors.Wrapf(err, "Unsupported hash identifier `%d`", hashIdentifier)
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
	versionDiff, err := longtaillib.CreateVersionDiff(
		hash,
		targetVersionIndex,
		sourceVersionIndex)
	if err != nil {
		err = errors.Wrapf(err, "Failed to create version diff. `%s` -> `%s`", targetFolderPath, sourceFilePaths[0])
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer versionDiff.Dispose()

	chunkHashes, err := longtaillib.GetRequiredChunkHashes(
		sourceVersionIndex,
		versionDiff)
	if err != nil {
		err = errors.Wrapf(err, "Failed to get required chunk hashes. `%s` -> `%s`", targetFolderPath, sourceFilePaths[0])
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	retargettedVersionStoreIndex, err := longtailutils.GetExistingStoreIndexSync(indexStore, chunkHashes, 0)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer retargettedVersionStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get content index", getExistingContentTime})

	if cacheTargetIndex && longtaillib.FileExists(fs, cacheTargetIndexPath) {
		err = longtaillib.DeleteFile(fs, cacheTargetIndexPath)
		if err != nil {
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
	}

	changeVersionStartTime := time.Now()
	changeVersionProgress := longtailutils.CreateProgress("Updating version          ", 1)
	defer changeVersionProgress.Dispose()
	concurrentChunkWriteAPI := longtaillib.CreateConcurrentChunkWriteAPI(fs, sourceVersionIndex, versionDiff, longtailstorelib.NormalizeFileSystemPath(resolvedTargetFolderPath))
	defer concurrentChunkWriteAPI.Dispose()

	if useLegacyWrite {
		err = longtaillib.ChangeVersion(
			indexStore,
			fs,
			hash,
			jobs,
			&changeVersionProgress,
			retargettedVersionStoreIndex,
			targetVersionIndex,
			sourceVersionIndex,
			versionDiff,
			longtailstorelib.NormalizeFileSystemPath(resolvedTargetFolderPath),
			retainPermissions)
	} else {
		err = longtaillib.ChangeVersion2(
			indexStore,
			fs,
			concurrentChunkWriteAPI,
			hash,
			jobs,
			&changeVersionProgress,
			retargettedVersionStoreIndex,
			targetVersionIndex,
			sourceVersionIndex,
			versionDiff,
			longtailstorelib.NormalizeFileSystemPath(resolvedTargetFolderPath),
			retainPermissions)
	}

	if err != nil {
		err = errors.Wrapf(err, "Failed writing version `%s` to `%s`", sourceFilePaths[0], targetFolderPath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	changeVersionTime := time.Since(changeVersionStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Change version", changeVersionTime})

	flushStartTime := time.Now()

	var stores []longtaillib.Longtail_BlockStoreAPI
	if useLegacyWrite {
		stores = []longtaillib.Longtail_BlockStoreAPI{
			indexStore,
			lruBlockStore,
			compressBlockStore,
			cacheBlockStore,
			localIndexStore,
			remoteIndexStore,
		}
	} else {
		stores = []longtaillib.Longtail_BlockStoreAPI{
			indexStore,
			compressBlockStore,
			cacheBlockStore,
			localIndexStore,
			remoteIndexStore,
		}
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
	if lruBlockStore.IsValid() {
		lruStoreStats, err := lruBlockStore.GetStats()
		if err == nil {
			storeStats = append(storeStats, longtailutils.StoreStat{"LRU", lruStoreStats})
		}
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

	if validate {
		validateStartTime := time.Now()
		validateFileInfos, err := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			longtailstorelib.NormalizeFileSystemPath(resolvedTargetFolderPath))
		if err != nil {
			err = errors.Wrapf(err, "Failed to scan `%s`", resolvedTargetFolderPath)
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
		defer validateFileInfos.Dispose()

		chunker := longtaillib.CreateHPCDCChunkerAPI()
		defer chunker.Dispose()

		createVersionIndexProgress := longtailutils.CreateProgress("Validating version        ", 1)
		defer createVersionIndexProgress.Dispose()
		validateVersionIndex, err := longtaillib.CreateVersionIndex(
			fs,
			hash,
			chunker,
			jobs,
			&createVersionIndexProgress,
			longtailstorelib.NormalizeFileSystemPath(resolvedTargetFolderPath),
			validateFileInfos,
			nil,
			targetChunkSize,
			enableFileMapping)
		if err != nil {
			err = errors.Wrapf(err, "Failed to create version index for `%s`", resolvedTargetFolderPath)
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

	if cacheTargetIndex {
		err = longtaillib.WriteVersionIndex(fs, sourceVersionIndex, cacheTargetIndexPath)
		if err != nil {
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
	}

	return storeStats, timeStats, nil
}

type DownsyncCmd struct {
	StorageURIOption
	S3EndpointResolverURLOption
	SourceUriOption
	MultiSourceUrisOption
	TargetPathOption
	TargetIndexUriOption
	CachePathOption
	RetainPermissionsOption
	ValidateTargetOption
	VersionLocalStoreIndexPathOption
	MultiVersionLocalStoreIndexPathsOption
	TargetPathIncludeRegExOption
	TargetPathExcludeRegExOption
	ScanTargetOption
	CacheTargetIndexOption
	EnableFileMappingOption
	UseLegacyWriteOption
}

func (r *DownsyncCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := downsync(
		ctx.NumWorkerCount,
		r.StorageURI,
		r.S3EndpointResolverURL,
		r.SourcePath,
		r.SourcePaths,
		r.TargetPath,
		r.TargetIndexPath,
		r.CachePath,
		r.RetainPermissions,
		r.Validate,
		r.VersionLocalStoreIndexPath,
		r.VersionLocalStoreIndexPaths,
		r.IncludeFilterRegEx,
		r.ExcludeFilterRegEx,
		r.ScanTarget,
		r.CacheTargetIndex,
		r.EnableFileMapping,
		r.UseLegacyWrite)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
