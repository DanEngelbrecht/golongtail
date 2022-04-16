package commands

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func unpack(
	numWorkerCount int,
	sourceFilePath string,
	targetFolderPath string,
	targetIndexPath string,
	retainPermissions bool,
	validate bool,
	includeFilterRegEx string,
	excludeFilterRegEx string,
	scanTarget bool,
	cacheTargetIndex bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "unpack"
	log := logrus.WithContext(context.Background()).WithFields(logrus.Fields{
		"fname":              fname,
		"numWorkerCount":     numWorkerCount,
		"sourceFilePath":     sourceFilePath,
		"targetFolderPath":   targetFolderPath,
		"targetIndexPath":    targetIndexPath,
		"retainPermissions":  retainPermissions,
		"validate":           validate,
		"includeFilterRegEx": includeFilterRegEx,
		"excludeFilterRegEx": excludeFilterRegEx,
		"scanTarget":         scanTarget,
		"cacheTargetIndex":   cacheTargetIndex,
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
			err = fmt.Errorf("unable to resolve target path using `%s` as base", sourceFilePath)
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

	cacheTargetIndexPath := resolvedTargetFolderPath + "/.longtail.index.cache.lvi"

	if cacheTargetIndex {
		if longtaillib.FileExists(fs, cacheTargetIndexPath) {
			targetIndexPath = cacheTargetIndexPath
		}
	}

	targetFolderScanner := longtailutils.AsyncFolderScanner{}
	if scanTarget && targetIndexPath == "" {
		targetFolderScanner.Scan(resolvedTargetFolderPath, pathFilter, fs)
	}

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	readSourceStartTime := time.Now()

	archiveIndex, err := longtaillib.ReadArchiveIndex(fs, sourceFilePath)
	if err != nil {
		err = errors.Wrapf(err, "Cant read archive index from `%s`", sourceFilePath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	sourceVersionIndex := archiveIndex.GetVersionIndex()

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

	archiveIndexBlockStore := longtaillib.CreateArchiveBlockStoreAPI(fs, sourceFilePath, archiveIndex, false)
	defer archiveIndexBlockStore.Dispose()

	compressBlockStore := longtaillib.CreateCompressBlockStore(archiveIndexBlockStore, creg)
	defer compressBlockStore.Dispose()

	lruBlockStore := longtaillib.CreateLRUBlockStoreAPI(compressBlockStore, 32)
	defer lruBlockStore.Dispose()

	shareBlockStore := longtaillib.CreateShareBlockStore(lruBlockStore)
	defer shareBlockStore.Dispose()

	indexStore := shareBlockStore

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

	getVersionDiffStartTime := time.Now()

	versionDiff, err := longtaillib.CreateVersionDiff(
		hash,
		targetVersionIndex,
		sourceVersionIndex)
	if err != nil {
		err = errors.Wrapf(err, "Failed to create version diff. `%s` -> `%s`", targetFolderPath, sourceFilePath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer versionDiff.Dispose()
	getVersionDiffTime := time.Since(getVersionDiffStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get diff", getVersionDiffTime})

	getExistingContentStartTime := time.Now()
	chunkHashes, err := longtaillib.GetRequiredChunkHashes(
		sourceVersionIndex,
		versionDiff)
	if err != nil {
		err = errors.Wrapf(err, "Failed to get required chunk hashes. `%s` -> `%s`", targetFolderPath, sourceFilePath)
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
		longtailutils.NormalizePath(resolvedTargetFolderPath),
		retainPermissions)
	if err != nil {
		err = errors.Wrapf(err, "Failed writing version `%s` to `%s`", sourceFilePath, targetFolderPath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	changeVersionTime := time.Since(changeVersionStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Change version", changeVersionTime})

	flushStartTime := time.Now()

	stores := []longtaillib.Longtail_BlockStoreAPI{
		shareBlockStore,
		lruBlockStore,
		compressBlockStore,
		archiveIndexBlockStore,
	}
	err = longtailutils.FlushStoresSync(stores)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Flush", flushTime})

	shareStoreStats, err := shareBlockStore.GetStats()
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
	archiveIndexStoreStats, err := archiveIndexBlockStore.GetStats()
	if err == nil {
		storeStats = append(storeStats, longtailutils.StoreStat{"Archive", archiveIndexStoreStats})
	}

	if validate {
		validateStartTime := time.Now()
		validateFileInfos, err := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			longtailutils.NormalizePath(resolvedTargetFolderPath))
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
			longtailutils.NormalizePath(resolvedTargetFolderPath),
			validateFileInfos,
			nil,
			targetChunkSize)
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

type UnpackCmd struct {
	SourcePath      string `name:"source-path" help:"Source folder path" required:""`
	TargetPath      string `name:"target-path" help:"Target file uri"`
	TargetIndexPath string `name:"target-index-path" help:"Optional pre-computed index of target-path"`
	RetainPermissionsOption
	ValidateTargetOption
	TargetPathIncludeRegExOption
	TargetPathExcludeRegExOption
	ScanTargetOption
	CacheTargetIndexOption
}

func (r *UnpackCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := unpack(
		ctx.NumWorkerCount,
		r.SourcePath,
		r.TargetPath,
		r.TargetIndexPath,
		r.RetainPermissions,
		r.Validate,
		r.IncludeFilterRegEx,
		r.ExcludeFilterRegEx,
		r.ScanTarget,
		r.CacheTargetIndex)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
