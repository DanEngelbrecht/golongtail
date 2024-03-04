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

func upsync(
	numWorkerCount int,
	blobStoreURI string,
	s3EndpointResolverURI string,
	sourceFolderPath string,
	sourceIndexPath string,
	targetFilePath string,
	targetChunkSize uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	compressionAlgorithm string,
	hashAlgorithm string,
	includeFilterRegEx string,
	excludeFilterRegEx string,
	minBlockUsagePercent uint32,
	versionLocalStoreIndexPath string,
	enableFileMapping bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "upsync"
	log := logrus.WithContext(context.Background()).WithFields(logrus.Fields{
		"fname":                      fname,
		"numWorkerCount":             numWorkerCount,
		"blobStoreURI":               blobStoreURI,
		"s3EndpointResolverURI":      s3EndpointResolverURI,
		"sourceFolderPath":           sourceFolderPath,
		"sourceIndexPath":            sourceIndexPath,
		"targetFilePath":             targetFilePath,
		"targetChunkSize":            targetChunkSize,
		"targetBlockSize":            targetBlockSize,
		"maxChunksPerBlock":          maxChunksPerBlock,
		"compressionAlgorithm":       compressionAlgorithm,
		"hashAlgorithm":              hashAlgorithm,
		"includeFilterRegEx":         includeFilterRegEx,
		"excludeFilterRegEx":         excludeFilterRegEx,
		"minBlockUsagePercent":       minBlockUsagePercent,
		"versionLocalStoreIndexPath": versionLocalStoreIndexPath,
		"enableFileMapping":          enableFileMapping,
	})
	log.Info(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()
	pathFilter, err := longtailutils.MakeRegexPathFilter(includeFilterRegEx, excludeFilterRegEx)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}

	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	sourceFolderScanner := longtailutils.AsyncFolderScanner{}
	if sourceIndexPath == "" {
		sourceFolderScanner.Scan(sourceFolderPath, pathFilter, fs, jobs)
	}

	compressionType, err := longtailutils.GetCompressionType(compressionAlgorithm)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}
	hashIdentifier, err := longtailutils.GetHashIdentifier(hashAlgorithm)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	sourceIndexReader := longtailutils.AsyncVersionIndexReader{}
	sourceIndexReader.Read(sourceFolderPath,
		sourceIndexPath,
		targetChunkSize,
		compressionType,
		hashIdentifier,
		pathFilter,
		fs,
		jobs,
		hashRegistry,
		enableFileMapping,
		&sourceFolderScanner)

	remoteStore, err := remotestore.CreateBlockStoreForURI(blobStoreURI, nil, jobs, numWorkerCount, targetBlockSize, maxChunksPerBlock, remotestore.ReadWrite, enableFileMapping, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}
	defer remoteStore.Dispose()

	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	indexStore := longtaillib.CreateCompressBlockStore(remoteStore, creg)
	defer indexStore.Dispose()

	vindex, hash, readSourceIndexTime, err := sourceIndexReader.Get()
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}
	defer vindex.Dispose()
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceIndexTime})

	getMissingContentStartTime := time.Now()
	existingRemoteStoreIndex, err := longtailutils.GetExistingStoreIndexSync(indexStore, vindex.GetChunkHashes(), minBlockUsagePercent)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}
	defer existingRemoteStoreIndex.Dispose()

	versionMissingStoreIndex, err := longtaillib.CreateMissingContent(
		hash,
		existingRemoteStoreIndex,
		vindex,
		targetBlockSize,
		maxChunksPerBlock)
	if err != nil {
		err = errors.Wrapf(err, "Failed creating missing content store index for `%s`", sourceFolderPath)
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}
	defer versionMissingStoreIndex.Dispose()

	getMissingContentTime := time.Since(getMissingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get content index", getMissingContentTime})

	writeContentStartTime := time.Now()
	if versionMissingStoreIndex.GetBlockCount() > 0 {
		writeContentProgress := longtailutils.CreateProgress("Writing content blocks    ", 1)
		defer writeContentProgress.Dispose()

		err = longtaillib.WriteContent(
			fs,
			indexStore,
			jobs,
			&writeContentProgress,
			versionMissingStoreIndex,
			vindex,
			longtailstorelib.NormalizeFileSystemPath(sourceFolderPath))
		if err != nil {
			err = errors.Wrapf(err, "Failed writing content from `%s`", sourceFolderPath)
			return storeStats, timeStats, errors.Wrapf(err, fname)
		}
	}
	writeContentTime := time.Since(writeContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Write version content", writeContentTime})

	flushStartTime := time.Now()

	stores := []longtaillib.Longtail_BlockStoreAPI{
		indexStore,
		remoteStore,
	}
	err = longtailutils.FlushStoresSync(stores)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Flush", flushTime})

	indexStoreStats, err := indexStore.GetStats()
	if err == nil {
		storeStats = append(storeStats, longtailutils.StoreStat{"Compress", indexStoreStats})
	}
	remoteStoreStats, err := remoteStore.GetStats()
	if err == nil {
		storeStats = append(storeStats, longtailutils.StoreStat{"Remote", remoteStoreStats})
	}

	writeVersionIndexStartTime := time.Now()
	vbuffer, err := longtaillib.WriteVersionIndexToBuffer(vindex)
	if err != nil {
		err = errors.Wrapf(err, "Failed serializing version index for `%s`", targetFilePath)
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}
	defer vbuffer.Dispose()

	err = longtailutils.WriteToURI(targetFilePath, vbuffer.ToBuffer(), longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}
	writeVersionIndexTime := time.Since(writeVersionIndexStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Write version index", writeVersionIndexTime})

	if versionLocalStoreIndexPath != "" {
		writeVersionLocalStoreIndexStartTime := time.Now()
		versionLocalStoreIndex, err := longtaillib.MergeStoreIndex(existingRemoteStoreIndex, versionMissingStoreIndex)
		if err != nil {
			err = errors.Wrapf(err, "Failed merging store index for `%s`", versionLocalStoreIndexPath)
			return storeStats, timeStats, errors.Wrapf(err, fname)
		}
		defer versionLocalStoreIndex.Dispose()
		versionLocalStoreIndexBuffer, err := longtaillib.WriteStoreIndexToBuffer(versionLocalStoreIndex)
		defer versionLocalStoreIndexBuffer.Dispose()
		if err != nil {
			err = errors.Wrapf(err, "Failed serializing store index for `%s`", versionLocalStoreIndexPath)
			return storeStats, timeStats, errors.Wrapf(err, fname)
		}
		err = longtailutils.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer.ToBuffer(), longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, fname)
		}
		writeVersionLocalStoreIndexTime := time.Since(writeVersionLocalStoreIndexStartTime)
		timeStats = append(timeStats, longtailutils.TimeStat{"Write version store index", writeVersionLocalStoreIndexTime})
	}

	return storeStats, timeStats, nil
}

type UpsyncCmd struct {
	SourcePath                 string `name:"source-path" help:"Source folder path" required:""`
	SourceIndexPath            string `name:"source-index-path" help:"Optional pre-computed index of source-path"`
	TargetPath                 string `name:"target-path" help:"Target file uri" required:""`
	VersionLocalStoreIndexPath string `name:"version-local-store-index-path" help:"Target file uri for a store index optimized for this particular version"`
	TargetChunkSizeOption
	MaxChunksPerBlockOption
	TargetBlockSizeOption
	MinBlockUsagePercentOption
	StorageURIOption
	S3EndpointResolverURLOption
	CompressionOption
	HashingOption
	SourcePathIncludeRegExOption
	SourcePathExcludeRegExOption
	EnableFileMappingOption
}

func (r *UpsyncCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := upsync(
		ctx.NumWorkerCount,
		r.StorageURI,
		r.S3EndpointResolverURL,
		r.SourcePath,
		r.SourceIndexPath,
		r.TargetPath,
		r.TargetChunkSize,
		r.TargetBlockSize,
		r.MaxChunksPerBlock,
		r.Compression,
		r.Hashing,
		r.IncludeFilterRegEx,
		r.ExcludeFilterRegEx,
		r.MinBlockUsagePercent,
		r.VersionLocalStoreIndexPath,
		r.EnableFileMapping)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
