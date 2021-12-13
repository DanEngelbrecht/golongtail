package commands

import (
	"context"
	"io/ioutil"
	"os"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/DanEngelbrecht/golongtail/remotestore"
	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"github.com/sirupsen/logrus"
)

func upsync(
	numWorkerCount int,
	blobStoreURI string,
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
	getConfigPath string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "upsync"
	log := logrus.WithContext(context.Background()).WithFields(logrus.Fields{
		"fname":                      fname,
		"numWorkerCount":             numWorkerCount,
		"blobStoreURI":               blobStoreURI,
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
		"getConfigPath":              getConfigPath,
	})
	log.Debug(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()
	pathFilter, err := longtailutils.MakeRegexPathFilter(includeFilterRegEx, excludeFilterRegEx)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}

	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()

	sourceFolderScanner := longtailutils.AsyncFolderScanner{}
	if sourceIndexPath == "" {
		sourceFolderScanner.Scan(sourceFolderPath, pathFilter, fs)
	}

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

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
		&sourceFolderScanner)

	remoteStore, err := remotestore.CreateBlockStoreForURI(blobStoreURI, "", jobs, numWorkerCount, remotestore.ReadWrite)
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
		writeContentProgress := longtailutils.CreateProgress("Writing content blocks", 2)
		defer writeContentProgress.Dispose()

		err = longtaillib.WriteContent(
			fs,
			indexStore,
			jobs,
			&writeContentProgress,
			versionMissingStoreIndex,
			vindex,
			longtailutils.NormalizePath(sourceFolderPath))
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

	err = longtailutils.WriteToURI(targetFilePath, vbuffer)
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
		if err != nil {
			err = errors.Wrapf(err, "Failed serializing store index for `%s`", versionLocalStoreIndexPath)
			return storeStats, timeStats, errors.Wrapf(err, fname)
		}
		err = longtailutils.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer)
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, fname)
		}
		writeVersionLocalStoreIndexTime := time.Since(writeVersionLocalStoreIndexStartTime)
		timeStats = append(timeStats, longtailutils.TimeStat{"Write version store index", writeVersionLocalStoreIndexTime})
	}

	if getConfigPath != "" {
		writeGetConfigStartTime := time.Now()

		v := viper.New()
		v.SetConfigType("json")
		v.Set("storage-uri", blobStoreURI)
		v.Set("source-path", targetFilePath)
		if versionLocalStoreIndexPath != "" {
			v.Set("version-local-store-index-path", versionLocalStoreIndexPath)
		}
		tmpFile, err := ioutil.TempFile(os.TempDir(), "longtail-")
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, fname)
		}
		tmpFilePath := tmpFile.Name()
		tmpFile.Close()
		log.WithField(tmpFilePath, "tmpFilePath").Debug("Writing get config temp file")
		err = v.WriteConfigAs(tmpFilePath)
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, fname)
		}

		bytes, err := ioutil.ReadFile(tmpFilePath)
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, fname)
		}
		os.Remove(tmpFilePath)

		err = longtailutils.WriteToURI(getConfigPath, bytes)
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, fname)
		}

		writeGetConfigTime := time.Since(writeGetConfigStartTime)
		timeStats = append(timeStats, longtailutils.TimeStat{"Write get config", writeGetConfigTime})
	}

	return storeStats, timeStats, nil
}

type UpsyncCmd struct {
	SourcePath                 string `name:"source-path" help:"Source folder path" required:""`
	SourceIndexPath            string `name:"source-index-path" help:"Optional pre-computed index of source-path"`
	TargetPath                 string `name:"target-path" help:"Target file uri" required:""`
	VersionLocalStoreIndexPath string `name:"version-local-store-index-path" help:"Target file uri for a store index optimized for this particular version"`
	GetConfigPath              string `name:"get-config-path" help:"Target file uri for json formatted get-config file"`
	TargetChunkSizeOption
	MaxChunksPerBlockOption
	TargetBlockSizeOption
	MinBlockUsagePercentOption
	StorageURIOption
	CompressionOption
	HashingOption
	SourcePathIncludeRegExOption
	SourcePathExcludeRegExOption
}

func (r *UpsyncCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := upsync(
		ctx.NumWorkerCount,
		r.StorageURI,
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
		r.GetConfigPath)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
