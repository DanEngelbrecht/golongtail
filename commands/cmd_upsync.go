package commands

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/DanEngelbrecht/golongtail/remotestore"
	"github.com/pkg/errors"
	"github.com/spf13/viper"

	log "github.com/sirupsen/logrus"
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
	log := log.WithContext(context.Background()).WithFields(log.Fields{
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
	log.Debug("upsync")

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()
	pathFilter, err := longtailutils.MakeRegexPathFilter(includeFilterRegEx, excludeFilterRegEx)
	if err != nil {
		log.WithError(err).Error("upsync")
		return storeStats, timeStats, errors.Wrapf(err, "upsync")
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
		log.WithError(err).Error("upsync")
		return storeStats, timeStats, errors.Wrapf(err, "upsync")
	}
	hashIdentifier, err := longtailutils.GetHashIdentifier(hashAlgorithm)
	if err != nil {
		log.WithError(err).Error("upsync")
		return storeStats, timeStats, errors.Wrapf(err, "upsync")
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

	remoteStore, err := remotestore.CreateBlockStoreForURI(blobStoreURI, "", jobs, numWorkerCount, targetBlockSize, maxChunksPerBlock, remotestore.ReadWrite)
	if err != nil {
		log.WithError(err).Error("upsync")
		return storeStats, timeStats, errors.Wrapf(err, "upsync")
	}
	defer remoteStore.Dispose()

	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	indexStore := longtaillib.CreateCompressBlockStore(remoteStore, creg)
	defer indexStore.Dispose()

	vindex, hash, readSourceIndexTime, err := sourceIndexReader.Get()
	if err != nil {
		log.WithError(err).Error("upsync")
		return storeStats, timeStats, errors.Wrapf(err, "upsync")
	}
	defer vindex.Dispose()
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceIndexTime})

	getMissingContentStartTime := time.Now()
	existingRemoteStoreIndex, err := longtailutils.GetExistingStoreIndexSync(indexStore, vindex.GetChunkHashes(), minBlockUsagePercent)
	if err != nil {
		log.WithError(err).Error("upsync")
		return storeStats, timeStats, errors.Wrapf(err, "upsync")
	}
	defer existingRemoteStoreIndex.Dispose()

	versionMissingStoreIndex, errno := longtaillib.CreateMissingContent(
		hash,
		existingRemoteStoreIndex,
		vindex,
		targetBlockSize,
		maxChunksPerBlock)
	if errno != 0 {
		err = longtailutils.MakeError(errno, "longtaillib.CreateMissingContent failed")
		log.WithError(err).Error("upsync")
		return storeStats, timeStats, err
	}
	defer versionMissingStoreIndex.Dispose()

	getMissingContentTime := time.Since(getMissingContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Get content index", getMissingContentTime})

	writeContentStartTime := time.Now()
	if versionMissingStoreIndex.GetBlockCount() > 0 {
		writeContentProgress := longtailutils.CreateProgress("Writing content blocks")
		defer writeContentProgress.Dispose()

		errno = longtaillib.WriteContent(
			fs,
			indexStore,
			jobs,
			&writeContentProgress,
			versionMissingStoreIndex,
			vindex,
			longtailutils.NormalizePath(sourceFolderPath))
		if errno != 0 {
			err = longtailutils.MakeError(errno, "longtaillib.WriteContent failed")
			log.WithError(err).Error("upsync")
			return storeStats, timeStats, errors.Wrapf(err, "upsync")
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
		log.WithError(err).Error("longtailutils.FlushStoresSync failed")
		log.WithError(err).Error("upsync")
		return storeStats, timeStats, err
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Flush", flushTime})

	indexStoreStats, errno := indexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Compress", indexStoreStats})
	}
	remoteStoreStats, errno := remoteStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, longtailutils.StoreStat{"Remote", remoteStoreStats})
	}

	writeVersionIndexStartTime := time.Now()
	vbuffer, errno := longtaillib.WriteVersionIndexToBuffer(vindex)
	if errno != 0 {
		err = longtailutils.MakeError(errno, "longtaillib.WriteVersionIndexToBuffer failed")
		log.WithError(err).Error("upsync")
		return storeStats, timeStats, err
	}

	err = longtailutils.WriteToURI(targetFilePath, vbuffer)
	if err != nil {
		err = longtailutils.MakeError(errno, fmt.Sprintf("longtailutils.WriteToURI failed for `%s`", targetFilePath))
		log.WithError(err).Error("upsync")
		return storeStats, timeStats, errors.Wrapf(err, "upsync")
	}
	writeVersionIndexTime := time.Since(writeVersionIndexStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Write version index", writeVersionIndexTime})

	if versionLocalStoreIndexPath != "" {
		writeVersionLocalStoreIndexStartTime := time.Now()
		versionLocalStoreIndex, errno := longtaillib.MergeStoreIndex(existingRemoteStoreIndex, versionMissingStoreIndex)
		if errno != 0 {
			err = longtailutils.MakeError(errno, "longtaillib.MergeStoreIndex failed")
			log.WithError(err).Error("upsync")
			return storeStats, timeStats, errors.Wrapf(err, "upsync")
		}
		defer versionLocalStoreIndex.Dispose()
		versionLocalStoreIndexBuffer, errno := longtaillib.WriteStoreIndexToBuffer(versionLocalStoreIndex)
		if errno != 0 {
			err = longtailutils.MakeError(errno, "longtaillib.WriteStoreIndexToBuffer failed")
			log.WithError(err).Error("upsync")
			return storeStats, timeStats, errors.Wrapf(err, "upsync")
		}
		err = longtailutils.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer)
		if err != nil {
			err = errors.Wrapf(err, "Failed writing version local store index to %s", versionLocalStoreIndexPath)
			log.WithError(err).Error("upsync")
			return storeStats, timeStats, errors.Wrapf(err, "upsync")
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
			err = errors.Wrap(err, "Failed get config temp file")
			log.WithError(err).Error("upsync")
			return storeStats, timeStats, errors.Wrapf(err, "upsync")
		}
		tmpFilePath := tmpFile.Name()
		tmpFile.Close()
		fmt.Printf("tmp file: %s", tmpFilePath)
		err = v.WriteConfigAs(tmpFilePath)
		if err != nil {
			err = errors.Wrapf(err, "Failed writing get config temp file %s", tmpFilePath)
			log.WithError(err).Error("upsync")
			return storeStats, timeStats, errors.Wrapf(err, "upsync")
		}

		bytes, err := ioutil.ReadFile(tmpFilePath)
		if err != nil {
			err = errors.Wrapf(err, "Failed read get config temp file %s", tmpFilePath)
			log.WithError(err).Error("upsync")
			return storeStats, timeStats, errors.Wrapf(err, "upsync")
		}
		os.Remove(tmpFilePath)

		err = longtailutils.WriteToURI(getConfigPath, bytes)
		if err != nil {
			err = errors.Wrapf(err, "Failed writing get config index to %s", getConfigPath)
			log.WithError(err).Error("upsync")
			return storeStats, timeStats, errors.Wrapf(err, "upsync")
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
	UpsyncIncludeRegExOption
	UpsyncExcludeRegExOption
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
