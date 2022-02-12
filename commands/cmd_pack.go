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

func pack(
	numWorkerCount int,
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
	ctx *Context) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "pack"
	log := logrus.WithContext(context.Background()).WithFields(logrus.Fields{
		"fname":                fname,
		"numWorkerCount":       numWorkerCount,
		"sourceFolderPath":     sourceFolderPath,
		"sourceIndexPath":      sourceIndexPath,
		"targetFilePath":       targetFilePath,
		"targetChunkSize":      targetChunkSize,
		"targetBlockSize":      targetBlockSize,
		"maxChunksPerBlock":    maxChunksPerBlock,
		"compressionAlgorithm": compressionAlgorithm,
		"hashAlgorithm":        hashAlgorithm,
		"includeFilterRegEx":   includeFilterRegEx,
		"excludeFilterRegEx":   excludeFilterRegEx,
	})
	log.Debug(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()
	pathFilter, err := longtailutils.MakeRegexPathFilter(includeFilterRegEx, excludeFilterRegEx)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}

	resolvedTargetPath := ""
	if targetFilePath == "" {
		urlSplit := strings.Split(longtailutils.NormalizePath(sourceFolderPath), "/")
		sourceName := urlSplit[len(urlSplit)-1]
		sourceNameSplit := strings.Split(sourceName, ".")
		resolvedTargetPath = sourceNameSplit[0]
		if resolvedTargetPath == "" {
			err = fmt.Errorf("Unable to resolve target path using `%s` as base", sourceFolderPath)
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
		resolvedTargetPath += ".la"
	} else {
		resolvedTargetPath = targetFilePath
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

	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	vindex, hash, readSourceIndexTime, err := sourceIndexReader.Get()
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}
	defer vindex.Dispose()
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceIndexTime})

	createArchiveIndexStartTime := time.Now()
	storeIndex, err := longtaillib.CreateStoreIndex(
		hash,
		vindex,
		targetBlockSize,
		maxChunksPerBlock)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}
	defer storeIndex.Dispose()

	archiveIndex, err := longtaillib.CreateArchiveIndex(
		storeIndex,
		vindex)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}
	defer archiveIndex.Dispose()

	createArchiveIndexTime := time.Since(createArchiveIndexStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Create archive index", createArchiveIndexTime})

	archiveIndexBlockStore := longtaillib.CreateArchiveBlockStoreAPI(fs, resolvedTargetPath, archiveIndex, true)
	ctx.AddStore(&archiveIndexBlockStore)
	if !archiveIndexBlockStore.IsValid() {
		err = errors.Wrapf(err, "Failed creating archive store for `%s`", resolvedTargetPath)
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}
	defer archiveIndexBlockStore.Dispose()

	indexStore := longtaillib.CreateCompressBlockStore(archiveIndexBlockStore, creg)
	ctx.AddStore(&indexStore)
	defer indexStore.Dispose()

	defer ctx.RemoveStores()

	writeContentProgress := longtailutils.CreateProgress("Writing content blocks", 2)
	defer writeContentProgress.Dispose()

	writeContentStartTime := time.Now()
	err = longtaillib.WriteContent(
		fs,
		indexStore,
		jobs,
		&writeContentProgress,
		storeIndex,
		vindex,
		longtailutils.NormalizePath(sourceFolderPath))
	if err != nil {
		err = errors.Wrapf(err, "Failed writing content from `%s`", sourceFolderPath)
		return storeStats, timeStats, errors.Wrapf(err, fname)
	}
	writeContentTime := time.Since(writeContentStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Write version content", writeContentTime})

	flushStartTime := time.Now()
	stores := []longtaillib.Longtail_BlockStoreAPI{
		indexStore,
		archiveIndexBlockStore,
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
	archiveIndexBlockStoreStats, err := archiveIndexBlockStore.GetStats()
	if err == nil {
		storeStats = append(storeStats, longtailutils.StoreStat{"Archive", archiveIndexBlockStoreStats})
	}

	return storeStats, timeStats, nil
}

type PackCmd struct {
	SourcePath      string `name:"source-path" help:"Source folder path" required:""`
	SourceIndexPath string `name:"source-index-path" help:"Optional pre-computed index of source-path"`
	TargetPath      string `name:"target-path" help:"Target file uri"`
	TargetChunkSizeOption
	MaxChunksPerBlockOption
	TargetBlockSizeOption
	CompressionOption
	HashingOption
	SourcePathIncludeRegExOption
	SourcePathExcludeRegExOption
}

func (r *PackCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := pack(
		ctx.NumWorkerCount,
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
		ctx)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
