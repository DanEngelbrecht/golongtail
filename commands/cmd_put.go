package commands

import (
	"context"
	"strings"

	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func put(
	numWorkerCount int,
	blobStoreURI string,
	sourceFolderPath string,
	sourceIndexPath string,
	targetIndexFilePath string,
	targetChunkSize uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	compressionAlgorithm string,
	hashAlgorithm string,
	includeFilterRegEx string,
	excludeFilterRegEx string,
	minBlockUsagePercent uint32,
	versionLocalStoreIndexPath string,
	targetPath string,
	enableFileMapping bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "put"
	log := logrus.WithContext(context.Background()).WithFields(logrus.Fields{
		"fname":                      fname,
		"numWorkerCount":             numWorkerCount,
		"blobStoreURI":               blobStoreURI,
		"sourceFolderPath":           sourceFolderPath,
		"sourceIndexPath":            sourceIndexPath,
		"targetIndexFilePath":        targetIndexFilePath,
		"targetChunkSize":            targetChunkSize,
		"targetBlockSize":            targetBlockSize,
		"maxChunksPerBlock":          maxChunksPerBlock,
		"compressionAlgorithm":       compressionAlgorithm,
		"hashAlgorithm":              hashAlgorithm,
		"includeFilterRegEx":         includeFilterRegEx,
		"excludeFilterRegEx":         excludeFilterRegEx,
		"minBlockUsagePercent":       minBlockUsagePercent,
		"versionLocalStoreIndexPath": versionLocalStoreIndexPath,
		"targetPath":                 targetPath,
		"enableFileMapping":          enableFileMapping,
	})
	log.Debug(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	parentPath := ""
	pathDelimiter := strings.LastIndexAny(targetPath, "\\/")
	if pathDelimiter != -1 {
		parentPath = targetPath[0:pathDelimiter]
	}
	configPathName := targetPath
	extensionDelimiter := strings.LastIndexAny(targetPath, ".")
	if extensionDelimiter != -1 {
		configPathName = configPathName[0:extensionDelimiter]
	}

	if blobStoreURI == "" {
		blobStoreURI = parentPath + "/store"
	}

	if targetIndexFilePath == "" {
		targetIndexFilePath = configPathName + ".lvi"
	}

	if versionLocalStoreIndexPath == "" {
		versionLocalStoreIndexPath = configPathName + ".lsi"
	}

	downSyncStoreStats, downSyncTimeStats, err := upsync(
		numWorkerCount,
		blobStoreURI,
		sourceFolderPath,
		sourceIndexPath,
		targetIndexFilePath,
		targetChunkSize,
		targetBlockSize,
		maxChunksPerBlock,
		compressionAlgorithm,
		hashAlgorithm,
		includeFilterRegEx,
		excludeFilterRegEx,
		minBlockUsagePercent,
		versionLocalStoreIndexPath,
		targetPath,
		enableFileMapping)

	storeStats = append(storeStats, downSyncStoreStats...)
	timeStats = append(timeStats, downSyncTimeStats...)

	return storeStats, timeStats, errors.Wrap(err, fname)
}

type PutCmd struct {
	GetConfigURI               string `name:"target-path" help:"File uri for json formatted get-config file" required:""`
	TargetFileIndexPath        string `name:"target-index-path" help:"Target index file uri"`
	VersionLocalStoreIndexPath string `name:"version-local-store-index-path" help:"Target file uri for a store index optimized for this particular version"`
	OptionalStorageURI         string `name:"storage-uri" help"Storage URI (local file system, GCS and S3 bucket URI supported)"`
	SourcePath                 string `name:"source-path" help:"Source folder path" required:""`
	SourceIndexPath            string `name:"source-index-path" help:"Optional pre-computed index of source-path"`
	TargetChunkSizeOption
	TargetBlockSizeOption
	MaxChunksPerBlockOption
	CompressionOption
	HashingOption
	SourcePathIncludeRegExOption
	SourcePathExcludeRegExOption
	MinBlockUsagePercentOption

	EnableFileMappingOption
}

func (r *PutCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := put(
		ctx.NumWorkerCount,
		r.OptionalStorageURI,
		r.SourcePath,
		r.SourceIndexPath,
		r.TargetFileIndexPath,
		r.TargetChunkSize,
		r.TargetBlockSize,
		r.MaxChunksPerBlock,
		r.Compression,
		r.Hashing,
		r.IncludeFilterRegEx,
		r.ExcludeFilterRegEx,
		r.MinBlockUsagePercent,
		r.VersionLocalStoreIndexPath,
		r.GetConfigURI,
		r.EnableFileMapping)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
