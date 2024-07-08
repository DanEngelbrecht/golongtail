package commands

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func put(
	numWorkerCount int,
	numRemoteWorkerCount int,
	blobStoreURI string,
	s3EndpointResolverURI string,
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
	disableVersionLocalStoreIndex bool,
	enableFileMapping bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "put"
	log := logrus.WithContext(context.Background()).WithFields(logrus.Fields{
		"fname":                      fname,
		"numWorkerCount":             numWorkerCount,
		"numRemoteWorkerCount":       numRemoteWorkerCount,
		"blobStoreURI":               blobStoreURI,
		"s3EndpointResolverURI":      s3EndpointResolverURI,
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
	log.Info(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	targetName := targetPath
	parentPath := "."
	pathDelimiter := strings.LastIndexAny(targetPath, "\\/")
	if pathDelimiter != -1 {
		parentPath = targetPath[0:pathDelimiter]
		targetName = targetPath[pathDelimiter+1:]
	}
	configName := targetName
	extensionDelimiter := strings.LastIndexAny(configName, ".")
	if extensionDelimiter != -1 {
		configName = configName[0:extensionDelimiter]
	}

	if blobStoreURI == "" {
		blobStoreURI = parentPath + "/store"
	}

	if targetIndexFilePath == "" {
		targetIndexFilePath = parentPath + "/version-data/version-index/" + configName + ".lvi"
	}

	if versionLocalStoreIndexPath == "" {
		if !disableVersionLocalStoreIndex {
			versionLocalStoreIndexPath = parentPath + "/version-data/version-store-index/" + configName + ".lsi"
		}
	} else if disableVersionLocalStoreIndex {
		return storeStats, timeStats, fmt.Errorf("put: conflicting options for version local store index, --no-version-local-store-index is set together with path `%s`", versionLocalStoreIndexPath)
	}

	downSyncStoreStats, downSyncTimeStats, err := upsync(
		numWorkerCount,
		numRemoteWorkerCount,
		blobStoreURI,
		s3EndpointResolverURI,
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
		enableFileMapping)

	storeStats = append(storeStats, downSyncStoreStats...)
	timeStats = append(timeStats, downSyncTimeStats...)

	if err == nil {
		writeGetConfigStartTime := time.Now()

		v := viper.New()
		v.SetConfigType("json")
		v.Set("storage-uri", blobStoreURI)
		if s3EndpointResolverURI != "" {
			v.Set("s3-endpoint-resolver-uri", s3EndpointResolverURI)
		}
		v.Set("source-path", targetIndexFilePath)
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

		err = longtailutils.WriteToURI(targetPath, bytes, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, fname)
		}

		writeGetConfigTime := time.Since(writeGetConfigStartTime)
		timeStats = append(timeStats, longtailutils.TimeStat{"Write get config", writeGetConfigTime})

	}

	return storeStats, timeStats, errors.Wrap(err, fname)
}

type PutCmd struct {
	GetConfigURI               string `name:"target-path" help:"File uri for json formatted get-config file" required:""`
	TargetFileIndexPath        string `name:"target-version-index-path" help:"Target version index file uri"`
	VersionLocalStoreIndexPath string `name:"version-local-store-index-path" help:"Target file uri for a store index optimized for this particular version"`
	OptionalStorageURI         string `name:"storage-uri" help"Storage URI (local file system, GCS and S3 bucket URI supported)"`
	S3EndpointResolverURLOption
	SourcePath                    string `name:"source-path" help:"Source folder path" required:""`
	SourceIndexPath               string `name:"source-index-path" help:"Optional pre-computed index of source-path"`
	DisableVersionLocalStoreIndex bool   `name:"no-version-local-store-index" help:"Disable saving of store index optimized for this particular version"`
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
		ctx.NumRemoteWorkerCount,
		r.OptionalStorageURI,
		r.S3EndpointResolverURL,
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
		r.DisableVersionLocalStoreIndex,
		r.EnableFileMapping)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
