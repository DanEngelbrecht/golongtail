package commands

import (
	"bytes"
	"fmt"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func get(
	numWorkerCount int,
	getConfigPaths []string,
	s3EndpointResolverURI string,
	targetFolderPath string,
	targetIndexPath string,
	localCachePath string,
	retainPermissions bool,
	validate bool,
	includeFilterRegEx string,
	excludeFilterRegEx string,
	scanTarget bool,
	cacheTargetIndex bool,
	enableFileMapping bool,
	useLegacyWrite bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "get"
	log := logrus.WithFields(logrus.Fields{
		"fname":                 fname,
		"numWorkerCount":        numWorkerCount,
		"getConfigPaths":        getConfigPaths,
		"s3EndpointResolverURI": s3EndpointResolverURI,
		"targetFolderPath":      targetFolderPath,
		"targetIndexPath":       targetIndexPath,
		"localCachePath":        localCachePath,
		"retainPermissions":     retainPermissions,
		"validate":              validate,
		"includeFilterRegEx":    includeFilterRegEx,
		"excludeFilterRegEx":    excludeFilterRegEx,
		"scanTarget":            scanTarget,
		"cacheTargetIndex":      cacheTargetIndex,
		"enableFileMapping":     enableFileMapping,
		"useLegacyWrite":        useLegacyWrite,
	})
	log.Info(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	readGetConfigStartTime := time.Now()

	if len(getConfigPaths) < 1 {
		err := fmt.Errorf("source-path is missing")
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	blobStoreURI := ""
	var sourceFilePaths []string
	var versionLocalStoreIndexPaths []string
	for _, getConfigPath := range getConfigPaths {
		vbuffer, err := longtailutils.ReadFromURI(getConfigPath, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
		if err != nil {
			return storeStats, timeStats, errors.Wrap(err, fname)
		}

		v := viper.New()
		v.SetConfigType("json")
		err = v.ReadConfig(bytes.NewBuffer(vbuffer))
		if err != nil {
			return storeStats, timeStats, errors.Wrap(err, fname)
		}

		localBlobStoreURI := v.GetString("storage-uri")
		if localBlobStoreURI == "" {
			err = fmt.Errorf("missing storage-uri in get-config `%s`", getConfigPath)
			return storeStats, timeStats, errors.Wrap(err, fname)
		} else if blobStoreURI == "" {
			blobStoreURI = localBlobStoreURI
		} else if blobStoreURI != localBlobStoreURI {
			err = fmt.Errorf("storage-uri in get-config `%s` does not match inital storage-uri `%s`", getConfigPath, blobStoreURI)
			return storeStats, timeStats, errors.Wrap(err, fname)
		}

		sourceFilePath := v.GetString("source-path")
		if sourceFilePath == "" {
			err = fmt.Errorf("missing source-path in get-config `%s`", getConfigPath)
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
		sourceFilePaths = append(sourceFilePaths, sourceFilePath)

		if v.IsSet("version-local-store-index-path") {
			versionLocalStoreIndexPath := v.GetString("version-local-store-index-path")
			if versionLocalStoreIndexPath != "" {
				versionLocalStoreIndexPaths = append(versionLocalStoreIndexPaths, versionLocalStoreIndexPath)
			}
		}
	}

	if len(versionLocalStoreIndexPaths) != len(sourceFilePaths) {
		versionLocalStoreIndexPaths = []string{}
	}

	readGetConfigTime := time.Since(readGetConfigStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read get config", readGetConfigTime})

	downSyncStoreStats, downSyncTimeStats, err := downsync(
		numWorkerCount,
		blobStoreURI,
		s3EndpointResolverURI,
		sourceFilePaths,
		targetFolderPath,
		targetIndexPath,
		localCachePath,
		retainPermissions,
		validate,
		versionLocalStoreIndexPaths,
		includeFilterRegEx,
		excludeFilterRegEx,
		scanTarget,
		cacheTargetIndex,
		enableFileMapping,
		useLegacyWrite)

	storeStats = append(storeStats, downSyncStoreStats...)
	timeStats = append(timeStats, downSyncTimeStats...)

	return storeStats, timeStats, errors.Wrap(err, fname)
}

type GetCmd struct {
	GetConfigURIs []string `name:"source-path" help:"File uri(s) for json formatted get-config file" required:"" sep:" "`
	S3EndpointResolverURLOption
	TargetPathOption
	TargetIndexUriOption
	ValidateTargetOption
	VersionLocalStoreIndexPathOption
	CachePathOption
	RetainPermissionsOption
	TargetPathIncludeRegExOption
	TargetPathExcludeRegExOption
	ScanTargetOption
	CacheTargetIndexOption
	EnableFileMappingOption
	UseLegacyWriteOption
}

func (r *GetCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := get(
		ctx.NumWorkerCount,
		r.GetConfigURIs,
		r.S3EndpointResolverURL,
		r.TargetPath,
		r.TargetIndexPath,
		r.CachePath,
		r.RetainPermissions,
		r.Validate,
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
