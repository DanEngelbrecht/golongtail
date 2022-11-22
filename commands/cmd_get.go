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
	getConfigPath string,
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
	enableFileMapping bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "get"
	log := logrus.WithFields(logrus.Fields{
		"fname":                 fname,
		"numWorkerCount":        numWorkerCount,
		"getConfigPath":         getConfigPath,
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
	})
	log.Info(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	readGetConfigStartTime := time.Now()

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

	blobStoreURI := v.GetString("storage-uri")
	if blobStoreURI == "" {
		err = fmt.Errorf("missing storage-uri in get-config `%s`", getConfigPath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	sourceFilePath := v.GetString("source-path")
	if sourceFilePath == "" {
		err = fmt.Errorf("missing source-path in get-config `%s`", getConfigPath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	var versionLocalStoreIndexPaths []string
	if v.IsSet("version-local-store-index-path") {
		path := v.GetString("version-local-store-index-path")
		if path != "" {
			versionLocalStoreIndexPaths = append(versionLocalStoreIndexPaths, path)
		}
	}

	readGetConfigTime := time.Since(readGetConfigStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read get config", readGetConfigTime})

	downSyncStoreStats, downSyncTimeStats, err := downsync(
		numWorkerCount,
		blobStoreURI,
		s3EndpointResolverURI,
		sourceFilePath,
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
		enableFileMapping)

	storeStats = append(storeStats, downSyncStoreStats...)
	timeStats = append(timeStats, downSyncTimeStats...)

	return storeStats, timeStats, errors.Wrap(err, fname)
}

type GetCmd struct {
	GetConfigURI string `name:"source-path" help:"File uri for json formatted get-config file" required:""`
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
}

func (r *GetCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := get(
		ctx.NumWorkerCount,
		r.GetConfigURI,
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
		r.EnableFileMapping)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
