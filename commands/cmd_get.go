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
	targetFolderPath string,
	targetIndexPath string,
	localCachePath string,
	retainPermissions bool,
	validate bool,
	includeFilterRegEx string,
	excludeFilterRegEx string,
	scanTarget bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "get"
	log := logrus.WithFields(logrus.Fields{
		"fname":              fname,
		"numWorkerCount":     numWorkerCount,
		"getConfigPath":      getConfigPath,
		"targetFolderPath":   targetFolderPath,
		"targetIndexPath":    targetIndexPath,
		"localCachePath":     localCachePath,
		"retainPermissions":  retainPermissions,
		"validate":           validate,
		"includeFilterRegEx": includeFilterRegEx,
		"excludeFilterRegEx": excludeFilterRegEx,
		"scanTarget":         scanTarget,
	})
	log.Debug(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	readGetConfigStartTime := time.Now()

	vbuffer, err := longtailutils.ReadFromURI(getConfigPath)
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
		err = fmt.Errorf("Missing storage-uri in get-config `%s`", getConfigPath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	sourceFilePath := v.GetString("source-path")
	if sourceFilePath == "" {
		err = fmt.Errorf("missing source-path in get-config `%s`", getConfigPath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	var versionLocalStoreIndexPath string
	if v.IsSet("version-local-store-index-path") {
		versionLocalStoreIndexPath = v.GetString("version-local-store-index-path")
	}

	readGetConfigTime := time.Since(readGetConfigStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read get config", readGetConfigTime})

	downSyncStoreStats, downSyncTimeStats, err := downsync(
		numWorkerCount,
		blobStoreURI,
		sourceFilePath,
		targetFolderPath,
		targetIndexPath,
		localCachePath,
		retainPermissions,
		validate,
		versionLocalStoreIndexPath,
		includeFilterRegEx,
		excludeFilterRegEx,
		scanTarget)

	storeStats = append(storeStats, downSyncStoreStats...)
	timeStats = append(timeStats, downSyncTimeStats...)

	return storeStats, timeStats, errors.Wrap(err, fname)
}

type GetCmd struct {
	GetConfigURI string `name:"get-config-path" help:"File uri for json formatted get-config file" required:""`
	TargetPathOption
	TargetIndexUriOption
	ValidateTargetOption
	VersionLocalStoreIndexPathOption
	CachePathOption
	RetainPermissionsOption
	TargetPathIncludeRegExOption
	TargetPathExcludeRegExOption
	ScanTargetOption
}

func (r *GetCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := get(
		ctx.NumWorkerCount,
		r.GetConfigURI,
		r.TargetPath,
		r.TargetIndexPath,
		r.CachePath,
		r.RetainPermissions,
		r.Validate,
		r.IncludeFilterRegEx,
		r.ExcludeFilterRegEx,
		r.ScanTarget)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
