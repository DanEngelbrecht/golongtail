package main

import (
	"bytes"
	"fmt"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
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
	excludeFilterRegEx string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	readGetConfigStartTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(getConfigPath)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, "get: longtailstorelib.ReadFromURI() failed")
	}

	v := viper.New()
	v.SetConfigType("json")
	err = v.ReadConfig(bytes.NewBuffer(vbuffer))
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, "get: v.ReadConfig() failed")
	}

	blobStoreURI := v.GetString("storage-uri")
	if blobStoreURI == "" {
		return storeStats, timeStats, fmt.Errorf("get: missing storage-uri in get-config")
	}
	sourceFilePath := v.GetString("source-path")
	if sourceFilePath == "" {
		return storeStats, timeStats, fmt.Errorf("get: missing source-path in get-config")
	}
	var versionLocalStoreIndexPath string
	if v.IsSet("version-local-store-index-path") {
		versionLocalStoreIndexPath = v.GetString("version-local-store-index-path")
	}

	readGetConfigTime := time.Since(readGetConfigStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read get config", readGetConfigTime})

	downSyncStoreStats, downSyncTimeStats, err := downSyncVersion(
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
		excludeFilterRegEx)

	storeStats = append(storeStats, downSyncStoreStats...)
	timeStats = append(timeStats, downSyncTimeStats...)

	return storeStats, timeStats, err
}

type GetCmd struct {
	GetConfigURI string `name:"get-config-path" help:"File uri for json formatted get-config file" required:""`
	TargetPathOption
	TargetIndexUriOption
	ValidateTargetOption
	VersionLocalStoreIndexPathOption
	StorageURIOption
	CachePathOption
	RetainPermissionsOption
	DownsyncIncludeRegExOption
	DownsyncExcludeRegExOption
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
		r.ExcludeFilterRegEx)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
