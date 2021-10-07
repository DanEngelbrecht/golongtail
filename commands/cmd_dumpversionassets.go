package commands

import (
	"fmt"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func dumpVersionAssets(
	numWorkerCount int,
	versionIndexPath string,
	showDetails bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	_ = logrus.WithFields(logrus.Fields{
		"numWorkerCount":   numWorkerCount,
		"versionIndexPath": versionIndexPath,
		"showDetails":      showDetails,
	})

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	if vbuffer == nil {
		return storeStats, timeStats, longtaillib.ErrENOENT
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "dumpVersionAssets: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	assetCount := versionIndex.GetAssetCount()

	var biggestAsset uint64
	biggestAsset = 0
	for i := uint32(0); i < assetCount; i++ {
		assetSize := versionIndex.GetAssetSize(i)
		if assetSize > biggestAsset {
			biggestAsset = assetSize
		}
	}

	sizePadding := len(fmt.Sprintf("%d", biggestAsset))

	for i := uint32(0); i < assetCount; i++ {
		path := versionIndex.GetAssetPath(i)
		if showDetails {
			isDir := strings.HasSuffix(path, "/")
			assetSize := versionIndex.GetAssetSize(i)
			permissions := versionIndex.GetAssetPermissions(i)
			detailsString := longtailutils.GetDetailsString(path, assetSize, permissions, isDir, sizePadding)
			fmt.Printf("%s\n", detailsString)
		} else {
			fmt.Printf("%s\n", path)
		}
	}

	return storeStats, timeStats, nil
}

type DumpVersionAssetsCmd struct {
	VersionIndexPathOption
	Details bool `name:"details" help:"Show details about assets"`
}

func (r *DumpVersionAssetsCmd) DumpVersionAssetsCmd(ctx *Context) error {
	storeStats, timeStats, err := dumpVersionAssets(
		ctx.NumWorkerCount,
		r.VersionIndexPath,
		r.Details)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
