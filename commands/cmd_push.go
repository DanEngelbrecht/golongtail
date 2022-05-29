package commands

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func push(
	numWorkerCount int,
	targetChunkSize uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	compressionAlgorithm string,
	hashAlgorithm string,
	minBlockUsagePercent uint32,
	enableFileMapping bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "get"
	log := logrus.WithFields(logrus.Fields{
		"fname":                fname,
		"numWorkerCount":       numWorkerCount,
		"targetChunkSize":      targetChunkSize,
		"targetBlockSize":      targetBlockSize,
		"maxChunksPerBlock":    maxChunksPerBlock,
		"compressionAlgorithm": compressionAlgorithm,
		"hashAlgorithm":        hashAlgorithm,
		"minBlockUsagePercent": minBlockUsagePercent,
		"enableFileMapping":    enableFileMapping,
	})
	log.Debug(fname)

	setupStartTime := time.Now()

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	excludeFilterRegEx := ".longtail/*."
	sourceFolderPath := ""
	targetFilePath := ".longtail/tmp.lvi"
	blobStoreURI := ".longtail/store"
	versionLocalStoreIndexPath := ".longtail/tmp.lsi"
	err := os.MkdirAll(".longtail/store/versions", 0777)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	upSyncStoreStats, upSyncTimeStats, err := upsync(
		numWorkerCount,
		blobStoreURI,
		sourceFolderPath,
		"",
		targetFilePath,
		targetChunkSize,
		targetBlockSize,
		maxChunksPerBlock,
		compressionAlgorithm,
		hashAlgorithm,
		"",
		excludeFilterRegEx,
		minBlockUsagePercent,
		versionLocalStoreIndexPath,
		enableFileMapping)

	versionFile, err := os.Open(targetFilePath)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	versionFileData, err := ioutil.ReadAll(versionFile)
	versionFile.Close()
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	sum := sha256.Sum256(versionFileData)

	hashedVersionFilePath := fmt.Sprintf(".longtail/store/versions/%x.lvi", sum)
	err = os.Rename(targetFilePath, hashedVersionFilePath)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	hashedVersionLocalStoreIndexPath := fmt.Sprintf(".longtail/store/versions/%x.lsi", sum)
	err = os.Rename(versionLocalStoreIndexPath, hashedVersionLocalStoreIndexPath)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	storeStats = append(storeStats, upSyncStoreStats...)
	timeStats = append(timeStats, upSyncTimeStats...)

	return storeStats, timeStats, errors.Wrap(err, fname)
}

type PushCmd struct {
	TargetChunkSizeOption
	MaxChunksPerBlockOption
	TargetBlockSizeOption
	MinBlockUsagePercentOption
	CompressionOption
	HashingOption
	EnableFileMappingOption
}

func (r *PushCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := push(
		ctx.NumWorkerCount,
		r.TargetChunkSize,
		r.TargetBlockSize,
		r.MaxChunksPerBlock,
		r.Compression,
		r.Hashing,
		r.MinBlockUsagePercent,
		r.EnableFileMapping)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
