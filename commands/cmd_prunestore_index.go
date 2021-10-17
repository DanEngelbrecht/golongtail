package commands

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func pruneOneUsingStoreIndex(
	storeIndex longtaillib.Longtail_StoreIndex,
	sourceFilePath string,
	versionLocalStoreIndexFilePath string,
	validateVersions bool,
	skipInvalidVersions bool,
	writeVersionLocalStoreIndex bool,
	dryRun bool,
	outResults chan<- pruneOneResult) {
	const fname = "pruneOne"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	log.Debug(fname)

	result := pruneOneResult{}
	defer func() {
		outResults <- result
	}()

	versionIndex, existingStoreIndex, err := readPruneVersion(sourceFilePath, versionLocalStoreIndexFilePath, writeVersionLocalStoreIndex)
	if err != nil {
		result.err = errors.Wrap(err, fname)
		return
	}
	defer versionIndex.Dispose()
	defer existingStoreIndex.Dispose()

	if !existingStoreIndex.IsValid() {
		neededChunks := versionIndex.GetChunkHashes()
		existingStoreIndex, err = longtaillib.GetExistingStoreIndex(storeIndex, neededChunks, 0)
		if err != nil {
			result.err = errors.Wrap(err, fname)
			return
		}
		defer existingStoreIndex.Dispose()

		if validateVersions {
			err = longtaillib.ValidateStore(existingStoreIndex, versionIndex)
			if err != nil {
				existingStoreIndex.Dispose()
				if skipInvalidVersions {
					log.Warnf("Data is missing for version `%s`", sourceFilePath)
					result.err = nil
					return
				} else {
					err = errors.Wrapf(err, "pruneStore: Data is missing for version `%s`", sourceFilePath)
					result.err = errors.Wrap(err, fname)
					return
				}
			}
		}
	}

	if versionLocalStoreIndexFilePath != "" && writeVersionLocalStoreIndex && !dryRun {
		sbuffer, err := longtaillib.WriteStoreIndexToBuffer(existingStoreIndex)
		if err != nil {
			existingStoreIndex.Dispose()
			result.err = errors.Wrap(err, fname)
			return
		}
		err = longtailutils.WriteToURI(versionLocalStoreIndexFilePath, sbuffer)
		if err != nil {
			existingStoreIndex.Dispose()
			result.err = errors.Wrap(err, fname)
			return
		}
	}

	result = pruneOneResult{err: nil}
	result.blocks = append(result.blocks, existingStoreIndex.GetBlockHashes()...)
}

func gatherBlocksToKeepFromStoreIndex(
	numWorkerCount int,
	storeIndex longtaillib.Longtail_StoreIndex,
	sourceFilePaths []string,
	versionLocalStoreIndexFilePaths []string,
	writeVersionLocalStoreIndex bool,
	validateVersions bool,
	skipInvalidVersions bool,
	dryRun bool) ([]uint64, error) {
	const fname = "gatherBlocksToKeep"
	log := logrus.WithFields(logrus.Fields{
		"fname":                       fname,
		"numWorkerCount":              numWorkerCount,
		"writeVersionLocalStoreIndex": writeVersionLocalStoreIndex,
		"validateVersions":            validateVersions,
		"skipInvalidVersions":         skipInvalidVersions,
		"dryRun":                      dryRun,
	})
	log.Debug(fname)

	usedBlocks := make(map[uint64]uint32)

	resultChannel := make(chan pruneOneResult, numWorkerCount)
	activeWorkerCount := 0

	progress := longtailutils.CreateProgress("Processing versions", 0)
	defer progress.Dispose()

	totalCount := uint32(len(sourceFilePaths))
	completed := 0

	var err error

	for i, sourceFilePath := range sourceFilePaths {
		versionLocalStoreIndexFilePath := ""
		if len(versionLocalStoreIndexFilePaths) > 0 {
			versionLocalStoreIndexFilePath = versionLocalStoreIndexFilePaths[i]
		}

		if activeWorkerCount == numWorkerCount {
			result := <-resultChannel
			completed++
			if result.err == nil {
				for _, h := range result.blocks {
					usedBlocks[h] += 1
				}
			}
			activeWorkerCount--
		}

		if sourceFilePath != "" {
			go pruneOneUsingStoreIndex(
				storeIndex,
				sourceFilePath,
				versionLocalStoreIndexFilePath,
				validateVersions,
				skipInvalidVersions,
				writeVersionLocalStoreIndex,
				dryRun,
				resultChannel)
			activeWorkerCount++
		}

		for activeWorkerCount > 0 {
			received := false
			select {
			case result := <-resultChannel:
				completed++
				if result.err == nil {
					for _, h := range result.blocks {
						usedBlocks[h] += 1
					}
				} else {
					err = result.err
				}
				activeWorkerCount--
				received = true
			default:
			}
			if !received {
				break
			}
		}

		if err != nil {
			break
		}

		progress.OnProgress(totalCount, uint32(completed))
	}

	for activeWorkerCount > 0 {
		result := <-resultChannel
		completed++
		activeWorkerCount--
		if result.err == nil {
			for _, h := range result.blocks {
				usedBlocks[h] += 1
			}
		} else {
			err = result.err
		}
		progress.OnProgress(totalCount, uint32(completed))
	}

	if err != nil {
		return nil, errors.Wrap(err, fname)
	}

	blockHashes := make([]uint64, len(usedBlocks))
	i := 0
	for k := range usedBlocks {
		blockHashes[i] = k
		i++
	}

	return blockHashes, nil
}

func pruneStoreIndex(
	numWorkerCount int,
	storeIndexPath string,
	sourcePaths string,
	versionLocalStoreIndexesPath string,
	writeVersionLocalStoreIndex bool,
	validateVersions bool,
	skipInvalidVersions bool,
	dryRun bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "pruneStore"
	log := logrus.WithFields(logrus.Fields{
		"fname":                        fname,
		"numWorkerCount":               numWorkerCount,
		"storeIndexPath":               storeIndexPath,
		"sourcePaths":                  sourcePaths,
		"versionLocalStoreIndexesPath": versionLocalStoreIndexesPath,
		"writeVersionLocalStoreIndex":  writeVersionLocalStoreIndex,
		"validateVersions":             validateVersions,
		"skipInvalidVersions":          skipInvalidVersions,
		"dryRun":                       dryRun,
	})
	log.Debug(fname)

	setupStartTime := time.Now()
	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", time.Since(setupStartTime)})

	sourceFilePathsStartTime := time.Now()

	sourcesFile, err := os.Open(sourcePaths)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer sourcesFile.Close()

	sourceFilePaths := make([]string, 0)
	sourcesScanner := bufio.NewScanner(sourcesFile)
	for sourcesScanner.Scan() {
		sourceFilePath := sourcesScanner.Text()
		sourceFilePaths = append(sourceFilePaths, sourceFilePath)
	}
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source file list", time.Since(sourceFilePathsStartTime)})

	versionLocalStoreIndexFilePaths := make([]string, 0)
	if strings.TrimSpace(versionLocalStoreIndexesPath) != "" {
		versionLocalStoreIndexesPathsStartTime := time.Now()
		versionLocalStoreIndexFile, err := os.Open(versionLocalStoreIndexesPath)
		if err != nil {
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
		defer versionLocalStoreIndexFile.Close()

		versionLocalStoreIndexesScanner := bufio.NewScanner(versionLocalStoreIndexFile)
		for versionLocalStoreIndexesScanner.Scan() {
			versionLocalStoreIndexFilePath := versionLocalStoreIndexesScanner.Text()
			versionLocalStoreIndexFilePaths = append(versionLocalStoreIndexFilePaths, versionLocalStoreIndexFilePath)
		}

		timeStats = append(timeStats, longtailutils.TimeStat{"Read version local store index file list", time.Since(versionLocalStoreIndexesPathsStartTime)})

		if len(sourceFilePaths) != len(versionLocalStoreIndexFilePaths) {
			err = fmt.Errorf("pruneStore: Number of files in `%s` does not match number of files in `%s`", sourcePaths, versionLocalStoreIndexesPath)
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
	}

	readStoreIndexStartTime := time.Now()
	storeIndexBuffer, err := longtailutils.ReadFromURI(storeIndexPath)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	storeIndex, err := longtaillib.ReadStoreIndexFromBuffer(storeIndexBuffer)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer storeIndex.Dispose()
	storeIndexBuffer = nil
	timeStats = append(timeStats, longtailutils.TimeStat{"Read store index", time.Since(readStoreIndexStartTime)})

	gatherBlocksToKeepStartTime := time.Now()

	blocksToKeep, err := gatherBlocksToKeepFromStoreIndex(
		numWorkerCount,
		storeIndex,
		sourceFilePaths,
		versionLocalStoreIndexFilePaths,
		writeVersionLocalStoreIndex,
		validateVersions,
		skipInvalidVersions,
		dryRun)

	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	timeStats = append(timeStats, longtailutils.TimeStat{"Gather used blocks", time.Since(gatherBlocksToKeepStartTime)})

	pruneStartTime := time.Now()
	prunedStoreIndex, err := longtaillib.PruneStoreIndex(storeIndex, blocksToKeep)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	timeStats = append(timeStats, longtailutils.TimeStat{"Prune", time.Since(pruneStartTime)})

	oldBlockCount := storeIndex.GetBlockCount()
	newBlockCount := prunedStoreIndex.GetBlockCount()

	storeIndex.Dispose()

	if dryRun {
		fmt.Printf("Pruned %d blocks out of %d", oldBlockCount-newBlockCount, oldBlockCount)
		return storeStats, timeStats, nil
	}

	writeStoreIndexStartTime := time.Now()
	prunedStoreIndexBuffer, err := longtaillib.WriteStoreIndexToBuffer(prunedStoreIndex)
	prunedStoreIndex.Dispose()
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	err = longtailutils.WriteToURI(storeIndexPath, prunedStoreIndexBuffer)
	timeStats = append(timeStats, longtailutils.TimeStat{"Write store index", time.Since(writeStoreIndexStartTime)})
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	return storeStats, timeStats, nil
}

type PruneStoreIndexCmd struct {
	StoreIndexPathOption
	SourcePaths                 string `name:"source-paths" help:"File containing list of source longtail uris" required:""`
	VersionLocalStoreIndexPaths string `name:"version-local-store-index-paths" help:"File containing list of version local store index longtail uris"`
	DryRun                      bool   `name:"dry-run" help:"Don't prune, just show how many blocks would be kept if prune was run"`
	WriteVersionLocalStoreIndex bool   `name:"write-version-local-store-index" help:"Write a new version local store index for each version. This requires a valid version-local-store-index-paths input parameter"`
	ValidateVersions            bool   `name:"validate-versions" help:"Verify that all content needed for a version is available in the store"`
	SkipInvalidVersions         bool   `name:"skip-invalid-versions" help:"If an invalid version is found, disregard its blocks. If not set and validate-version is set, invalid version will abort with an error"`
}

func (r *PruneStoreIndexCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := pruneStoreIndex(
		ctx.NumWorkerCount,
		r.StoreIndexPath,
		r.SourcePaths,
		r.VersionLocalStoreIndexPaths,
		r.WriteVersionLocalStoreIndex,
		r.ValidateVersions,
		r.SkipInvalidVersions,
		r.DryRun)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
