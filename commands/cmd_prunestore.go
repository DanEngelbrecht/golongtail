package commands

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/DanEngelbrecht/golongtail/remotestore"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type pruneOneResult struct {
	err    error
	blocks []uint64
}

func readPruneVersion(
	sourceFilePath string,
	s3EndpointResolverURI string,
	versionLocalStoreIndexFilePath string,
	writeVersionLocalStoreIndex bool) (longtaillib.Longtail_VersionIndex, longtaillib.Longtail_StoreIndex, error) {
	const fname = "readPruneVersion"
	vbuffer, err := longtailutils.ReadFromURI(sourceFilePath, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
	if err != nil {
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	versionIndex, err := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if err != nil {
		err = errors.Wrapf(err, "Cant parse version index from `%s`", sourceFilePath)
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	var storeIndex longtaillib.Longtail_StoreIndex
	if versionLocalStoreIndexFilePath != "" && !writeVersionLocalStoreIndex {
		sbuffer, err := longtailutils.ReadFromURI(versionLocalStoreIndexFilePath, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
		if err == nil {
			storeIndex, err = longtaillib.ReadStoreIndexFromBuffer(sbuffer)
			if err != nil {
				err = errors.Wrapf(err, "Cant parse store index from `%s`", versionLocalStoreIndexFilePath)
				versionIndex.Dispose()
				return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			}
			err = longtaillib.ValidateStore(storeIndex, versionIndex)
			if err != nil {
				storeIndex.Dispose()
				versionIndex.Dispose()
				err = errors.Wrapf(err, "Cant validate store index for `%s`", versionLocalStoreIndexFilePath)
				return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			}
		}
	}
	return versionIndex, storeIndex, nil
}

func pruneOne(
	remoteStore longtaillib.Longtail_BlockStoreAPI,
	sourceFilePath string,
	s3EndpointResolverURI string,
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

	versionIndex, existingStoreIndex, err := readPruneVersion(sourceFilePath, s3EndpointResolverURI, versionLocalStoreIndexFilePath, writeVersionLocalStoreIndex)
	if err != nil {
		result.err = errors.Wrap(err, fname)
		return
	}
	defer versionIndex.Dispose()
	defer existingStoreIndex.Dispose()

	if !existingStoreIndex.IsValid() {
		neededChunks := versionIndex.GetChunkHashes()
		existingStoreIndex, err = longtailutils.GetExistingStoreIndexSync(remoteStore, neededChunks, 0)
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
		defer sbuffer.Dispose()
		if err != nil {
			existingStoreIndex.Dispose()
			result.err = errors.Wrap(err, fname)
			return
		}
		err = longtailutils.WriteToURI(versionLocalStoreIndexFilePath, sbuffer.ToBuffer(), longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
		if err != nil {
			existingStoreIndex.Dispose()
			result.err = errors.Wrap(err, fname)
			return
		}
	}

	result = pruneOneResult{err: nil}
	result.blocks = append(result.blocks, existingStoreIndex.GetBlockHashes()...)
}

func gatherBlocksToKeep(
	numWorkerCount int,
	storageURI string,
	s3EndpointResolverURI string,
	sourceFilePaths []string,
	versionLocalStoreIndexFilePaths []string,
	writeVersionLocalStoreIndex bool,
	validateVersions bool,
	skipInvalidVersions bool,
	dryRun bool,
	jobs longtaillib.Longtail_JobAPI) ([]uint64, error) {
	const fname = "gatherBlocksToKeep"
	log := logrus.WithFields(logrus.Fields{
		"fname":                       fname,
		"numWorkerCount":              numWorkerCount,
		"storageURI":                  storageURI,
		"s3EndpointResolverURI":       s3EndpointResolverURI,
		"writeVersionLocalStoreIndex": writeVersionLocalStoreIndex,
		"validateVersions":            validateVersions,
		"skipInvalidVersions":         skipInvalidVersions,
		"dryRun":                      dryRun,
	})
	log.Debug(fname)
	remoteStore, err := remotestore.CreateBlockStoreForURI(storageURI, false, nil, jobs, numWorkerCount, 8388608, 1024, remotestore.ReadOnly, false, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	defer remoteStore.Dispose()

	usedBlocks := make(map[uint64]uint32)

	resultChannel := make(chan pruneOneResult, numWorkerCount)
	activeWorkerCount := 0

	progress := longtailutils.CreateProgress("Processing versions       ", 0)
	defer progress.Dispose()

	totalCount := uint32(len(sourceFilePaths))
	completed := 0

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
			go pruneOne(
				remoteStore,
				sourceFilePath,
				s3EndpointResolverURI,
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

func pruneStore(
	numWorkerCount int,
	storageURI string,
	s3EndpointResolverURI string,
	sourcePaths string,
	versionLocalStoreIndexesPath string,
	writeVersionLocalStoreIndex bool,
	validateVersions bool,
	skipInvalidVersions bool,
	maxStoreIndexSize int64,
	dryRun bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "pruneStore"
	log := logrus.WithFields(logrus.Fields{
		"fname":                        fname,
		"numWorkerCount":               numWorkerCount,
		"storageURI":                   storageURI,
		"s3EndpointResolverURI":        s3EndpointResolverURI,
		"sourcePaths":                  sourcePaths,
		"versionLocalStoreIndexesPath": versionLocalStoreIndexesPath,
		"writeVersionLocalStoreIndex":  writeVersionLocalStoreIndex,
		"validateVersions":             validateVersions,
		"skipInvalidVersions":          skipInvalidVersions,
		"maxStoreIndexSize":            maxStoreIndexSize,
		"dryRun":                       dryRun,
	})
	log.Info(fname)

	setupStartTime := time.Now()
	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

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
	sourceFilePathsTime := time.Since(sourceFilePathsStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source file list", sourceFilePathsTime})

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

		versionLocalStoreIndexesPathsTime := time.Since(versionLocalStoreIndexesPathsStartTime)
		timeStats = append(timeStats, longtailutils.TimeStat{"Read version local store index file list", versionLocalStoreIndexesPathsTime})

		if len(sourceFilePaths) != len(versionLocalStoreIndexFilePaths) {
			err = fmt.Errorf("pruneStore: Number of files in `%s` does not match number of files in `%s`", sourcePaths, versionLocalStoreIndexesPath)
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
	}

	gatherBlocksToKeepStartTime := time.Now()
	blocksToKeep, err := gatherBlocksToKeep(
		numWorkerCount,
		storageURI,
		s3EndpointResolverURI,
		sourceFilePaths,
		versionLocalStoreIndexFilePaths,
		writeVersionLocalStoreIndex,
		validateVersions,
		skipInvalidVersions,
		dryRun,
		jobs)

	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	gatherBlocksToKeepTime := time.Since(gatherBlocksToKeepStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Gather used blocks", gatherBlocksToKeepTime})

	if dryRun {
		fmt.Printf("Prune would keep %d blocks", len(blocksToKeep))
		return storeStats, timeStats, nil
	}
	remoteStore, err := remotestore.CreateBlockStoreForURI(storageURI, maxStoreIndexSize == -1, nil, jobs, numWorkerCount, 8388608, 1024, remotestore.ReadWrite, false, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer remoteStore.Dispose()

	pruneStartTime := time.Now()
	prunedBlockCount, err := longtailutils.PruneBlocksSync(remoteStore, blocksToKeep)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	pruneTime := time.Since(pruneStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Prune", pruneTime})

	log.Infof("Pruned %d blocks", prunedBlockCount)

	flushStartTime := time.Now()
	err = longtailutils.FlushStoreSync(&remoteStore)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Flush", flushTime})

	remoteStoreStats, err := remoteStore.GetStats()
	if err == nil {
		storeStats = append(storeStats, longtailutils.StoreStat{"Remote", remoteStoreStats})
	}

	return storeStats, timeStats, nil
}

type PruneStoreCmd struct {
	StorageURIOption
	S3EndpointResolverURLOption
	SourcePaths                 string `name:"source-paths" help:"File containing list of source longtail uris" required:""`
	VersionLocalStoreIndexPaths string `name:"version-local-store-index-paths" help:"File containing list of version local store index longtail uris"`
	DryRun                      bool   `name:"dry-run" help:"Don't prune, just show how many blocks would be kept if prune was run"`
	WriteVersionLocalStoreIndex bool   `name:"write-version-local-store-index" help:"Write a new version local store index for each version. This requires a valid version-local-store-index-paths input parameter"`
	ValidateVersions            bool   `name:"validate-versions" help:"Verify that all content needed for a version is available in the store"`
	SkipInvalidVersions         bool   `name:"skip-invalid-versions" help:"If an invalid version is found, disregard its blocks. If not set and validate-version is set, invalid version will abort with an error"`
	MaxStoreIndexSizeOption
}

func (r *PruneStoreCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := pruneStore(
		ctx.NumWorkerCount,
		r.StorageURI,
		r.S3EndpointResolverURL,
		r.SourcePaths,
		r.VersionLocalStoreIndexPaths,
		r.WriteVersionLocalStoreIndex,
		r.ValidateVersions,
		r.SkipInvalidVersions,
		r.MaxStoreIndexSize,
		r.DryRun)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
