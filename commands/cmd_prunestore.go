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

func pruneStore(
	numWorkerCount int,
	storageURI string,
	sourcePaths string,
	versionLocalStoreIndexesPath string,
	writeVersionLocalStoreIndex bool,
	dryRun bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "pruneStore"
	log := logrus.WithFields(logrus.Fields{
		"fname":                        fname,
		"numWorkerCount":               numWorkerCount,
		"storageURI":                   storageURI,
		"sourcePaths":                  sourcePaths,
		"versionLocalStoreIndexesPath": versionLocalStoreIndexesPath,
		"writeVersionLocalStoreIndex":  writeVersionLocalStoreIndex,
		"dryRun":                       dryRun,
	})
	log.Debug(fname)

	setupStartTime := time.Now()
	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	storeMode := remotestore.ReadOnly
	if !dryRun {
		storeMode = remotestore.ReadWrite
	}

	remoteStore, err := remotestore.CreateBlockStoreForURI(storageURI, "", jobs, numWorkerCount, 8388608, 1024, storeMode)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer remoteStore.Dispose()

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

	usedBlocks := make(map[uint64]uint32)

	batchCount := numWorkerCount
	if batchCount > len(sourceFilePaths) {
		batchCount = len(sourceFilePaths)
	}
	batchStart := 0

	scanningForBlocksStartTime := time.Now()

	batchErrors := make(chan error, batchCount)
	progress := longtailutils.CreateProgress("Processing versions")
	defer progress.Dispose()
	for batchStart < len(sourceFilePaths) {
		batchLength := batchCount
		if batchStart+batchLength > len(sourceFilePaths) {
			batchLength = len(sourceFilePaths) - batchStart
		}
		blockHashesPerBatch := make([][]uint64, batchLength)
		for batchPos := 0; batchPos < batchLength; batchPos++ {
			i := batchStart + batchPos
			sourceFilePath := sourceFilePaths[i]
			versionLocalStoreIndexFilePath := ""
			if len(versionLocalStoreIndexFilePaths) > 0 {
				versionLocalStoreIndexFilePath = versionLocalStoreIndexFilePaths[i]
			}
			go func(batchPos int, sourceFilePath string, versionLocalStoreIndexFilePath string) {
				const fname = "batch"
				log := logrus.WithFields(logrus.Fields{
					"fname": fname,
				})
				log.Debug(fname)
				vbuffer, err := longtailutils.ReadFromURI(sourceFilePath)
				if err != nil {
					batchErrors <- errors.Wrap(err, fname)
					return
				}
				sourceVersionIndex, err := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
				if err != nil {
					err = errors.Wrapf(err, "Cant parse version index from `%s`", sourceFilePath)
					batchErrors <- errors.Wrap(err, fname)
					return
				}

				var existingStoreIndex longtaillib.Longtail_StoreIndex
				if versionLocalStoreIndexFilePath != "" && !writeVersionLocalStoreIndex {
					sbuffer, err := longtailutils.ReadFromURI(versionLocalStoreIndexFilePath)
					if err == nil {
						existingStoreIndex, err = longtaillib.ReadStoreIndexFromBuffer(sbuffer)
						if err != nil {
							err = errors.Wrapf(err, "Cant parse store index from `%s`", versionLocalStoreIndexFilePath)
							batchErrors <- errors.Wrap(err, fname)
							return
						}
						err = longtaillib.ValidateStore(existingStoreIndex, sourceVersionIndex)
						if err != nil {
							existingStoreIndex.Dispose()
						}
					}
				}
				if !existingStoreIndex.IsValid() {
					existingStoreIndex, err = longtailutils.GetExistingStoreIndexSync(remoteStore, sourceVersionIndex.GetChunkHashes(), 0)
					if err != nil {
						sourceVersionIndex.Dispose()
						batchErrors <- errors.Wrap(err, fname)
						return
					}
					err = longtaillib.ValidateStore(existingStoreIndex, sourceVersionIndex)
					if err != nil {
						existingStoreIndex.Dispose()
						sourceVersionIndex.Dispose()
						if dryRun {
							log.Warnf("Data is missing in store `%s` for version `%s`", storageURI, sourceFilePath)
							batchErrors <- nil
						} else {
							err = errors.Wrapf(err, "pruneStore: Data is missing in store `%s` for version `%s`", storageURI, sourceFilePath)
							batchErrors <- errors.Wrap(err, fname)
						}
						return
					}
				}

				blockHashesPerBatch[batchPos] = append(blockHashesPerBatch[batchPos], existingStoreIndex.GetBlockHashes()...)

				if versionLocalStoreIndexFilePath != "" && writeVersionLocalStoreIndex && !dryRun {
					sbuffer, err := longtaillib.WriteStoreIndexToBuffer(existingStoreIndex)
					if err != nil {
						existingStoreIndex.Dispose()
						sourceVersionIndex.Dispose()
						batchErrors <- errors.Wrap(err, fname)
						return
					}
					err = longtailutils.WriteToURI(versionLocalStoreIndexFilePath, sbuffer)
					if err != nil {
						existingStoreIndex.Dispose()
						sourceVersionIndex.Dispose()
						batchErrors <- errors.Wrap(err, fname)
						return
					}
				}
				existingStoreIndex.Dispose()
				sourceVersionIndex.Dispose()

				batchErrors <- nil
			}(batchPos, sourceFilePath, versionLocalStoreIndexFilePath)
		}

		for batchPos := 0; batchPos < batchLength; batchPos++ {
			batchError := <-batchErrors
			if batchError != nil {
				return storeStats, timeStats, errors.Wrap(batchError, fname)
			}
			progress.OnProgress(uint32(len(sourceFilePaths)), uint32(batchStart+batchPos))
		}
		for batchPos := 0; batchPos < batchLength; batchPos++ {
			for _, h := range blockHashesPerBatch[batchPos] {
				usedBlocks[h] += 1
			}
		}

		batchStart += batchLength
	}
	progress.OnProgress(uint32(len(sourceFilePaths)), uint32(len(sourceFilePaths)))

	scanningForBlocksTime := time.Since(scanningForBlocksStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Scanning", scanningForBlocksTime})

	if dryRun {
		fmt.Printf("Prune would keep %d blocks\n", len(usedBlocks))
		return storeStats, timeStats, nil
	}

	pruneStartTime := time.Now()

	blockHashes := make([]uint64, len(usedBlocks))
	i := 0
	for k := range usedBlocks {
		blockHashes[i] = k
		i++
	}

	prunedBlockCount, err := longtailutils.PruneBlocksSync(remoteStore, blockHashes)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	pruneTime := time.Since(pruneStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Prune", pruneTime})

	fmt.Printf("Pruned %d blocks\n", prunedBlockCount)

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
	SourcePaths                 string `name:"source-paths" help:"File containing list of source longtail uris" required:""`
	VersionLocalStoreIndexPaths string `name:"version-local-store-index-paths" help:"File containing list of version local store index longtail uris"`
	DryRun                      bool   `name:"dry-run" help:"Don't prune, just show how many blocks would be kept if prune was run"`
	WriteVersionLocalStoreIndex bool   `name:"write-version-local-store-index" help:"Write a new version local store index for each version. This requires a valid version-local-store-index-paths input parameter"`
}

func (r *PruneStoreCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := pruneStore(
		ctx.NumWorkerCount,
		r.StorageURI,
		r.SourcePaths,
		r.VersionLocalStoreIndexPaths,
		r.WriteVersionLocalStoreIndex,
		r.DryRun)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
