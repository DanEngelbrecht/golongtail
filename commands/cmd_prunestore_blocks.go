package commands

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type ReadStoreIndexAsyncResult struct {
	storeIndex longtaillib.Longtail_StoreIndex
	elapsed    time.Duration
	err        error
}

func pruneStoreBlocks(
	numWorkerCount int,
	storeIndexPath string,
	blocksRootPath string,
	blockExtension string,
	dryRun bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "pruneStore"
	log := logrus.WithFields(logrus.Fields{
		"fname":          fname,
		"numWorkerCount": numWorkerCount,
		"storeIndexPath": storeIndexPath,
		"blocksRootPath": blocksRootPath,
		"blockExtension": blockExtension,
		"dryRun":         dryRun,
	})
	log.Debug(fname)
	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}
	// TODO

	blobStore, err := longtailstorelib.CreateBlobStoreForURI(blocksRootPath)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer client.Close()

	readStoreIndexAsyncResultChannel := make(chan ReadStoreIndexAsyncResult, 1)
	go func() {
		const fname = "GetStoreIndexAsync"
		start := time.Now()
		storeIndexBuffer, err := longtailutils.ReadFromURI(storeIndexPath)
		if err != nil {
			readStoreIndexAsyncResultChannel <- ReadStoreIndexAsyncResult{err: errors.Wrap(err, fname), elapsed: time.Since(start)}
			return
		}
		storeIndex, err := longtaillib.ReadStoreIndexFromBuffer(storeIndexBuffer)
		if err != nil {
			readStoreIndexAsyncResultChannel <- ReadStoreIndexAsyncResult{err: errors.Wrap(err, fname), elapsed: time.Since(start)}
			return
		}
		readStoreIndexAsyncResultChannel <- ReadStoreIndexAsyncResult{err: nil, elapsed: time.Since(start), storeIndex: storeIndex}
	}()

	getBlockObjectsStartTime := time.Now()
	allObjects, err := client.GetObjects("")
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	timeStats = append(timeStats, longtailutils.TimeStat{"Get block objects", time.Since(getBlockObjectsStartTime)})

	checkFoundBlocksStartTime := time.Now()
	blockNameRegExPattern := ".*0x([0-9,a-f,A-F]*).*" + blockExtension
	blockNameRegEx, err := regexp.Compile(blockNameRegExPattern)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	findObjectsProgress := longtailutils.CreateProgress("Checking found blocks", 2)
	defer findObjectsProgress.Dispose()

	blocksFound := make(map[uint64]string)
	for i, object := range allObjects {
		findObjectsProgress.OnProgress(uint32(len(allObjects)), uint32(i))
		m := blockNameRegEx.FindSubmatch([]byte(object.Name))
		if len(m) < 2 {
			continue
		}
		hashString := string(m[1])
		hash, err := strconv.ParseUint(hashString, 16, 64)
		if err != nil {
			continue
		}
		blocksFound[hash] = object.Name
	}
	findObjectsProgress.OnProgress(uint32(len(allObjects)), uint32(len(allObjects)))
	timeStats = append(timeStats, longtailutils.TimeStat{"Check found blocks", time.Since(checkFoundBlocksStartTime)})

	fmt.Printf("Found %d blocks\n", len(blocksFound))

	readStoreIndexAsyncResult := <-readStoreIndexAsyncResultChannel
	timeStats = append(timeStats, longtailutils.TimeStat{"Read store index", readStoreIndexAsyncResult.elapsed})
	if readStoreIndexAsyncResult.err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	storeIndex := readStoreIndexAsyncResult.storeIndex
	defer storeIndex.Dispose()

	indexingUsedBlocksStartTime := time.Now()
	indexUsedBlocksProgress := longtailutils.CreateProgress("Indexing used blocks", 2)
	defer indexUsedBlocksProgress.Dispose()

	blockHashes := storeIndex.GetBlockHashes()
	blocksUsed := make(map[uint64]bool)
	for i, blockHash := range blockHashes {
		indexUsedBlocksProgress.OnProgress(uint32(len(blockHashes)), uint32(i))
		blocksUsed[blockHash] = true
	}
	indexUsedBlocksProgress.OnProgress(uint32(len(blockHashes)), uint32(len(blockHashes)))
	timeStats = append(timeStats, longtailutils.TimeStat{"Index used blocks", time.Since(indexingUsedBlocksStartTime)})

	checkForUnusedBlocksStartTime := time.Now()
	checkForUnusedBlocksProgress := longtailutils.CreateProgress("Checking for unused blocks", 2)
	defer checkForUnusedBlocksProgress.Dispose()
	i := 0
	unusedBlocks := make([]string, 0)
	for blockHash, blockName := range blocksFound {
		checkForUnusedBlocksProgress.OnProgress(uint32(len(blocksFound)), uint32(i))
		if _, exists := blocksUsed[blockHash]; exists {
			continue
		}
		unusedBlocks = append(unusedBlocks, blockName)
	}
	checkForUnusedBlocksProgress.OnProgress(uint32(len(blocksFound)), uint32(len(blocksFound)))
	timeStats = append(timeStats, longtailutils.TimeStat{"Check for unused blocks", time.Since(checkForUnusedBlocksStartTime)})

	fmt.Printf("Found %d blocks to prune\n", len(unusedBlocks))

	deleteUnusedBlocksStartTime := time.Now()
	deleteUnusedBlocksProgress := longtailutils.CreateProgress("Deleting unused blocks", 1)
	defer deleteUnusedBlocksProgress.Dispose()

	workerCount := numWorkerCount
	if workerCount > len(unusedBlocks) {
		workerCount = len(unusedBlocks)
	}
	deleteResultChannel := make(chan error, workerCount)
	totalCount := uint32(len(unusedBlocks))
	activeWorkerCount := 0
	completed := uint32(0)

	clients := make([]*longtailstorelib.BlobClient, workerCount)
	defer func() {
		for i := 0; i < workerCount; i++ {
			if clients[i] != nil {
				(*clients[i]).Close()
			}
		}
	}()

	deleteClientChannel := make(chan *longtailstorelib.BlobClient, workerCount)
	for i := 0; i < workerCount; i++ {
		workerClient, err := blobStore.NewClient(context.Background())
		if err != nil {
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
		clients[i] = &workerClient
	}

	for i, blockName := range unusedBlocks {
		if activeWorkerCount == workerCount {
			deleteErr := <-deleteResultChannel
			deleteClient := <-deleteClientChannel
			completed++
			activeWorkerCount--
			clients[activeWorkerCount] = deleteClient
			if deleteErr != nil {
				err = deleteErr
				break
			}
		}

		workerClient := clients[activeWorkerCount]
		go func(client *longtailstorelib.BlobClient, blockName string) {
			const fname = "deleteBlockWorker"
			defer func() {
				deleteClientChannel <- client
			}()

			log.Infof("Delete `%s`\n", blockName)
			if !dryRun {
				object, err := (*client).NewObject(blockName)
				if err != nil {
					deleteResultChannel <- errors.Wrap(err, fname)
					return
				}
				err = object.Delete()
				if err != nil {
					deleteResultChannel <- errors.Wrap(err, fname)
					return
				}
			}
			deleteResultChannel <- nil
		}(workerClient, blockName)
		activeWorkerCount++

		for activeWorkerCount > 0 {
			received := false
			select {
			case deleteErr := <-deleteResultChannel:
				deleteClient := <-deleteClientChannel
				completed++
				activeWorkerCount--
				clients[activeWorkerCount] = deleteClient
				if deleteErr != nil {
					err = deleteErr
				}
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
		deleteUnusedBlocksProgress.OnProgress(totalCount, uint32(i))
	}

	for activeWorkerCount > 0 {
		deleteErr := <-deleteResultChannel
		deleteClient := <-deleteClientChannel
		completed++
		activeWorkerCount--
		clients[activeWorkerCount] = deleteClient
		if deleteErr != nil {
			err = deleteErr
		}
		deleteUnusedBlocksProgress.OnProgress(completed, totalCount)
	}

	timeStats = append(timeStats, longtailutils.TimeStat{"Delete unused blocks", time.Since(deleteUnusedBlocksStartTime)})

	if !dryRun {
		fmt.Printf("Deleted %d blocks\n", len(unusedBlocks))
	}

	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	return storeStats, timeStats, nil
}

type PruneStoreBlocksCmd struct {
	StoreIndexPathOption
	BlocksRootPath string `name:"blocks-root-path" help:"Root path uri for all blocks to check" required:""`
	BlockExtension string `name:"block-extension" help:"The file extension to use when finding blocks" default:".lsb"`
	DryRun         bool   `name:"dry-run" help:"Don't prune, just show how many blocks would be deleted if prune was run"`
}

func (r *PruneStoreBlocksCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := pruneStoreBlocks(
		ctx.NumWorkerCount,
		r.StoreIndexPath,
		r.BlocksRootPath,
		r.BlockExtension,
		r.DryRun)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
