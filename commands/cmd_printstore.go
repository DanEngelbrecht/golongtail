package commands

import (
	"fmt"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func printStore(
	numWorkerCount int,
	storeIndexPath string,
	compact bool,
	details bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	log := logrus.WithFields(logrus.Fields{
		"numWorkerCount": numWorkerCount,
		"storeIndexPath": storeIndexPath,
		"compact":        compact,
		"details":        details,
	})
	log.Debug("print-store")

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	readStoreIndexStartTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(storeIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	if vbuffer == nil {
		return storeStats, timeStats, longtaillib.ErrENOENT
	}
	storeIndex, errno := longtaillib.ReadStoreIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "printStore: longtaillib.ReadStoreIndexFromBuffer() failed")
	}
	defer storeIndex.Dispose()
	readStoreIndexTime := time.Since(readStoreIndexStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read store index", readStoreIndexTime})

	storedChunksSizes := uint64(0)
	uniqueStoredChunksSizes := uint64(0)
	if details {
		getChunkSizesStartTime := time.Now()
		uniqueChunks := make(map[uint64]uint32)
		chunkHashes := storeIndex.GetChunkHashes()
		chunkSizes := storeIndex.GetChunkSizes()
		for i, chunkHash := range chunkHashes {
			uniqueChunks[chunkHash] = chunkSizes[i]
			storedChunksSizes += uint64(chunkSizes[i])
		}
		for _, size := range uniqueChunks {
			uniqueStoredChunksSizes += uint64(size)
		}
		getChunkSizesTime := time.Since(getChunkSizesStartTime)
		timeStats = append(timeStats, longtailutils.TimeStat{"Get chunk sizes", getChunkSizesTime})
	}

	if compact {
		fmt.Printf("%s\t%d\t%s\t%d\t%d",
			storeIndexPath,
			storeIndex.GetVersion(),
			longtailutils.HashIdentifierToString(storeIndex.GetHashIdentifier()),
			storeIndex.GetBlockCount(),
			storeIndex.GetChunkCount())
		if details {
			fmt.Printf("\t%d\t%d",
				storedChunksSizes,
				uniqueStoredChunksSizes)
		}
		fmt.Printf("\n")
	} else {
		fmt.Printf("Version:             %d\n", storeIndex.GetVersion())
		fmt.Printf("Hash Identifier:     %s\n", longtailutils.HashIdentifierToString(storeIndex.GetHashIdentifier()))
		fmt.Printf("Block Count:         %d   (%s)\n", storeIndex.GetBlockCount(), longtailutils.ByteCountDecimal(uint64(storeIndex.GetBlockCount())))
		fmt.Printf("Chunk Count:         %d   (%s)\n", storeIndex.GetChunkCount(), longtailutils.ByteCountDecimal(uint64(storeIndex.GetChunkCount())))
		if details {
			fmt.Printf("Data size:           %d   (%s)\n", storedChunksSizes, longtailutils.ByteCountBinary(storedChunksSizes))
			fmt.Printf("Unique Data size:    %d   (%s)\n", uniqueStoredChunksSizes, longtailutils.ByteCountBinary(uniqueStoredChunksSizes))
		}
	}

	return storeStats, timeStats, nil
}

type PrintStoreCmd struct {
	StoreIndexPathOption
	CompactOption
	Details bool `name:"details" help:"Show details about data sizes"`
}

func (r *PrintStoreCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := printStore(
		ctx.NumWorkerCount,
		r.StoreIndexPath,
		r.Compact,
		r.Details)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
