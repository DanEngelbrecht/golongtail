package commands

import (
	"fmt"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func printStore(
	numWorkerCount int,
	storeIndexPath string,
	s3EndpointResolverURI string,
	compact bool,
	details bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "printStore"
	log := logrus.WithFields(logrus.Fields{
		"fname":                 fname,
		"numWorkerCount":        numWorkerCount,
		"storeIndexPath":        storeIndexPath,
		"s3EndpointResolverURI": s3EndpointResolverURI,
		"compact":               compact,
		"details":               details,
	})
	log.Debug(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	readStoreIndexStartTime := time.Now()

	vbuffer, err := longtailutils.ReadFromURI(storeIndexPath, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	storeIndex, err := longtaillib.ReadStoreIndexFromBuffer(vbuffer)
	if err != nil {
		err = errors.Wrapf(err, "Cant parse store index from `%s`", storeIndexPath)
		return storeStats, timeStats, errors.Wrap(err, fname)
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
	S3EndpointResolverURLOption
	CompactOption
	Details bool `name:"details" help:"Show details about data sizes"`
}

func (r *PrintStoreCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := printStore(
		ctx.NumWorkerCount,
		r.StoreIndexPath,
		r.S3EndpointResolverURL,
		r.Compact,
		r.Details)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
