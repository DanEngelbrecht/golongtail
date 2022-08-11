package commands

import (
	"fmt"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func printVersion(
	numWorkerCount int,
	versionIndexPath string,
	s3EndpointResolverURI string,
	compact bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "printVersion"
	log := logrus.WithFields(logrus.Fields{
		"fname":                 fname,
		"numWorkerCount":        numWorkerCount,
		"versionIndexPath":      versionIndexPath,
		"s3EndpointResolverURI": s3EndpointResolverURI,
		"compact":               compact,
	})
	log.Info(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	readSourceStartTime := time.Now()

	vbuffer, err := longtailutils.ReadFromURI(versionIndexPath, longtailutils.WithS3EndpointResolverURI(s3EndpointResolverURI))
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	versionIndex, err := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if err != nil {
		err = errors.Wrapf(err, "Cant parse version index from `%s`", versionIndexPath)
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	var smallestChunkSize uint32
	var largestChunkSize uint32
	var averageChunkSize uint32
	var totalAssetSize uint64
	var totalChunkSize uint64
	totalAssetSize = 0
	totalChunkSize = 0
	chunkSizes := versionIndex.GetChunkSizes()
	if len(chunkSizes) > 0 {
		smallestChunkSize = uint32(chunkSizes[0])
		largestChunkSize = uint32(chunkSizes[0])
	} else {
		smallestChunkSize = 0
		largestChunkSize = 0
	}
	for i := uint32(0); i < uint32(len(chunkSizes)); i++ {
		chunkSize := uint32(chunkSizes[i])
		if chunkSize < smallestChunkSize {
			smallestChunkSize = chunkSize
		}
		if chunkSize > largestChunkSize {
			largestChunkSize = chunkSize
		}
		totalChunkSize = totalChunkSize + uint64(chunkSize)
	}
	if len(chunkSizes) > 0 {
		averageChunkSize = uint32(totalChunkSize / uint64(len(chunkSizes)))
	} else {
		averageChunkSize = 0
	}
	assetSizes := versionIndex.GetAssetSizes()
	for i := uint32(0); i < uint32(len(assetSizes)); i++ {
		assetSize := uint64(assetSizes[i])
		totalAssetSize = totalAssetSize + uint64(assetSize)
	}

	if compact {
		fmt.Printf("%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			versionIndexPath,
			versionIndex.GetVersion(),
			longtailutils.HashIdentifierToString(versionIndex.GetHashIdentifier()),
			versionIndex.GetTargetChunkSize(),
			versionIndex.GetAssetCount(),
			totalAssetSize,
			versionIndex.GetChunkCount(),
			totalChunkSize,
			averageChunkSize,
			smallestChunkSize,
			largestChunkSize)
	} else {
		fmt.Printf("Version:             %d\n", versionIndex.GetVersion())
		fmt.Printf("Hash Identifier:     %s\n", longtailutils.HashIdentifierToString(versionIndex.GetHashIdentifier()))
		fmt.Printf("Target Chunk Size:   %d\n", versionIndex.GetTargetChunkSize())
		fmt.Printf("Asset Count:         %d   (%s)\n", versionIndex.GetAssetCount(), longtailutils.ByteCountDecimal(uint64(versionIndex.GetAssetCount())))
		fmt.Printf("Asset Total Size:    %d   (%s)\n", totalAssetSize, longtailutils.ByteCountBinary(totalAssetSize))
		fmt.Printf("Chunk Count:         %d   (%s)\n", versionIndex.GetChunkCount(), longtailutils.ByteCountDecimal(uint64(versionIndex.GetChunkCount())))
		fmt.Printf("Chunk Total Size:    %d   (%s)\n", totalChunkSize, longtailutils.ByteCountBinary(totalChunkSize))
		fmt.Printf("Average Chunk Size:  %d   (%s)\n", averageChunkSize, longtailutils.ByteCountBinary(uint64(averageChunkSize)))
		fmt.Printf("Smallest Chunk Size: %d   (%s)\n", smallestChunkSize, longtailutils.ByteCountBinary(uint64(smallestChunkSize)))
		fmt.Printf("Largest Chunk Size:  %d   (%s)\n", largestChunkSize, longtailutils.ByteCountBinary(uint64(largestChunkSize)))
	}

	return storeStats, timeStats, nil
}

type PrintVersionCmd struct {
	VersionIndexPathOption
	S3EndpointResolverURLOption
	CompactOption
}

func (r *PrintVersionCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := printVersion(
		ctx.NumWorkerCount,
		r.VersionIndexPath,
		r.S3EndpointResolverURL,
		r.Compact)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
