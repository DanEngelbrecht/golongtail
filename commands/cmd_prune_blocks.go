package commands

import (
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/sirupsen/logrus"
)

func pruneBlocks(
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
	return storeStats, timeStats, nil
}

type PruneBlocksCmd struct {
	StoreIndexPathOption
	BlocksRootPath string `name:"blocks-root-path" help:"Root path uri for all blocks to check" required:""`
	BlockExtension string `name:"block-extension" help:"The file extension to use when finding blocks" default:".lsb"`
	DryRun         bool   `name:"dry-run" help:"Don't prune, just show how many blocks would be deleted if prune was run"`
}

func (r *PruneBlocksCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := pruneBlocks(
		ctx.NumWorkerCount,
		r.StoreIndexPath,
		r.BlocksRootPath,
		r.BlockExtension,
		r.DryRun)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
