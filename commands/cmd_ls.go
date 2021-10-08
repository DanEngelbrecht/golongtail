package commands

import (
	"fmt"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func ls(
	numWorkerCount int,
	versionIndexPath string,
	commandLSVersionDir string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "ls"
	log := logrus.WithFields(logrus.Fields{
		"fname":               fname,
		"numWorkerCount":      numWorkerCount,
		"versionIndexPath":    versionIndexPath,
		"commandLSVersionDir": commandLSVersionDir,
	})
	log.Debug(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	readSourceStartTime := time.Now()
	vbuffer, err := longtailutils.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Cant parse version index from `%s`", versionIndexPath))
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Read source index", readSourceTime})

	setupStartTime := time.Now()
	hashIdentifier := versionIndex.GetHashIdentifier()

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Unsupported hash identifier `%d`", hashIdentifier))
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	fakeBlockStoreFS := longtaillib.CreateInMemStorageAPI()
	defer fakeBlockStoreFS.Dispose()

	fakeBlockStore := longtaillib.CreateFSBlockStore(jobs, fakeBlockStoreFS, "store")
	defer fakeBlockStoreFS.Dispose()

	storeIndex, errno := longtaillib.CreateStoreIndex(
		hash,
		versionIndex,
		1024*1024*1024,
		1024)

	blockStoreFS := longtaillib.CreateBlockStoreStorageAPI(
		hash,
		jobs,
		fakeBlockStore,
		storeIndex,
		versionIndex)
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Failed creating block store storage for `%s`", versionIndexPath))
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer blockStoreFS.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	searchDir := ""
	if commandLSVersionDir != "." {
		searchDir = commandLSVersionDir
	}

	iterator, errno := blockStoreFS.StartFind(searchDir)
	if errno == longtaillib.ENOENT {
		return storeStats, timeStats, nil
	}
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Failed scanning dir `%s`", searchDir))
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer blockStoreFS.CloseFind(iterator)
	for {
		properties, errno := blockStoreFS.GetEntryProperties(iterator)
		if errno != 0 {
			err = longtailutils.MakeError(errno, fmt.Sprintf("Can't get properties of entry in `%s`", searchDir))
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
		detailsString := longtailutils.GetDetailsString(properties.Name, properties.Size, properties.Permissions, properties.IsDir, 16)
		fmt.Printf("%s\n", detailsString)

		errno = blockStoreFS.FindNext(iterator)
		if errno == longtaillib.ENOENT {
			break
		}
		if errno != 0 {
			err = longtailutils.MakeError(errno, fmt.Sprintf("Can't step to next entry in `%s`", searchDir))
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
	}
	return storeStats, timeStats, nil
}

type LsCmd struct {
	VersionIndexPathOption
	Path string `name:"path" arg:"" optional:"" help:"Path inside the version index to list"`
}

func (r *LsCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := ls(
		ctx.NumWorkerCount,
		r.VersionIndexPath,
		r.Path)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
