package commands

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func pop(
	numWorkerCount int,
	stackOffset uint32,
	retainPermissions bool,
	enableFileMapping bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "get"
	log := logrus.WithFields(logrus.Fields{
		"fname":             fname,
		"stackOffset":       stackOffset,
		"numWorkerCount":    numWorkerCount,
		"retainPermissions": retainPermissions,
		"enableFileMapping": enableFileMapping,
	})
	log.Debug(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	setupStartTime := time.Now()

	logFile := ".longtail/log"
	targetFolderPath := "."
	excludeFilterRegEx := "^\\.longtail(/|$)"
	blobStoreURI := ".longtail/store"

	history := ""
	_, err := os.Stat(logFile)
	if err == nil {
		historyBytes, err := ioutil.ReadFile(logFile)
		if err != nil {
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
		history = string(historyBytes)
	} else if !os.IsNotExist(err) {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	historyLines := make([]string, 0)
	if history != "" {
		historyLines = strings.Split(string(history), "\n")
	}

	if len(historyLines) < int(stackOffset+1) {
		err = fmt.Errorf("stack offset out of bounds for `%s`, log contains `%d` entries, need `%d` entries", logFile, len(historyLines), int(stackOffset+1))
		return storeStats, timeStats, errors.Wrap(err, fname)
	}

	sourceFilePath := historyLines[len(historyLines)-int(stackOffset+1)]
	versionLocalStoreIndexPath := sourceFilePath[:len(sourceFilePath)-3] + "lsi"

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, longtailutils.TimeStat{"Setup", setupTime})

	downSyncStoreStats, downSyncTimeStats, err := downsync(
		numWorkerCount,
		blobStoreURI,
		sourceFilePath,
		targetFolderPath,
		"",
		"",
		retainPermissions,
		false,
		versionLocalStoreIndexPath,
		"",
		excludeFilterRegEx,
		true,
		false,
		enableFileMapping)

	storeStats = append(storeStats, downSyncStoreStats...)
	timeStats = append(timeStats, downSyncTimeStats...)

	return storeStats, timeStats, errors.Wrap(err, fname)
}

type PopCmd struct {
	StackOffset uint32 `name:"offset" help:"Offset into log, zero equals top of stack (latest push)" default:"0"`
	RetainPermissionsOption
	EnableFileMappingOption
}

func (r *PopCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := pop(
		ctx.NumWorkerCount,
		r.StackOffset,
		r.RetainPermissions,
		r.EnableFileMapping)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
