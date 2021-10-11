package main

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/commands"
	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/alecthomas/kong"
	"github.com/sirupsen/logrus"
)

var cli struct {
	LogLevel                string                              `name:"log-level" help:"Log level [debug, info, warn, error]" enum:"debug, info, warn, error" default:"warn" `
	ShowStats               bool                                `name:"show-stats" help:"Output brief stats summary"`
	ShowStoreStats          bool                                `name:"show-store-stats" help:"Output detailed stats for block stores"`
	MemTrace                bool                                `name:"mem-trace" help:"Output summary memory statistics from longtail"`
	MemTraceDetailed        bool                                `name:"mem-trace-detailed" help:"Output detailed memory statistics from longtail"`
	MemTraceCSV             string                              `name:"mem-trace-csv" help:"Output path for detailed memory statistics from longtail in csv format"`
	WorkerCount             int                                 `name:"worker-count" help:"Limit number of workers created, defaults to match number of logical CPUs (zero for default count)" default:"0"`
	Upsync                  commands.UpsyncCmd                  `cmd:"" name:"upsync" help:"Upload a folder"`
	Downsync                commands.DownsyncCmd                `cmd:"" name:"downsync" help:"Download a folder"`
	Get                     commands.GetCmd                     `cmd:"" name:"get" help:"Download a folder using a get-config"`
	ValidateVersion         commands.ValidateVersionCmd         `cmd:"" name:"validate-version" help:"Validate a version index against a content store making sure all content needed is in the store" aliases:"validate"`
	PrintVersion            commands.PrintVersionCmd            `cmd:"" name:"print-version" help:"Print info about a version index" aliases:"printVersionIndex"`
	PrintStore              commands.PrintStoreCmd              `cmd:"" name:"print-store" help:"Print info about a store index" aliases:"printStoreIndex"`
	PrintVersionUsage       commands.PrintVersionUsageCmd       `cmd:"" name:"print-version-usage" help:"Shows block usage and asset fragmentaiton stats about a version index" aliases:"stats"`
	DumpVersionAssets       commands.DumpVersionAssetsCmd       `cmd:"" name:"dump-version-assets" help:"Lists all the asset paths inside a version index" aliases:"dump"`
	Ls                      commands.LsCmd                      `cmd:"" name:"ls" help:"List the content of a path inside a version index"`
	Cp                      commands.CpCmd                      `cmd:"" name:"cp" help:"Copies a file from inside a version index"`
	InitRemoteStore         commands.InitRemoteStoreCmd         `cmd:"" name:"init-remote-store" help:"Open/create a remote store and force rebuild the store index" aliases:"init"`
	CreateVersionStoreIndex commands.CreateVersionStoreIndexCmd `cmd:"" name:"create-version-store-index" help:"Create a store index optimized for a version index" aliases:"createVersionStoreIndex"`
	CloneStore              commands.CloneStoreCmd              `cmd:"" name:"clone-store" help:"Clone all the data needed to cover a set of versions from one store into a new store" aliases:"cloneStore"`
	PruneStore              commands.PruneStoreCmd              `cmd:"" name:"prune-store" help:"Prune blocks in a store which are not used by the files in the input list. CAUTION! Running uploads to a store that is being pruned may cause loss of the uploaded data" aliases:"pruneStore"`
}

func main() {
	executionStartTime := time.Now()
	initStartTime := executionStartTime

	context := &commands.Context{}

	defer func() {
		executionTime := time.Since(executionStartTime)
		context.TimeStats = append(context.TimeStats, longtailutils.TimeStat{"Execution", executionTime})

		if cli.ShowStoreStats {
			for _, s := range context.StoreStats {
				longtailutils.PrintStats(s.Name, s.Stats)
			}
		}

		if cli.ShowStats {
			maxLen := 0
			for _, s := range context.TimeStats {
				if len(s.Name) > maxLen {
					maxLen = len(s.Name)
				}
			}
			for _, s := range context.TimeStats {
				name := fmt.Sprintf("%s:", s.Name)
				log.Printf("%-*s %s", maxLen+1, name, s.Dur)
			}
		}
	}()

	ctx := kong.Parse(&cli)

	longtailLogLevel, err := longtailutils.ParseLevel(cli.LogLevel)
	if err != nil {
		log.Fatal(err)
	}

	switch cli.LogLevel {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	}

	longtaillib.SetLogger(&longtailutils.LoggerData{})
	defer longtaillib.SetLogger(nil)
	longtaillib.SetLogLevel(longtailLogLevel)

	longtaillib.SetAssert(&longtailutils.AssertData{})
	defer longtaillib.SetAssert(nil)

	if cli.WorkerCount == 0 {
		context.NumWorkerCount = runtime.NumCPU()
	} else {
		context.NumWorkerCount = cli.WorkerCount
	}

	if cli.MemTrace || cli.MemTraceDetailed || cli.MemTraceCSV != "" {
		longtaillib.EnableMemtrace()
		defer func() {
			memTraceLogLevel := longtaillib.MemTraceSummary
			if cli.MemTraceDetailed {
				memTraceLogLevel = longtaillib.MemTraceDetailed
			}
			if cli.MemTraceCSV != "" {
				longtaillib.MemTraceDumpStats(cli.MemTraceCSV)
			}
			memTraceLog := longtaillib.GetMemTraceStats(memTraceLogLevel)
			memTraceLines := strings.Split(memTraceLog, "\n")
			for _, l := range memTraceLines {
				if l == "" {
					continue
				}
				log.Printf("[MEM] %s", l)
			}
			longtaillib.DisableMemtrace()
		}()
	}

	initTime := time.Since(initStartTime)

	err = ctx.Run(context)

	context.TimeStats = append([]longtailutils.TimeStat{{"Init", initTime}}, context.TimeStats...)

	if err != nil {
		log.Fatal(err)
	}
}
