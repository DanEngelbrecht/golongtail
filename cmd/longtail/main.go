package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/alecthomas/kong"
)

type Context struct {
	NumWorkerCount int
	StoreStats     []longtailutils.StoreStat
	TimeStats      []longtailutils.TimeStat
}

type CompressionOption struct {
	Compression string `name:"compression-algorithm" help:"Compression algorithm [none brotli brotli_min brotli_max brotli_text brotli_text_min brotli_text_max lz4 zstd zstd_min zstd_max]" enum:"none,brotli,brotli_min,brotli_max,brotli_text,brotli_text_min,brotli_text_max,lz4,zstd,zstd_min,zstd_max" default:"zstd"`
}

type HashingOption struct {
	Hashing string `name:"hash-algorithm" help:"Hash algorithm [meow blake2 blake3]" enum:"meow,blake2,blake3" default:"blake3"`
}

type UpsyncIncludeRegExOption struct {
	IncludeFilterRegEx string `name:"include-filter-regex" help:"Optional include regex filter for assets in --source-path. Separate regexes with **"`
}

type DownsyncIncludeRegExOption struct {
	IncludeFilterRegEx string `name:"include-filter-regex" help:"Optional include regex filter for assets in --target-path on downsync. Separate regexes with **"`
}

type UpsyncExcludeRegExOption struct {
	ExcludeFilterRegEx string `name:"exclude-filter-regex" help:"Optional exclude regex filter for assets in --source-path on upsync. Separate regexes with **"`
}

type DownsyncExcludeRegExOption struct {
	ExcludeFilterRegEx string `name:"exclude-filter-regex" help:"Optional exclude regex filter for assets in --target-path on downsync. Separate regexes with **"`
}

type StorageURIOption struct {
	StorageURI string `name:"storage-uri" help"Storage URI (local file system, GCS and S3 bucket URI supported)" required:""`
}

type CachePathOption struct {
	CachePath string `name:"cache-path" help:"Location for cached blocks"`
}

type RetainPermissionsOption struct {
	RetainPermissions bool `name:"retain-permissions" negatable:"" help:"Set permission on file/directories from source" default:"true"`
}

type TargetPathOption struct {
	TargetPath string `name:"target-path" help:"Target folder path"`
}

type TargetIndexUriOption struct {
	TargetIndexPath string `name:"target-index-path" help:"Optional pre-computed index of target-path"`
}

type SourceUriOption struct {
	SourcePath string `name:"source-path" help:"Source file uri" required:""`
}

type ValidateTargetOption struct {
	Validate bool `name:"validate" help:"Validate target path once completed"`
}

type VersionLocalStoreIndexPathOption struct {
	VersionLocalStoreIndexPath string `name:"version-local-store-index-path" help:"Path to an optimized store index for this particular version. If the file can't be read it will fall back to the master store index"`
}

type VersionIndexPathOption struct {
	VersionIndexPath string `name:"version-index-path" help:"URI to version index (local file system, GCS and S3 bucket URI supported)"`
}

type CompactOption struct {
	Compact bool `name:"compact" help:"Show info in compact layout"`
}

type StoreIndexPathOption struct {
	StoreIndexPath string `name:"store-index-path" help:"URI to store index (local file system, GCS and S3 bucket URI supported)"`
}

type MinBlockUsagePercentOption struct {
	MinBlockUsagePercent uint32 `name:"min-block-usage-percent" help:"Minimum percent of block content than must match for it to be considered \"existing\". Default is zero = use all" default:"0"`
}

type TargetChunkSizeOption struct {
	TargetChunkSize uint32 `name:"target-chunk-size" help:"Target chunk size" default:"32768"`
}

type MaxChunksPerBlockOption struct {
	MaxChunksPerBlock uint32 `name:"max-chunks-per-block" help:"Max chunks per block" default:"1024"`
}

type TargetBlockSizeOption struct {
	TargetBlockSize uint32 `name:"target-block-size" help:"Target block size" default:"8388608"`
}

var cli struct {
	LogLevel                string                     `name:"log-level" help:"Log level [debug, info, warn, error]" enum:"debug, info, warn, error" default:"warn" `
	ShowStats               bool                       `name:"show-stats" help:"Output brief stats summary"`
	ShowStoreStats          bool                       `name:"show-store-stats" help:"Output detailed stats for block stores"`
	MemTrace                bool                       `name:"mem-trace" help:"Output summary memory statistics from longtail"`
	MemTraceDetailed        bool                       `name:"mem-trace-detailed" help:"Output detailed memory statistics from longtail"`
	MemTraceCSV             string                     `name:"mem-trace-csv" help:"Output path for detailed memory statistics from longtail in csv format"`
	WorkerCount             int                        `name:"worker-count" help:"Limit number of workers created, defaults to match number of logical CPUs (zero for default count)" default:"0"`
	Upsync                  UpsyncCmd                  `cmd:"" name:"upsync" help:"Upload a folder"`
	Downsync                DownsyncCmd                `cmd:"" name:"downsync" help:"Download a folder"`
	Get                     GetCmd                     `cmd:"" name:"get" help:"Download a folder using a get-config"`
	ValidateVersion         ValidateVersionCmd         `cmd:"" name:"validate-version" help:"Validate a version index against a content store making sure all content needed is in the store" aliases:"validate"`
	PrintVersion            PrintVersionCmd            `cmd:"" name:"print-version" help:"Print info about a version index" aliases:"printVersionIndex"`
	PrintStore              PrintStoreCmd              `cmd:"" name:"print-store" help:"Print info about a store index" aliases:"printStoreIndex"`
	PrintVersionUsage       PrintVersionUsageCmd       `cmd:"" name:"print-version-usage" help:"Shows block usage and asset fragmentaiton stats about a version index" aliases:"stats"`
	DumpVersionAssets       DumpVersionAssetsCmd       `cmd:"" name:"dump-version-assets" help:"Lists all the asset paths inside a version index" aliases:"dump"`
	Ls                      LsCmd                      `cmd:"" name:"ls" help:"List the content of a path inside a version index"`
	Cp                      CpCmd                      `cmd:"" name:"cp" help:"Copies a file from inside a version index"`
	InitRemoteStore         InitRemoteStoreCmd         `cmd:"" name:"init-remote-store" help:"Open/create a remote store and force rebuild the store index" aliases:"init"`
	CreateVersionStoreIndex CreateVersionStoreIndexCmd `cmd:"" name:"create-version-store-index" help:"Create a store index optimized for a version index" aliases:"createVersionStoreIndex"`
	CloneStore              CloneStoreCmd              `cmd:"" name:"clone-store" help:"Clone all the data needed to cover a set of versions from one store into a new store" aliases:"cloneStore"`
	PruneStore              PruneStoreCmd              `cmd:"" name:"prune-store" help:"Prune blocks in a store which are not used by the files in the input list. CAUTION! Running uploads to a store that is being pruned may cause loss of the uploaded data" aliases:"pruneStore"`
}

func main() {
	executionStartTime := time.Now()
	initStartTime := executionStartTime

	context := &Context{}

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

	longtaillib.SetLogger(&longtailutils.LoggerData{})
	defer longtaillib.SetLogger(nil)
	longtaillib.SetLogLevel(longtailLogLevel)

	longtaillib.SetAssert(&longtailutils.AssertData{})
	defer longtaillib.SetAssert(nil)

	if cli.WorkerCount != 0 {
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
