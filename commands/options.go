package commands

import "github.com/DanEngelbrecht/golongtail/longtailutils"

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