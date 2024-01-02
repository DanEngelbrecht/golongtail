package commands

import "github.com/DanEngelbrecht/golongtail/longtailutils"

type Context struct {
	NumWorkerCount int
	StoreStats     []longtailutils.StoreStat
	TimeStats      []longtailutils.TimeStat
}

type CompressionOption struct {
	Compression string `name:"compression-algorithm" help:"Compression algorithm [none brotli brotli_min brotli_max brotli_text brotli_text_min brotli_text_max lz4 zstd zstd_min zstd_max zstd_low zstd_high]" enum:"none,brotli,brotli_min,brotli_max,brotli_text,brotli_text_min,brotli_text_max,lz4,zstd,zstd_min,zstd_max,zstd_low,zstd_high" default:"zstd"`
}

type HashingOption struct {
	Hashing string `name:"hash-algorithm" help:"Hash algorithm [meow blake2 blake3]" enum:"meow,blake2,blake3" default:"blake3"`
}

type SourcePathIncludeRegExOption struct {
	IncludeFilterRegEx string `name:"include-filter-regex" help:"Optional include regex filter for assets in --source-path. Separate regexes with **"`
}

type TargetPathIncludeRegExOption struct {
	IncludeFilterRegEx string `name:"include-filter-regex" help:"Optional include regex filter for assets in --target-path on downsync. Separate regexes with **"`
}

type SourcePathExcludeRegExOption struct {
	ExcludeFilterRegEx string `name:"exclude-filter-regex" help:"Optional exclude regex filter for assets in --source-path on upsync. Separate regexes with **"`
}

type TargetPathExcludeRegExOption struct {
	ExcludeFilterRegEx string `name:"exclude-filter-regex" help:"Optional exclude regex filter for assets in --target-path on downsync. Separate regexes with **"`
}

type StorageURIOption struct {
	StorageURI string `name:"storage-uri" help"Storage URI (local file system, GCS and S3 bucket URI supported)" required:""`
}

type S3EndpointResolverURLOption struct {
	S3EndpointResolverURL string `name:"s3-endpoint-resolver-uri" help"Optional URI for S3 endpoint resolver"`
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

type MultiSourceUrisOption struct {
	SourcePaths []string `name:"source-path" help:"Source file uri(s)" required:"" sep:" "`
}

type ValidateTargetOption struct {
	Validate bool `name:"validate" help:"Validate target path once completed"`
}

type VersionLocalStoreIndexPathOption struct {
	VersionLocalStoreIndexPath string `name:"version-local-store-index-path" help:"Path to an optimized store index for this particular version. If the file cant be read it will fall back to the master store index"`
}

type MultiVersionLocalStoreIndexPathsOption struct {
	VersionLocalStoreIndexPaths []string `name:"version-local-store-index-path" help:"Path(s) to an optimized store index matching the source. If any of the file(s) cant be read it will fall back to the master store index" sep:" "`
}

type VersionIndexPathOption struct {
	VersionIndexPath string `name:"version-index-path" required:"" help:"URI to version index (local file system, GCS and S3 bucket URI supported)"`
}

type CompactOption struct {
	Compact bool `name:"compact" help:"Show info in compact layout"`
}

type StoreIndexPathOption struct {
	StoreIndexPath string `name:"store-index-path" required:"" help:"URI to store index (local file system, GCS and S3 bucket URI supported)"`
}

type MinBlockUsagePercentOption struct {
	MinBlockUsagePercent uint32 `name:"min-block-usage-percent" help:"Minimum percent of block content than must match for it to be considered \"existing\". Default is 80, allowing for up to 20% redundant data in blocks. Use 0 to use any block use all and 100 for no redundant data in blocks" default:"80"`
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

type ScanTargetOption struct {
	ScanTarget bool `name:"scan-target" help:"Enables scanning of target folder before write. Disable it to only add/write content to a folder" default:"true" negatable:""`
}

type CacheTargetIndexOption struct {
	CacheTargetIndex bool `name:"cache-target-index" help:"Stores a copy version index for the target folder and uses it if it exists, skipping folder scanning" default:"true" negatable:""`
}

type EnableFileMappingOption struct {
	EnableFileMapping bool `name:"enable-file-mapping" help:"Enabled memory mapped file for file reads"`
}

type UseLegacyWriteOption struct {
	UseLegacyWrite bool `name:"use-legacy-write" help:"Uses legacy algorithm to update a version"`
}
