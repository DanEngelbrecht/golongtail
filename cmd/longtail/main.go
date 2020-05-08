package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"

	"gopkg.in/alecthomas/kingpin.v2"
)

type loggerData struct {
}

func (l *loggerData) OnLog(level int, message string) {
	switch level {
	case 0:
		log.Printf("DEBUG: %s", message)
	case 1:
		log.Printf("INFO: %s", message)
	case 2:
		log.Printf("WARNING: %s", message)
	case 3:
		log.Printf("ERROR: %s", message)
	}
}

func parseLevel(lvl string) (int, error) {
	switch strings.ToLower(lvl) {
	case "debug":
		return 0, nil
	case "info":
		return 1, nil
	case "warn":
		return 2, nil
	case "error":
		return 3, nil
	case "off":
		return 4, nil
	}

	return -1, fmt.Errorf("not a valid log Level: %q", lvl)
}

type assertData struct {
}

func (a *assertData) OnAssert(expression string, file string, line int) {
	log.Fatalf("ASSERT: %s %s:%d", expression, file, line)
}

type progressData struct {
	inited     bool
	oldPercent uint32
	task       string
}

func (p *progressData) OnProgress(totalCount uint32, doneCount uint32) {
	if doneCount < totalCount {
		if !p.inited {
			fmt.Fprintf(os.Stderr, "%s: ", p.task)
			p.inited = true
		}
		percentDone := (100 * doneCount) / totalCount
		if (percentDone - p.oldPercent) >= 5 {
			fmt.Fprintf(os.Stderr, "%d%% ", percentDone)
			p.oldPercent = percentDone
		}
		return
	}
	if p.inited {
		if p.oldPercent != 100 {
			fmt.Fprintf(os.Stderr, "100%%")
		}
		fmt.Fprintf(os.Stderr, " Done\n")
	}
}

type getIndexCompletionAPI struct {
	wg           sync.WaitGroup
	contentIndex longtaillib.Longtail_ContentIndex
	err          int
}

func (a *getIndexCompletionAPI) OnComplete(contentIndex longtaillib.Longtail_ContentIndex, err int) {
	a.err = err
	a.contentIndex = contentIndex
	a.wg.Done()
}

func createBlockStoreForURI(uri string, jobAPI longtaillib.Longtail_JobAPI, targetBlockSize uint32, maxChunksPerBlock uint32, outFinalStats *longtaillib.BlockStoreStats) (longtaillib.Longtail_BlockStoreAPI, error) {
	blobStoreURL, err := url.Parse(uri)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			gcsBlockStore, err := longtailstorelib.NewGCSBlockStore(blobStoreURL, targetBlockSize, maxChunksPerBlock, outFinalStats)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, err
			}
			return longtaillib.CreateBlockStoreAPI(gcsBlockStore), nil
		case "s3":
			return longtaillib.Longtail_BlockStoreAPI{}, fmt.Errorf("AWS storage not yet implemented")
		case "abfs":
			return longtaillib.Longtail_BlockStoreAPI{}, fmt.Errorf("Azure Gen1 storage not yet implemented")
		case "abfss":
			return longtaillib.Longtail_BlockStoreAPI{}, fmt.Errorf("Azure Gen2 storage not yet implemented")
		case "file":
			fsBlockStore, err := longtailstorelib.NewFSBlockStore(blobStoreURL.Path[1:], jobAPI)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, err
			}
			return longtaillib.CreateBlockStoreAPI(fsBlockStore), nil
		}
	}
	return longtaillib.CreateFSBlockStore(longtaillib.CreateFSStorageAPI(), uri), nil
}

func createFileStorageForURI(uri string) (longtailstorelib.FileStorage, error) {
	blobStoreURL, err := url.Parse(uri)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			return longtailstorelib.NewGCSFileStorage(blobStoreURL)
		case "s3":
			return nil, fmt.Errorf("AWS storage not yet implemented")
		case "abfs":
			return nil, fmt.Errorf("Azure Gen1 storage not yet implemented")
		case "abfss":
			return nil, fmt.Errorf("Azure Gen2 storage not yet implemented")
		case "file":
			return longtailstorelib.NewFSFileStorage()
		}
	}

	return longtailstorelib.NewFSFileStorage()
}

const noCompressionType = uint32(0)

func getCompressionType(compressionAlgorithm *string) (uint32, error) {
	switch *compressionAlgorithm {
	case "none":
		return noCompressionType, nil
	case "brotli":
		return longtaillib.GetBrotliGenericDefaultCompressionType(), nil
	case "brotli_min":
		return longtaillib.GetBrotliGenericMinCompressionType(), nil
	case "brotli_max":
		return longtaillib.GetBrotliGenericMaxCompressionType(), nil
	case "brotli_text":
		return longtaillib.GetBrotliTextDefaultCompressionType(), nil
	case "brotli_text_min":
		return longtaillib.GetBrotliTextMinCompressionType(), nil
	case "brotli_text_max":
		return longtaillib.GetBrotliTextMaxCompressionType(), nil
	case "lz4":
		return longtaillib.GetLZ4DefaultCompressionType(), nil
	case "zstd":
		return longtaillib.GetZStdMaxCompressionType(), nil
	case "zstd_min":
		return longtaillib.GetZStdMinCompressionType(), nil
	case "zstd_max":
		return longtaillib.GetZStdMaxCompressionType(), nil
	}
	return 0, fmt.Errorf("Unsupported compression algorithm: `%s`", *compressionAlgorithm)
}

func getCompressionTypesForFiles(fileInfos longtaillib.Longtail_FileInfos, compressionType uint32) []uint32 {
	pathCount := fileInfos.GetFileCount()
	compressionTypes := make([]uint32, pathCount)
	for i := uint32(0); i < pathCount; i++ {
		compressionTypes[i] = compressionType
	}
	return compressionTypes
}

func getHashIdentifier(hashAlgorithm *string) (uint32, error) {
	switch *hashAlgorithm {
	case "meow":
		return longtaillib.GetMeowHashIdentifier(), nil
	case "blake2":
		return longtaillib.GetBlake2HashIdentifier(), nil
	case "blake3":
		return longtaillib.GetBlake3HashIdentifier(), nil
	}
	return 0, fmt.Errorf("not a supportd hash api: `%s`", *hashAlgorithm)
}

func byteCountDecimal(b uint64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %c", float64(b)/float64(div), "kMGTPE"[exp])
}

func byteCountBinary(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

//Include(rootPath string, assetFolder string, assetName string, isDir bool, size uint64, permissions uint16) bool
type regexPathFilter struct {
	compiledIncludeRegexes []*regexp.Regexp
	compiledExcludeRegexes []*regexp.Regexp
}

func (f *regexPathFilter) Include(rootPath string, assetPath string, assetName string, isDir bool, size uint64, permissions uint16) bool {
	for _, r := range f.compiledExcludeRegexes {
		if r.MatchString(assetPath) {
			log.Printf("INFO: Skipping `%s`", assetPath)
			return false
		}
	}
	if len(f.compiledIncludeRegexes) == 0 {
		return true
	}
	for _, r := range f.compiledIncludeRegexes {
		if r.MatchString(assetPath) {
			return true
		}
	}
	log.Printf("INFO: Skipping `%s`", assetPath)
	return false
}

func splitRegexes(regexes string) ([]*regexp.Regexp, error) {
	var compiledRegexes []*regexp.Regexp
	m := 0
	s := 0
	for i := 0; i < len(regexes); i++ {
		if (regexes)[i] == '\\' {
			m = -1
		} else if m == 0 && (regexes)[i] == '*' {
			m++
		} else if m == 1 && (regexes)[i] == '*' {
			r := (regexes)[s:(i - 1)]
			regex, err := regexp.Compile(r)
			if err != nil {
				return nil, err
			}
			compiledRegexes = append(compiledRegexes, regex)
			s = i + 1
			m = 0
		} else {
			m = 0
		}
	}
	if s < len(regexes) {
		r := (regexes)[s:]
		regex, err := regexp.Compile(r)
		if err != nil {
			return nil, err
		}
		compiledRegexes = append(compiledRegexes, regex)
	}
	return compiledRegexes, nil
}

func upSyncVersion(
	blobStoreURI string,
	sourceFolderPath string,
	sourceIndexPath *string,
	targetFilePath string,
	targetChunkSize uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	compressionAlgorithm *string,
	hashAlgorithm *string,
	includeFilterRegEx *string,
	excludeFilterRegEx *string,
	outFinalStats *longtaillib.BlockStoreStats) error {

	var pathFilter longtaillib.Longtail_PathFilterAPI

	if includeFilterRegEx != nil || excludeFilterRegEx != nil {
		regexPathFilter := &regexPathFilter{}
		if includeFilterRegEx != nil {
			compiledIncludeRegexes, err := splitRegexes(*includeFilterRegEx)
			if err != nil {
				return err
			}
			regexPathFilter.compiledIncludeRegexes = compiledIncludeRegexes
		}
		if excludeFilterRegEx != nil {
			compiledExcludeRegexes, err := splitRegexes(*excludeFilterRegEx)
			if err != nil {
				return err
			}
			regexPathFilter.compiledExcludeRegexes = compiledExcludeRegexes
		}
		if len(regexPathFilter.compiledIncludeRegexes) > 0 || len(regexPathFilter.compiledExcludeRegexes) > 0 {
			pathFilter = longtaillib.CreatePathFilterAPI(regexPathFilter)
		}
	}

	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobs.Dispose()
	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	remoteStore, err := createBlockStoreForURI(blobStoreURI, jobs, targetBlockSize, maxChunksPerBlock, outFinalStats)
	if err != nil {
		return err
	}
	defer remoteStore.Dispose()

	indexStore := longtaillib.CreateCompressBlockStore(remoteStore, creg)
	defer indexStore.Dispose()

	getIndexComplete := &getIndexCompletionAPI{}
	getIndexComplete.wg.Add(1)
	errno := indexStore.GetIndex(longtaillib.CreateAsyncGetIndexAPI(getIndexComplete))
	if errno != 0 {
		getIndexComplete.wg.Done()
		return fmt.Errorf("indexStore.GetIndex: Failed for `%s` failed with error %d", blobStoreURI, errno)
	}
	getIndexComplete.wg.Wait()
	if getIndexComplete.err != 0 {
		return fmt.Errorf("indexStore.GetIndex: Failed for `%s` failed with error %d", blobStoreURI, errno)
	}
	remoteContentIndex := getIndexComplete.contentIndex

	hashIdentifier := remoteContentIndex.GetHashIdentifier()
	if hashIdentifier == 0 {
		hashIdentifier, err = getHashIdentifier(hashAlgorithm)
		if err != nil {
			return err
		}
	}

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	hash, err := hashRegistry.GetHashAPI(hashIdentifier)
	if err != nil {
		return err
	}

	var vindex longtaillib.Longtail_VersionIndex
	if sourceIndexPath == nil || len(*sourceIndexPath) == 0 {
		fileInfos, err := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			sourceFolderPath)
		if err != nil {
			return err
		}
		defer fileInfos.Dispose()

		compressionType, err := getCompressionType(compressionAlgorithm)
		if err != nil {
			return err
		}
		compressionTypes := getCompressionTypesForFiles(fileInfos, compressionType)

		createVersionIndexProgress := longtaillib.CreateProgressAPI(&progressData{task: "Indexing version"})
		defer createVersionIndexProgress.Dispose()
		vindex, err = longtaillib.CreateVersionIndex(
			fs,
			hash,
			jobs,
			&createVersionIndexProgress,
			sourceFolderPath,
			fileInfos,
			compressionTypes,
			targetChunkSize)
		if err != nil {
			return err
		}
	} else {
		fileStorage, err := createFileStorageForURI(*sourceIndexPath)
		if err != nil {
			return nil
		}
		defer fileStorage.Close()
		vbuffer, err := fileStorage.ReadFromPath(context.Background(), *sourceIndexPath)
		if err != nil {
			return err
		}
		vindex, err = longtaillib.ReadVersionIndexFromBuffer(vbuffer)
		if err != nil {
			return err
		}
	}
	defer vindex.Dispose()

	missingContentIndex, err := longtaillib.CreateMissingContent(
		hash,
		remoteContentIndex,
		vindex,
		targetBlockSize,
		maxChunksPerBlock)
	if err != nil {
		return err
	}
	defer missingContentIndex.Dispose()
	if missingContentIndex.GetBlockCount() > 0 {
		writeContentProgress := longtaillib.CreateProgressAPI(&progressData{task: "Writing content blocks"})
		defer writeContentProgress.Dispose()

		err = longtaillib.WriteContent(
			fs,
			indexStore,
			jobs,
			&writeContentProgress,
			remoteContentIndex,
			missingContentIndex,
			vindex,
			sourceFolderPath)
		if err != nil {
			return err
		}
	}

	vbuffer, err := longtaillib.WriteVersionIndexToBuffer(vindex)
	if err != nil {
		return err
	}
	fileStorage, err := createFileStorageForURI(targetFilePath)
	if err != nil {
		return nil
	}
	defer fileStorage.Close()
	err = fileStorage.WriteToPath(context.Background(), targetFilePath, vbuffer)
	if err != nil {
		return err
	}
	return nil
}

func downSyncVersion(
	blobStoreURI string,
	sourceFilePath string,
	targetFolderPath string,
	targetIndexPath *string,
	localCachePath string,
	retainPermissions bool,
	includeFilterRegEx *string,
	excludeFilterRegEx *string,
	outFinalStats *longtaillib.BlockStoreStats) error {

	var pathFilter longtaillib.Longtail_PathFilterAPI

	if includeFilterRegEx != nil || excludeFilterRegEx != nil {
		regexPathFilter := &regexPathFilter{}
		if includeFilterRegEx != nil {
			compiledIncludeRegexes, err := splitRegexes(*includeFilterRegEx)
			if err != nil {
				return err
			}
			regexPathFilter.compiledIncludeRegexes = compiledIncludeRegexes
		}
		if excludeFilterRegEx != nil {
			compiledExcludeRegexes, err := splitRegexes(*excludeFilterRegEx)
			if err != nil {
				return err
			}
			regexPathFilter.compiledExcludeRegexes = compiledExcludeRegexes
		}
		if len(regexPathFilter.compiledIncludeRegexes) > 0 || len(regexPathFilter.compiledExcludeRegexes) > 0 {
			pathFilter = longtaillib.CreatePathFilterAPI(regexPathFilter)
		}
	}

	//	defer un(trace("downSyncVersion " + sourceFilePath))
	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobs.Dispose()
	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, jobs, 8388608, 1024, outFinalStats)
	if err != nil {
		return err
	}
	defer remoteIndexStore.Dispose()

	localFS := longtaillib.CreateFSStorageAPI()
	defer localFS.Dispose()

	localIndexStore := longtaillib.CreateFSBlockStore(localFS, localCachePath)
	if err != nil {
		return err
	}
	defer localIndexStore.Dispose()

	cacheBlockStore := longtaillib.CreateCacheBlockStore(localIndexStore, remoteIndexStore)
	defer cacheBlockStore.Dispose()

	compressBlockStore := longtaillib.CreateCompressBlockStore(cacheBlockStore, creg)
	defer compressBlockStore.Dispose()

	indexStore := longtaillib.CreateShareBlockStore(compressBlockStore)
	defer indexStore.Dispose()

	getIndexComplete := &getIndexCompletionAPI{}
	getIndexComplete.wg.Add(1)
	errno := indexStore.GetIndex(longtaillib.CreateAsyncGetIndexAPI(getIndexComplete))
	if errno != 0 {
		getIndexComplete.wg.Done()
		return fmt.Errorf("indexStore.GetIndex: Failed for `%s` failed with error %d", blobStoreURI, errno)
	}
	getIndexComplete.wg.Wait()
	if getIndexComplete.err != 0 {
		return fmt.Errorf("indexStore.GetIndex: Failed for `%s` failed with error %d", blobStoreURI, errno)
	}
	remoteContentIndex := getIndexComplete.contentIndex

	var remoteVersionIndex longtaillib.Longtail_VersionIndex

	{
		fileStorage, err := createFileStorageForURI(sourceFilePath)
		if err != nil {
			return nil
		}
		defer fileStorage.Close()
		vbuffer, err := fileStorage.ReadFromPath(context.Background(), sourceFilePath)
		if err != nil {
			return err
		}
		remoteVersionIndex, err = longtaillib.ReadVersionIndexFromBuffer(vbuffer)
		if err != nil {
			return err
		}
	}
	defer remoteVersionIndex.Dispose()

	hashIdentifier := remoteContentIndex.GetHashIdentifier()
	if hashIdentifier == 0 {
		hashIdentifier = remoteVersionIndex.GetHashIdentifier()
	} else if remoteVersionIndex.GetHashIdentifier() != hashIdentifier {
		return fmt.Errorf("Remote store hash algorithm (%d) does not match hash algorithm of version (%d)", remoteVersionIndex.GetHashIdentifier(), hashIdentifier)
	}

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	hash, err := hashRegistry.GetHashAPI(hashIdentifier)
	if err != nil {
		return err
	}

	var localVersionIndex longtaillib.Longtail_VersionIndex
	if targetIndexPath == nil || len(*targetIndexPath) == 0 {
		fileInfos, err := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			targetFolderPath)
		if err != nil {
			return err
		}
		defer fileInfos.Dispose()

		compressionTypes := getCompressionTypesForFiles(fileInfos, noCompressionType)

		createVersionIndexProgress := longtaillib.CreateProgressAPI(&progressData{task: "Indexing version"})
		defer createVersionIndexProgress.Dispose()
		localVersionIndex, err = longtaillib.CreateVersionIndex(
			fs,
			hash,
			jobs,
			&createVersionIndexProgress,
			targetFolderPath,
			fileInfos,
			compressionTypes,
			remoteVersionIndex.GetTargetChunkSize())
		if err != nil {
			return err
		}
	} else {
		fileStorage, err := createFileStorageForURI(*targetIndexPath)
		if err != nil {
			return nil
		}
		defer fileStorage.Close()
		vbuffer, err := fileStorage.ReadFromPath(context.Background(), *targetIndexPath)
		if err != nil {
			return err
		}
		localVersionIndex, err = longtaillib.ReadVersionIndexFromBuffer(vbuffer)
		if err != nil {
			return err
		}
	}
	defer localVersionIndex.Dispose()

	versionDiff, err := longtaillib.CreateVersionDiff(localVersionIndex, remoteVersionIndex)
	if err != nil {
		return err
	}
	defer versionDiff.Dispose()

	changeVersionProgress := longtaillib.CreateProgressAPI(&progressData{task: "Updating version"})
	defer changeVersionProgress.Dispose()
	err = longtaillib.ChangeVersion(
		indexStore,
		fs,
		hash,
		jobs,
		&changeVersionProgress,
		remoteContentIndex,
		localVersionIndex,
		remoteVersionIndex,
		versionDiff,
		targetFolderPath,
		retainPermissions)
	if err != nil {
		return err
	}
	return nil
}

var (
	logLevel           = kingpin.Flag("log-level", "Log level").Default("warn").Enum("debug", "info", "warn", "error")
	storageURI         = kingpin.Flag("storage-uri", "Storage URI (only GCS bucket URI supported)").Required().String()
	showStats          = kingpin.Flag("show-stats", "Output brief stats summary").Bool()
	includeFilterRegEx = kingpin.Flag("include-filter-regex", "Optional include regex filter for assets in --source-path on upsync and --target-path on downsync. Separate regexes with **").String()
	excludeFilterRegEx = kingpin.Flag("exclude-filter-regex", "Optional exclude regex filter for assets in --source-path on upsync and --target-path on downsync. Separate regexes with **").String()

	commandUpSync = kingpin.Command("upsync", "Upload a folder")
	hashing       = commandUpSync.Flag("hash-algorithm", "Hashing algorithm: blake2, blake3, meow").
			Default("blake3").
			Enum("meow", "blake2", "blake3")
	targetChunkSize   = commandUpSync.Flag("target-chunk-size", "Target chunk size").Default("32768").Uint32()
	targetBlockSize   = commandUpSync.Flag("target-block-size", "Target block size").Default("8388608").Uint32()
	maxChunksPerBlock = commandUpSync.Flag("max-chunks-per-block", "Max chunks per block").Default("1024").Uint32()
	sourceFolderPath  = commandUpSync.Flag("source-path", "Source folder path").Required().String()
	sourceIndexPath   = commandUpSync.Flag("source-index-path", "Optional pre-computed index of source-path").String()
	targetFilePath    = commandUpSync.Flag("target-path", "Target file uri").Required().String()
	compression       = commandUpSync.Flag("compression-algorithm", "Compression algorithm: none, brotli[_min|_max], brotli_text[_min|_max], lz4, ztd[_min|_max]").
				Default("zstd").
				Enum(
			"none",
			"brotli",
			"brotli_min",
			"brotli_max",
			"brotli_text",
			"brotli_text_min",
			"brotli_text_max",
			"lz4",
			"zstd",
			"zstd_min",
			"zstd_max")

	commandDownSync     = kingpin.Command("downsync", "Download a folder")
	localCachePath      = commandDownSync.Flag("cache-path", "Location for cached blocks").Default(path.Join(os.TempDir(), "longtail_block_store")).String()
	targetFolderPath    = commandDownSync.Flag("target-path", "Target folder path").Required().String()
	targetIndexPath     = commandUpSync.Flag("target-index-path", "Optional pre-computed index of target-path").String()
	sourceFilePath      = commandDownSync.Flag("source-path", "Source file uri").Required().String()
	noRetainPermissions = commandDownSync.Flag("no-retain-permissions", "Disable setting permission on file/directories from source").Bool()
)

func main() {
	kingpin.HelpFlag.Short('h')
	kingpin.CommandLine.DefaultEnvars()
	kingpin.Parse()

	longtailLogLevel, err := parseLevel(*logLevel)
	if err != nil {
		log.Fatal(err)
	}

	longtaillib.SetLogger(&loggerData{})
	defer longtaillib.SetLogger(nil)
	longtaillib.SetLogLevel(longtailLogLevel)

	longtaillib.SetAssert(&assertData{})
	defer longtaillib.SetAssert(nil)

	var stats longtaillib.BlockStoreStats

	switch kingpin.Parse() {
	case commandUpSync.FullCommand():
		err := upSyncVersion(
			*storageURI,
			*sourceFolderPath,
			sourceIndexPath,
			*targetFilePath,
			*targetChunkSize,
			*targetBlockSize,
			*maxChunksPerBlock,
			compression,
			hashing,
			includeFilterRegEx,
			excludeFilterRegEx,
			&stats)
		if err != nil {
			log.Fatal(err)
		}
	case commandDownSync.FullCommand():
		err := downSyncVersion(
			*storageURI,
			*sourceFilePath,
			*targetFolderPath,
			targetIndexPath,
			*localCachePath,
			!(*noRetainPermissions),
			includeFilterRegEx,
			excludeFilterRegEx,
			&stats)
		if err != nil {
			log.Fatal(err)
		}
	}

	if *showStats {
		log.Printf("STATS:\n")
		log.Printf("------------------\n")
		log.Printf("IndexGetCount:      %s\n", byteCountDecimal(stats.IndexGetCount))
		log.Printf("BlocksGetCount:     %s\n", byteCountDecimal(stats.BlocksGetCount))
		log.Printf("BlocksPutCount:     %s\n", byteCountDecimal(stats.BlocksPutCount))
		log.Printf("ChunksGetCount:     %s\n", byteCountDecimal(stats.ChunksGetCount))
		log.Printf("ChunksPutCount:     %s\n", byteCountDecimal(stats.ChunksPutCount))
		log.Printf("BytesGetCount:      %s\n", byteCountBinary(stats.BytesGetCount))
		log.Printf("BytesPutCount:      %s\n", byteCountBinary(stats.BytesPutCount))
		log.Printf("IndexGetRetryCount: %s\n", byteCountDecimal(stats.IndexGetRetryCount))
		log.Printf("BlockGetRetryCount: %s\n", byteCountDecimal(stats.BlockGetRetryCount))
		log.Printf("BlockPutRetryCount: %s\n", byteCountDecimal(stats.BlockPutRetryCount))
		log.Printf("IndexGetFailCount:  %s\n", byteCountDecimal(stats.IndexGetFailCount))
		log.Printf("BlockGetFailCount:  %s\n", byteCountDecimal(stats.BlockGetFailCount))
		log.Printf("BlockPutFailCount:  %s\n", byteCountDecimal(stats.BlockPutFailCount))
		log.Printf("------------------\n")
	}
}
