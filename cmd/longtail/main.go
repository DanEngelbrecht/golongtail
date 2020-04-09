package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
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

func (a *getIndexCompletionAPI) OnComplete(contentIndex longtaillib.Longtail_ContentIndex, err int) int {
	a.err = err
	a.contentIndex = contentIndex
	a.wg.Done()
	return 0
}

type managedBlockStore struct {
	BlockStore    longtaillib.BlockStoreAPI
	BlockStoreAPI longtaillib.Longtail_BlockStoreAPI
}

func (blockStore *managedBlockStore) Dispose() {
	blockStore.BlockStoreAPI.Dispose()
	//	blockStore.BlockStore.Close()
}

func createBlockStoreForURI(uri string, defaultHashAPI longtaillib.Longtail_HashAPI, jobAPI longtaillib.Longtail_JobAPI, targetBlockSize uint32, maxChunksPerBlock uint32) (managedBlockStore, error) {
	blobStoreURL, err := url.Parse(uri)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			gcsBlockStore, err := longtailstorelib.NewGCSBlockStore(blobStoreURL, defaultHashAPI, targetBlockSize, maxChunksPerBlock)
			if err != nil {
				return managedBlockStore{BlockStore: nil, BlockStoreAPI: longtaillib.Longtail_BlockStoreAPI{}}, err
			}
			blockStoreAPI := longtaillib.CreateBlockStoreAPI(gcsBlockStore)
			return managedBlockStore{BlockStore: gcsBlockStore, BlockStoreAPI: blockStoreAPI}, nil
		case "s3":
			return managedBlockStore{BlockStore: nil, BlockStoreAPI: longtaillib.Longtail_BlockStoreAPI{}}, fmt.Errorf("AWS storage not yet implemented")
		case "abfs":
			return managedBlockStore{BlockStore: nil, BlockStoreAPI: longtaillib.Longtail_BlockStoreAPI{}}, fmt.Errorf("Azure Gen1 storage not yet implemented")
		case "abfss":
			return managedBlockStore{BlockStore: nil, BlockStoreAPI: longtaillib.Longtail_BlockStoreAPI{}}, fmt.Errorf("Azure Gen2 storage not yet implemented")
		case "file":
			fsBlockStore, err := longtailstorelib.NewFSBlockStore(blobStoreURL.Path[1:], defaultHashAPI.GetIdentifier(), jobAPI)
			if err != nil {
				return managedBlockStore{BlockStore: nil, BlockStoreAPI: longtaillib.Longtail_BlockStoreAPI{}}, err
			}
			blockStoreAPI := longtaillib.CreateBlockStoreAPI(fsBlockStore)
			return managedBlockStore{BlockStore: fsBlockStore, BlockStoreAPI: blockStoreAPI}, nil
		}
	}
	return managedBlockStore{BlockStore: nil, BlockStoreAPI: longtaillib.CreateFSBlockStore(longtaillib.CreateFSStorageAPI(), uri)}, nil
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
		//	case "brotli":
		//		return longtaillib.GetBrotliGenericDefaultCompressionType(), nil
		//	case "brotli_min":
		//		return longtaillib.GetBrotliGenericMinCompressionType(), nil
		//	case "brotli_max":
		//		return longtaillib.GetBrotliGenericMaxCompressionType(), nil
		//	case "brotli_text":
		//		return longtaillib.GetBrotliTextDefaultCompressionType(), nil
		//	case "brotli_text_min":
		//		return longtaillib.GetBrotliTextMinCompressionType(), nil
		//	case "brotli_text_max":
		//		return longtaillib.GetBrotliTextMaxCompressionType(), nil
		//	case "lz4":
		//		return longtaillib.GetLZ4DefaultCompressionType(), nil
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

func createHashAPIFromIdentifier(hashIdentifier uint32) (longtaillib.Longtail_HashAPI, error) {
	if hashIdentifier == longtaillib.GetMeowHashIdentifier() {
		return longtaillib.CreateMeowHashAPI(), nil
	}
	if hashIdentifier == longtaillib.GetBlake2HashIdentifier() {
		return longtaillib.CreateBlake2HashAPI(), nil
	}
	if hashIdentifier == longtaillib.GetBlake3HashIdentifier() {
		return longtaillib.CreateBlake3HashAPI(), nil
	}
	return longtaillib.Longtail_HashAPI{}, fmt.Errorf("not a supported hash identifier: `%d`", hashIdentifier)
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

func createHashAPI(hashAlgorithm *string) (longtaillib.Longtail_HashAPI, error) {
	hashIdentifier, err := getHashIdentifier(hashAlgorithm)
	if err != nil {
		return longtaillib.Longtail_HashAPI{}, err
	}
	return createHashAPIFromIdentifier(hashIdentifier)
}

func byteCountDecimal(b uint64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
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
	showStats bool) error {
	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobs.Dispose()
	creg := longtaillib.CreateZStdCompressionRegistry()
	defer creg.Dispose()

	hashIdentifier, err := getHashIdentifier(hashAlgorithm)
	if err != nil {
		return err
	}

	defaultHashAPI, err := createHashAPIFromIdentifier(hashIdentifier)
	if err != nil {
		return err
	}
	defer defaultHashAPI.Dispose()

	remoteStore, err := createBlockStoreForURI(blobStoreURI, defaultHashAPI, jobs, targetBlockSize, maxChunksPerBlock)
	if err != nil {
		return err
	}
	defer remoteStore.Dispose()

	indexStore := longtaillib.CreateCompressBlockStore(remoteStore.BlockStoreAPI, creg)
	defer indexStore.Dispose()

	getIndexComplete := &getIndexCompletionAPI{}
	getIndexComplete.wg.Add(1)
	errno := indexStore.GetIndex(hashIdentifier, longtaillib.CreateAsyncGetIndexAPI(getIndexComplete))
	if errno != 0 {
		getIndexComplete.wg.Done()
		return fmt.Errorf("indexStore.GetIndex: Failed for `%s` failed with error %d", blobStoreURI, errno)
	}
	getIndexComplete.wg.Wait()
	if getIndexComplete.err != 0 {
		return fmt.Errorf("indexStore.GetIndex: Failed for `%s` failed with error %d", blobStoreURI, errno)
	}
	remoteContentIndex := getIndexComplete.contentIndex

	hash, err := createHashAPIFromIdentifier(remoteContentIndex.GetHashAPI())
	if err != nil {
		return err
	}
	defer hash.Dispose()

	var vindex longtaillib.Longtail_VersionIndex
	if sourceIndexPath == nil || len(*sourceIndexPath) == 0 {
		fileInfos, err := longtaillib.GetFilesRecursively(fs, longtaillib.Longtail_PathFilterAPI{}, sourceFolderPath)
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
			fileInfos.GetPaths(),
			fileInfos.GetFileSizes(),
			fileInfos.GetFilePermissions(),
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
	if showStats {
		stats, errno := indexStore.GetStats()
		if errno == 0 {
			log.Printf("STATS:\n------------------\n")
			log.Printf("IndexGetCount:  %s\n", byteCountDecimal(stats.IndexGetCount))
			log.Printf("BlocksGetCount: %s\n", byteCountDecimal(stats.BlocksGetCount))
			log.Printf("BlocksPutCount: %s\n", byteCountDecimal(stats.BlocksPutCount))
			log.Printf("ChunksGetCount: %s\n", byteCountDecimal(stats.ChunksGetCount))
			log.Printf("ChunksPutCount: %s\n", byteCountDecimal(stats.ChunksPutCount))
			log.Printf("BytesGetCount:  %s\n", byteCountBinary(stats.BytesGetCount))
			log.Printf("BytesPutCount:  %s\n", byteCountBinary(stats.BytesPutCount))
			log.Printf("------------------\n")
		}
	}
	return nil
}

func downSyncVersion(
	blobStoreURI string,
	sourceFilePath string,
	targetFolderPath string,
	targetIndexPath *string,
	localCachePath string,
	hashAlgorithm *string,
	retainPermissions bool,
	showStats bool) error {
	//	defer un(trace("downSyncVersion " + sourceFilePath))
	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobs.Dispose()
	creg := longtaillib.CreateZStdCompressionRegistry()
	defer creg.Dispose()

	hashIdentifier, err := getHashIdentifier(hashAlgorithm)
	if err != nil {
		return err
	}

	defaultHashAPI, err := createHashAPIFromIdentifier(hashIdentifier)
	if err != nil {
		return err
	}
	defer defaultHashAPI.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, defaultHashAPI, jobs, 524288, 1024)
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

	cacheBlockStore := longtaillib.CreateCacheBlockStore(localIndexStore, remoteIndexStore.BlockStoreAPI)
	defer cacheBlockStore.Dispose()

	indexStore := longtaillib.CreateCompressBlockStore(cacheBlockStore, creg)
	defer indexStore.Dispose()

	getIndexComplete := &getIndexCompletionAPI{}
	getIndexComplete.wg.Add(1)
	errno := indexStore.GetIndex(hashIdentifier, longtaillib.CreateAsyncGetIndexAPI(getIndexComplete))
	if errno != 0 {
		getIndexComplete.wg.Done()
		return fmt.Errorf("indexStore.GetIndex: Failed for `%s` failed with error %d", blobStoreURI, errno)
	}
	getIndexComplete.wg.Wait()
	if getIndexComplete.err != 0 {
		return fmt.Errorf("indexStore.GetIndex: Failed for `%s` failed with error %d", blobStoreURI, errno)
	}
	remoteContentIndex := getIndexComplete.contentIndex

	hash, err := createHashAPIFromIdentifier(remoteContentIndex.GetHashAPI())
	if err != nil {
		return err
	}
	defer hash.Dispose()

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

	var localVersionIndex longtaillib.Longtail_VersionIndex
	if targetIndexPath == nil || len(*targetIndexPath) == 0 {
		fileInfos, err := longtaillib.GetFilesRecursively(fs, longtaillib.Longtail_PathFilterAPI{}, targetFolderPath)
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
			fileInfos.GetPaths(),
			fileInfos.GetFileSizes(),
			fileInfos.GetFilePermissions(),
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
	if showStats {
		stats, errno := remoteIndexStore.BlockStore.GetStats()
		if errno == 0 {
			log.Printf("STATS:\n------------------\n")
			log.Printf("IndexGetCount:  %s\n", byteCountDecimal(stats.IndexGetCount))
			log.Printf("BlocksGetCount: %s\n", byteCountDecimal(stats.BlocksGetCount))
			log.Printf("BlocksPutCount: %s\n", byteCountDecimal(stats.BlocksPutCount))
			log.Printf("ChunksGetCount: %s\n", byteCountDecimal(stats.ChunksGetCount))
			log.Printf("ChunksPutCount: %s\n", byteCountDecimal(stats.ChunksPutCount))
			log.Printf("BytesGetCount:  %s\n", byteCountBinary(stats.BytesGetCount))
			log.Printf("BytesPutCount:  %s\n", byteCountBinary(stats.BytesPutCount))
			log.Printf("------------------\n")
		}
	}
	return nil
}

var (
	logLevel   = kingpin.Flag("log-level", "Log level").Default("warn").Enum("debug", "info", "warn", "error")
	storageURI = kingpin.Flag("storage-uri", "Storage URI (only GCS bucket URI supported)").Required().String()
	hashing    = kingpin.Flag("hash-algorithm", "Hashing algorithm: blake2, blake3, meow").
			Default("blake3").
			Enum("meow", "blake2", "blake3")
	showStats = kingpin.Flag("show-stats", "Output brief stats summary").Bool()

	commandUpSync     = kingpin.Command("upsync", "Upload a folder")
	targetChunkSize   = commandUpSync.Flag("target-chunk-size", "Target chunk size").Default("32768").Uint32()
	targetBlockSize   = commandUpSync.Flag("target-block-size", "Target block size").Default("524288").Uint32()
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
			*showStats)
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
			hashing,
			!(*noRetainPermissions),
			*showStats)
		if err != nil {
			log.Fatal(err)
		}
	}
}
