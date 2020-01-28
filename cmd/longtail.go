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
	"time"

	"github.com/DanEngelbrecht/golongtail/lib"
	"github.com/DanEngelbrecht/golongtail/store"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"
)

type loggerData struct {
}

func logger(context interface{}, level int, message string) {
	switch level {
	case 0:
		log.Printf("DEBUG: %s", message)
	case 1:
		log.Printf("INFO: %s", message)
	case 2:
		log.Printf("WARNING: %s", message)
	case 3:
		log.Fatal(message)
	}
}

type progressData struct {
	inited     bool
	oldPercent int
	task       string
}

func progress(context interface{}, total int, current int) {
	p := context.(*progressData)
	if current < total {
		if !p.inited {
			fmt.Fprintf(os.Stderr, "%s: ", p.task)
			p.inited = true
		}
		percentDone := (100 * current) / total
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

func trace(s string) (string, time.Time) {
	return s, time.Now()
}

func un(s string, startTime time.Time) {
	elapsed := time.Since(startTime)
	log.Printf("%s: elapsed %f secs\n", s, elapsed.Seconds())
}

func createBlobStoreForURI(uri string) (store.BlobStore, error) {
	blobStoreURL, err := url.Parse(*storageURI)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			return store.NewGCSBlobStore(blobStoreURL)
		case "s3":
			return nil, fmt.Errorf("AWS storage not yet implemented")
		case "abfs":
			return nil, fmt.Errorf("Azure Gen1 storage not yet implemented")
		case "abfss":
			return nil, fmt.Errorf("Azure Gen2 storage not yet implemented")
		case "file":
			return store.NewFSBlobStore(blobStoreURL.Path[1:])
		}
	}
	return store.NewFSBlobStore(uri)
}

const noCompressionType = uint32(0)

func getCompressionType(compressionAlgorithm *string) (uint32, error) {
	switch *compressionAlgorithm {
	case "none":
		return noCompressionType, nil
	case "brotli":
		return lib.GetBrotliGenericDefaultCompressionType(), nil
	case "brotli_min":
		return lib.GetBrotliGenericMinCompressionType(), nil
	case "brotli_max":
		return lib.GetBrotliGenericMaxCompressionType(), nil
	case "brotli_text":
		return lib.GetBrotliTextDefaultCompressionType(), nil
	case "brotli_text_min":
		return lib.GetBrotliTextMinCompressionType(), nil
	case "brotli_text_max":
		return lib.GetBrotliTextMaxCompressionType(), nil
	case "lizard":
		return lib.GetLizardDefaultCompressionType(), nil
	case "lizard_min":
		return lib.GetLizardMinCompressionType(), nil
	case "lizard_max":
		return lib.GetLizardDefaultCompressionType(), nil
	case "lz4":
		return lib.GetLZ4DefaultCompressionType(), nil
	case "zstd":
		return lib.GetZStdMaxCompressionType(), nil
	case "zstd_min":
		return lib.GetZStdMinCompressionType(), nil
	case "zstd_max":
		return lib.GetZStdMaxCompressionType(), nil
	}
	return 0, fmt.Errorf("Unsupported compression algorithm: `%s`", *compressionAlgorithm)
}

func getCompressionTypesForFiles(fileInfos lib.Longtail_FileInfos, compressionType uint32) []uint32 {
	pathCount := fileInfos.GetFileCount()
	compressionTypes := make([]uint32, pathCount)
	for i := uint32(0); i < pathCount; i++ {
		compressionTypes[i] = compressionType
	}
	return compressionTypes
}

func createHashAPIFromIdentifier(hashIdentifier uint32) (lib.Longtail_HashAPI, error) {
	if hashIdentifier == lib.GetMeowHashIdentifier() {
		return lib.CreateMeowHashAPI(), nil
	}
	if hashIdentifier == lib.GetBlake2HashIdentifier() {
		return lib.CreateBlake2HashAPI(), nil
	}
	if hashIdentifier == lib.GetBlake3HashIdentifier() {
		return lib.CreateBlake3HashAPI(), nil
	}
	return lib.Longtail_HashAPI{}, fmt.Errorf("not a supported hash identifier: `%d`", hashIdentifier)
}

func createHashAPI(hashAlgorithm *string) (lib.Longtail_HashAPI, error) {
	switch *hashAlgorithm {
	case "meow":
		return createHashAPIFromIdentifier(lib.GetMeowHashIdentifier())
	case "blake2":
		return createHashAPIFromIdentifier(lib.GetBlake2HashIdentifier())
	case "blake3":
		return createHashAPIFromIdentifier(lib.GetBlake3HashIdentifier())
	}
	return lib.Longtail_HashAPI{}, fmt.Errorf("not a supportd hash api: `%s`", *hashAlgorithm)
}

func upSyncVersion(
	blobStoreURI string,
	sourceFolderPath string,
	targetFilePath string,
	localCachePath string,
	targetChunkSize uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	compressionAlgorithm *string,
	hashAlgorithm *string) error {
	//	defer un(trace("upSyncVersion " + targetFilePath))
	fs := lib.CreateFSStorageAPI()
	defer fs.Dispose()
	jobs := lib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobs.Dispose()
	creg := lib.CreateDefaultCompressionRegistry()
	defer creg.Dispose()

	//	log.Printf("Connecting to `%s`\n", blobStoreURI)
	indexStore, err := createBlobStoreForURI(blobStoreURI)
	if err != nil {
		return err
	}
	defer indexStore.Close()

	var hash lib.Longtail_HashAPI
	//	log.Printf("Fetching remote store index from `%s`\n", "store.lci")
	var remoteContentIndex lib.Longtail_ContentIndex
	remoteContentIndexBlob, err := indexStore.GetBlob(context.Background(), "store.lci")
	if err == nil {
		remoteContentIndex, err = lib.ReadContentIndexFromBuffer(remoteContentIndexBlob)
		if err != nil {
			return errors.Wrap(err, blobStoreURI+"/store.lci")
		}
		hash, err = createHashAPIFromIdentifier(remoteContentIndex.GetHashAPI())
		if err != nil {
			return err
		}
	} else {
		hash, err = createHashAPI(hashAlgorithm)
		if err != nil {
			return err
		}
		remoteContentIndex, err = lib.CreateContentIndex(
			hash,
			0,
			nil,
			nil,
			nil,
			0,
			0)
		if err != nil {
			return err
		}
	}
	defer hash.Dispose()

	//	log.Printf("Indexing files and folders in `%s`\n", sourceFolderPath)
	fileInfos, err := lib.GetFilesRecursively(fs, sourceFolderPath)
	if err != nil {
		return err
	}
	defer fileInfos.Dispose()

	//pathCount := fileInfos.GetFileCount()
	//	log.Printf("Found %d assets\n", int(pathCount))

	compressionType, err := getCompressionType(compressionAlgorithm)
	if err != nil {
		return err
	}
	compressionTypes := getCompressionTypesForFiles(fileInfos, compressionType)

	//	log.Printf("Indexing `%s`\n", sourceFolderPath)
	vindex, err := lib.CreateVersionIndex(
		fs,
		hash,
		jobs,
		progress,
		&progressData{task: "Indexing version"},
		sourceFolderPath,
		fileInfos.GetPaths(),
		fileInfos.GetFileSizes(),
		fileInfos.GetFilePermissions(),
		compressionTypes,
		targetChunkSize)
	if err != nil {
		return err
	}
	defer vindex.Dispose()

	versionBlob, err := lib.WriteVersionIndexToBuffer(vindex)
	if err != nil {
		return err
	}

	missingContentIndex, err := lib.CreateMissingContent(
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
		err = lib.WriteContent(
			fs,
			fs,
			creg,
			jobs,
			progress,
			&progressData{task: "Writing content blocks"},
			missingContentIndex,
			vindex,
			sourceFolderPath,
			localCachePath)
		if err != nil {
			return err
		}
		err = indexStore.PutContent(
			context.Background(),
			progress,
			&progressData{task: "Uploading"},
			missingContentIndex,
			fs,
			localCachePath)
		if err != nil {
			return err
		}
	}

	err = indexStore.PutBlob(
		context.Background(),
		targetFilePath,
		"application/octet-stream",
		versionBlob)
	if err != nil {
		return err
	}

	return nil
}

func downSyncVersion(
	blobStoreURI string,
	sourceFilePath string,
	targetFolderPath string,
	localCachePath string,
	targetChunkSize uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	hashAlgorithm *string,
	retainPermissions bool) error {
	//	defer un(trace("downSyncVersion " + sourceFilePath))
	fs := lib.CreateFSStorageAPI()
	defer fs.Dispose()
	jobs := lib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobs.Dispose()
	creg := lib.CreateDefaultCompressionRegistry()
	defer creg.Dispose()

	//	log.Printf("Connecting to `%v`\n", blobStoreURI)
	var indexStore store.BlobStore
	indexStore, err := createBlobStoreForURI(blobStoreURI)
	if err != nil {
		return err
	}
	defer indexStore.Close()

	var hash lib.Longtail_HashAPI
	//log.Printf("Fetching remote store index from `%s`\n", "store.lci")
	var remoteContentIndex lib.Longtail_ContentIndex
	remoteContentIndexBlob, err := indexStore.GetBlob(context.Background(), "store.lci")
	if err == nil {
		remoteContentIndex, err = lib.ReadContentIndexFromBuffer(remoteContentIndexBlob)
		if err != nil {
			errors.Wrap(err, blobStoreURI+"/store.lci")
		}
		hash, err = createHashAPIFromIdentifier(remoteContentIndex.GetHashAPI())
		if err != nil {
			return err
		}
	} else {
		hash, err = createHashAPI(hashAlgorithm)
		if err != nil {
			return err
		}
		remoteContentIndex, err = lib.CreateContentIndex(
			hash,
			0,
			nil,
			nil,
			nil,
			0,
			0)
		if err != nil {
			return err
		}
	}
	defer hash.Dispose()

	var remoteVersionIndex lib.Longtail_VersionIndex

	//	log.Printf("Fetching remote version index from `%s`\n", sourceFilePath)
	remoteVersionBlob, err := indexStore.GetBlob(context.Background(), sourceFilePath)
	if err != nil {
		return err
	}
	remoteVersionIndex, err = lib.ReadVersionIndexFromBuffer(remoteVersionBlob)
	if err != nil {
		return err
	}

	//	log.Printf("Indexing files and folders in `%s`\n", targetFolderPath)
	fileInfos, err := lib.GetFilesRecursively(fs, targetFolderPath)
	if err != nil {
		return err
	}
	defer fileInfos.Dispose()

	//pathCount := fileInfos.GetFileCount()
	//	log.Printf("Found %d assets\n", int(pathCount))

	compressionTypes := getCompressionTypesForFiles(fileInfos, noCompressionType)

	localVersionIndex, err := lib.CreateVersionIndex(
		fs,
		hash,
		jobs,
		progress,
		&progressData{task: "Indexing version"},
		targetFolderPath,
		fileInfos.GetPaths(),
		fileInfos.GetFileSizes(),
		fileInfos.GetFilePermissions(),
		compressionTypes,
		targetChunkSize)
	if err != nil {
		return err
	}
	defer localVersionIndex.Dispose()

	localContentIndex, err := lib.ReadContent(
		fs,
		hash,
		jobs,
		progress,
		&progressData{task: "Scanning local blocks"},
		localCachePath)
	if err != nil {
		return err
	}
	defer localContentIndex.Dispose()

	missingContentIndex, err := lib.CreateMissingContent(
		hash,
		localContentIndex,
		remoteVersionIndex,
		targetBlockSize,
		maxChunksPerBlock)
	if err != nil {
		return err
	}
	defer missingContentIndex.Dispose()

	if missingContentIndex.GetBlockCount() > 0 {
		neededContentIndex, err := lib.RetargetContent(remoteContentIndex, missingContentIndex)
		if err != nil {
			return err
		}
		defer neededContentIndex.Dispose()

		err = indexStore.GetContent(
			context.Background(),
			progress,
			&progressData{task: "Downloading"},
			neededContentIndex,
			fs,
			localCachePath)
		if err != nil {
			return err
		}

		mergedContentIndex, err := lib.MergeContentIndex(localContentIndex, neededContentIndex)
		if err != nil {
			return err
		}
		localContentIndex.Dispose()
		localContentIndex = mergedContentIndex
	}

	versionDiff, err := lib.CreateVersionDiff(localVersionIndex, remoteVersionIndex)
	if err != nil {
		return err
	}
	defer versionDiff.Dispose()

	err = lib.ChangeVersion(
		fs,
		fs,
		hash,
		jobs,
		progress,
		&progressData{task: "Updating version"},
		creg,
		localContentIndex,
		localVersionIndex,
		remoteVersionIndex,
		versionDiff,
		localCachePath,
		targetFolderPath,
		retainPermissions)
	if err != nil {
		return err
	}
	return nil
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

var (
	logLevel          = kingpin.Flag("log-level", "Log level").Default("warn").Enum("debug", "info", "warn", "error")
	targetChunkSize   = kingpin.Flag("target-chunk-size", "Target chunk size").Default("32768").Uint32()
	targetBlockSize   = kingpin.Flag("target-block-size", "Target block size").Default("524288").Uint32()
	maxChunksPerBlock = kingpin.Flag("max-chunks-per-block", "Max chunks per block").Default("1024").Uint32()
	storageURI        = kingpin.Flag("storage-uri", "Storage URI (only GCS bucket URI supported)").String()
	hashing           = kingpin.Flag("hash-algorithm", "Hashing algorithm: blake2, blake3, meow").
				Default("blake3").
				Enum("meow", "blake2", "blake3")

	commandUpSync     = kingpin.Command("upsync", "Upload a folder")
	upSyncContentPath = commandUpSync.Flag("content-path", "Location to store blocks prepared for upload").Default(path.Join(os.TempDir(), "longtail_block_store")).String()
	sourceFolderPath  = commandUpSync.Flag("source-path", "Source folder path").String()
	targetFilePath    = commandUpSync.Flag("target-path", "Target file path relative to --storage-uri").String()
	compression       = commandUpSync.Flag("compression-algorithm", "Compression algorithm: none, brotli[_min|_max], brotli_text[_min|_max], lizard[_min|_max], lz4, ztd[_min|_max]").
				Default("zstd").
				Enum(
			"none",
			"brotli",
			"brotli_min",
			"brotli_max",
			"brotli_text",
			"brotli_text_min",
			"brotli_text_max",
			"lizard",
			"lizard_min",
			"lizard_max",
			"lz4",
			"zstd",
			"zstd_min",
			"zstd_max")

	commandDownSync     = kingpin.Command("downsync", "Download a folder")
	downSyncContentPath = commandDownSync.Flag("content-path", "Location for downloaded/cached blocks").Default(path.Join(os.TempDir(), "longtail_block_store")).String()
	targetFolderPath    = commandDownSync.Flag("target-path", "Target folder path").String()
	sourceFilePath      = commandDownSync.Flag("source-path", "Source file path relative to --storage-uri").String()
	noRetainPermissions = commandDownSync.Flag("no-retain-permissions", "Disable setting permission on file/directories from source").Bool()
)

func cmdAssertFunc(context interface{}, expression string, file string, line int) {
	log.Fatalf("ASSERT: %s %s:%d", expression, file, line)
}

func main() {
	kingpin.HelpFlag.Short('h')
	kingpin.CommandLine.DefaultEnvars()
	kingpin.Parse()

	longtailLogLevel, err := parseLevel(*logLevel)
	if err != nil {
		log.Fatal(err)
	}

	l := lib.SetLogger(logger, &loggerData{})
	defer lib.ClearLogger(l)
	lib.SetLogLevel(longtailLogLevel)

	lib.SetAssert(cmdAssertFunc, nil)
	defer lib.ClearAssert()

	switch kingpin.Parse() {
	case commandUpSync.FullCommand():
		err := upSyncVersion(*storageURI, *sourceFolderPath, *targetFilePath, *upSyncContentPath, *targetChunkSize, *targetBlockSize, *maxChunksPerBlock, compression, hashing)
		if err != nil {
			log.Fatal(err)
		}
	case commandDownSync.FullCommand():
		err := downSyncVersion(*storageURI, *sourceFilePath, *targetFolderPath, *downSyncContentPath, *targetChunkSize, *targetBlockSize, *maxChunksPerBlock, hashing, !(*noRetainPermissions))
		if err != nil {
			log.Fatal(err)
		}
	}
}
