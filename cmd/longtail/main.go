package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"gopkg.in/alecthomas/kingpin.v2"
)

type loggerData struct {
}

var numWorkerCount = runtime.NumCPU()

var logLevelNames = [...]string{"DEBUG", "INFO", "WARNING", "ERROR", "OFF"}

func (l *loggerData) OnLog(file string, function string, line int, level int, logFields []longtaillib.LogField, message string) {
	var b strings.Builder
	b.Grow(32 + len(message))
	fmt.Fprintf(&b, "{")
	fmt.Fprintf(&b, "\"file\": \"%s\"", file)
	fmt.Fprintf(&b, ", \"func\": \"%s\"", function)
	fmt.Fprintf(&b, ", \"line\": \"%d\"", line)
	fmt.Fprintf(&b, ", \"level\": \"%s\"", logLevelNames[level])
	for _, field := range logFields {
		fmt.Fprintf(&b, ", \"%s\": \"%s\"", field.Name, field.Value)
	}
	fmt.Fprintf(&b, ", \"msg\": \"%s\"", message)
	fmt.Fprintf(&b, "}")
	log.Printf("%s", b.String())
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

	return -1, errors.Wrapf(longtaillib.ErrnoToError(longtaillib.EIO, longtaillib.ErrEIO), "not a valid log Level: %s", lvl)
}

func normalizePath(path string) string {
	doubleForwardRemoved := strings.Replace(path, "//", "/", -1)
	doubleBackwardRemoved := strings.Replace(doubleForwardRemoved, "\\\\", "/", -1)
	backwardRemoved := strings.Replace(doubleBackwardRemoved, "\\", "/", -1)
	return backwardRemoved
}

type assertData struct {
}

func (a *assertData) OnAssert(expression string, file string, line int) {
	log.Fatalf("ASSERT: %s %s:%d", expression, file, line)
}

type progressData struct {
	inited bool
	task   string
}

func (p *progressData) OnProgress(totalCount uint32, doneCount uint32) {
	if doneCount == totalCount {
		if p.inited {
			fmt.Fprintf(os.Stderr, "100%%")
			fmt.Fprintf(os.Stderr, " Done\n")
		}
		return
	}
	if !p.inited {
		fmt.Fprintf(os.Stderr, "%s: ", p.task)
		p.inited = true
	}
	percentDone := (100 * doneCount) / totalCount
	fmt.Fprintf(os.Stderr, "%d%% ", percentDone)
}

func CreateProgress(task string) longtaillib.Longtail_ProgressAPI {
	baseProgress := longtaillib.CreateProgressAPI(&progressData{task: task})
	return longtaillib.CreateRateLimitedProgressAPI(baseProgress, 5)
}

type timeStat struct {
	name string
	dur  time.Duration
}

type storeStat struct {
	name  string
	stats longtaillib.BlockStoreStats
}

type getExistingContentCompletionAPI struct {
	wg         sync.WaitGroup
	storeIndex longtaillib.Longtail_StoreIndex
	err        int
}

func (a *getExistingContentCompletionAPI) OnComplete(storeIndex longtaillib.Longtail_StoreIndex, err int) {
	a.err = err
	a.storeIndex = storeIndex
	a.wg.Done()
}

type flushCompletionAPI struct {
	wg  sync.WaitGroup
	err int
}

func (a *flushCompletionAPI) OnComplete(err int) {
	a.err = err
	a.wg.Done()
}

type getStoredBlockCompletionAPI struct {
	wg          sync.WaitGroup
	storedBlock longtaillib.Longtail_StoredBlock
	err         int
}

func (a *getStoredBlockCompletionAPI) OnComplete(storedBlock longtaillib.Longtail_StoredBlock, err int) {
	a.err = err
	a.storedBlock = storedBlock
	a.wg.Done()
}

func printStats(name string, stats longtaillib.BlockStoreStats) {
	log.Printf("%s:\n", name)
	log.Printf("------------------\n")
	log.Printf("GetStoredBlock_Count:          %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]))
	log.Printf("GetStoredBlock_RetryCount:     %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_RetryCount]))
	log.Printf("GetStoredBlock_FailCount:      %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount]))
	log.Printf("GetStoredBlock_Chunk_Count:    %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count]))
	log.Printf("GetStoredBlock_Byte_Count:     %s\n", byteCountBinary(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count]))
	log.Printf("PutStoredBlock_Count:          %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count]))
	log.Printf("PutStoredBlock_RetryCount:     %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount]))
	log.Printf("PutStoredBlock_FailCount:      %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount]))
	log.Printf("PutStoredBlock_Chunk_Count:    %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count]))
	log.Printf("PutStoredBlock_Byte_Count:     %s\n", byteCountBinary(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count]))
	log.Printf("GetExistingContent_Count:      %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_Count]))
	log.Printf("GetExistingContent_RetryCount: %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_RetryCount]))
	log.Printf("GetExistingContent_FailCount:  %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_FailCount]))
	log.Printf("PreflightGet_Count:            %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_Count]))
	log.Printf("PreflightGet_RetryCount:       %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_RetryCount]))
	log.Printf("PreflightGet_FailCount:        %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount]))
	log.Printf("Flush_Count:                   %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_Flush_Count]))
	log.Printf("Flush_FailCount:               %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_Flush_FailCount]))
	log.Printf("GetStats_Count:                %s\n", byteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStats_Count]))
	log.Printf("------------------\n")
}

func getExistingStoreIndexSync(indexStore longtaillib.Longtail_BlockStoreAPI, chunkHashes []uint64, minBlockUsagePercent uint32) (longtaillib.Longtail_StoreIndex, int) {
	getExistingContentComplete := &getExistingContentCompletionAPI{}
	getExistingContentComplete.wg.Add(1)
	errno := indexStore.GetExistingContent(chunkHashes, minBlockUsagePercent, longtaillib.CreateAsyncGetExistingContentAPI(getExistingContentComplete))
	if errno != 0 {
		getExistingContentComplete.wg.Done()
		getExistingContentComplete.wg.Wait()
		return longtaillib.Longtail_StoreIndex{}, errno
	}
	getExistingContentComplete.wg.Wait()
	return getExistingContentComplete.storeIndex, getExistingContentComplete.err
}

func createBlockStoreForURI(uri string, optionalStoreIndexPath string, jobAPI longtaillib.Longtail_JobAPI, targetBlockSize uint32, maxChunksPerBlock uint32, accessType longtailstorelib.AccessType) (longtaillib.Longtail_BlockStoreAPI, error) {
	blobStoreURL, err := url.Parse(uri)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			gcsBlobStore, err := longtailstorelib.NewGCSBlobStore(blobStoreURL, false)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, err
			}
			gcsBlockStore, err := longtailstorelib.NewRemoteBlockStore(
				jobAPI,
				gcsBlobStore,
				optionalStoreIndexPath,
				numWorkerCount,
				accessType)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, err
			}
			return longtaillib.CreateBlockStoreAPI(gcsBlockStore), nil
		case "s3":
			s3BlobStore, err := longtailstorelib.NewS3BlobStore(blobStoreURL)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, err
			}
			s3BlockStore, err := longtailstorelib.NewRemoteBlockStore(
				jobAPI,
				s3BlobStore,
				optionalStoreIndexPath,
				numWorkerCount,
				accessType)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, err
			}
			return longtaillib.CreateBlockStoreAPI(s3BlockStore), nil
		case "abfs":
			return longtaillib.Longtail_BlockStoreAPI{}, fmt.Errorf("azure Gen1 storage not yet implemented")
		case "abfss":
			return longtaillib.Longtail_BlockStoreAPI{}, fmt.Errorf("azure Gen2 storage not yet implemented")
		case "file":
			return longtaillib.CreateFSBlockStore(jobAPI, longtaillib.CreateFSStorageAPI(), blobStoreURL.Path[1:], targetBlockSize, maxChunksPerBlock), nil
		}
	}
	return longtaillib.CreateFSBlockStore(jobAPI, longtaillib.CreateFSStorageAPI(), uri, targetBlockSize, maxChunksPerBlock), nil
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
		return longtaillib.GetZStdDefaultCompressionType(), nil
	case "zstd_min":
		return longtaillib.GetZStdMinCompressionType(), nil
	case "zstd_max":
		return longtaillib.GetZStdMaxCompressionType(), nil
	}
	return 0, fmt.Errorf("unsupported compression algorithm: `%s`", *compressionAlgorithm)
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
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

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

type asyncFolderScanner struct {
	wg        sync.WaitGroup
	fileInfos longtaillib.Longtail_FileInfos
	elapsed   time.Duration
	err       error
}

func (scanner *asyncFolderScanner) scan(
	sourceFolderPath string,
	pathFilter longtaillib.Longtail_PathFilterAPI,
	fs longtaillib.Longtail_StorageAPI) {

	scanner.wg.Add(1)
	go func() {
		startTime := time.Now()
		fileInfos, errno := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			normalizePath(sourceFolderPath))
		if errno != 0 {
			scanner.err = errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtaillib.GetFilesRecursively(%s) failed", sourceFolderPath)
		}
		scanner.fileInfos = fileInfos
		scanner.elapsed = time.Since(startTime)
		scanner.wg.Done()
	}()
}

func (scanner *asyncFolderScanner) get() (longtaillib.Longtail_FileInfos, time.Duration, error) {
	scanner.wg.Wait()
	return scanner.fileInfos, scanner.elapsed, scanner.err
}

func getFolderIndex(
	sourceFolderPath string,
	sourceIndexPath *string,
	targetChunkSize uint32,
	compressionType uint32,
	hashIdentifier uint32,
	pathFilter longtaillib.Longtail_PathFilterAPI,
	fs longtaillib.Longtail_StorageAPI,
	jobs longtaillib.Longtail_JobAPI,
	hashRegistry longtaillib.Longtail_HashRegistryAPI,
	scanner *asyncFolderScanner) (longtaillib.Longtail_VersionIndex, longtaillib.Longtail_HashAPI, time.Duration, error) {
	if sourceIndexPath == nil || len(*sourceIndexPath) == 0 {
		fileInfos, scanTime, err := scanner.get()
		if err != nil {
			return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, scanTime, err
		}
		defer fileInfos.Dispose()

		startTime := time.Now()

		compressionTypes := getCompressionTypesForFiles(fileInfos, compressionType)

		hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
		if errno != 0 {
			return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, scanTime + time.Since(startTime), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "hashRegistry.GetHashAPI(%d) failed", hashIdentifier)
		}

		chunker := longtaillib.CreateHPCDCChunkerAPI()
		defer chunker.Dispose()

		createVersionIndexProgress := CreateProgress("Indexing version")
		defer createVersionIndexProgress.Dispose()
		vindex, errno := longtaillib.CreateVersionIndex(
			fs,
			hash,
			chunker,
			jobs,
			&createVersionIndexProgress,
			normalizePath(sourceFolderPath),
			fileInfos,
			compressionTypes,
			targetChunkSize)
		if errno != 0 {
			return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, scanTime + time.Since(startTime), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtaillib.CreateVersionIndex(%s)", sourceFolderPath)
		}

		return vindex, hash, scanTime + time.Since(startTime), nil
	}
	startTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(*sourceIndexPath)
	if err != nil {
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, time.Since(startTime), err
	}
	var errno int
	vindex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, time.Since(startTime), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtaillib.ReadVersionIndexFromBuffer(%s) failed", *sourceIndexPath)
	}

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, time.Since(startTime), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "hashRegistry.GetHashAPI(%d) failed", hashIdentifier)
	}

	return vindex, hash, time.Since(startTime), nil
}

type asyncVersionIndexReader struct {
	wg           sync.WaitGroup
	versionIndex longtaillib.Longtail_VersionIndex
	hashAPI      longtaillib.Longtail_HashAPI
	elapsedTime  time.Duration
	err          error
}

func (indexReader *asyncVersionIndexReader) read(
	sourceFolderPath string,
	sourceIndexPath *string,
	targetChunkSize uint32,
	compressionType uint32,
	hashIdentifier uint32,
	pathFilter longtaillib.Longtail_PathFilterAPI,
	fs longtaillib.Longtail_StorageAPI,
	jobs longtaillib.Longtail_JobAPI,
	hashRegistry longtaillib.Longtail_HashRegistryAPI,
	scanner *asyncFolderScanner) {
	indexReader.wg.Add(1)
	go func() {
		indexReader.versionIndex, indexReader.hashAPI, indexReader.elapsedTime, indexReader.err = getFolderIndex(
			sourceFolderPath,
			sourceIndexPath,
			targetChunkSize,
			compressionType,
			hashIdentifier,
			pathFilter,
			fs,
			jobs,
			hashRegistry,
			scanner)
		indexReader.wg.Done()
	}()
}

func (indexReader *asyncVersionIndexReader) get() (longtaillib.Longtail_VersionIndex, longtaillib.Longtail_HashAPI, time.Duration, error) {
	indexReader.wg.Wait()
	return indexReader.versionIndex, indexReader.hashAPI, indexReader.elapsedTime, indexReader.err
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
	minBlockUsagePercent uint32,
	versionLocalStoreIndexPath *string,
	getConfigPath *string) ([]storeStat, []timeStat, error) {

	storeStats := []storeStat{}
	timeStats := []timeStat{}

	setupStartTime := time.Now()
	var pathFilter longtaillib.Longtail_PathFilterAPI

	if includeFilterRegEx != nil || excludeFilterRegEx != nil {
		regexPathFilter := &regexPathFilter{}
		if includeFilterRegEx != nil {
			compiledIncludeRegexes, err := splitRegexes(*includeFilterRegEx)
			if err != nil {
				return storeStats, timeStats, err
			}
			regexPathFilter.compiledIncludeRegexes = compiledIncludeRegexes
		}
		if excludeFilterRegEx != nil {
			compiledExcludeRegexes, err := splitRegexes(*excludeFilterRegEx)
			if err != nil {
				return storeStats, timeStats, err
			}
			regexPathFilter.compiledExcludeRegexes = compiledExcludeRegexes
		}
		if len(regexPathFilter.compiledIncludeRegexes) > 0 || len(regexPathFilter.compiledExcludeRegexes) > 0 {
			pathFilter = longtaillib.CreatePathFilterAPI(regexPathFilter)
		}
	}

	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()

	sourceFolderScanner := asyncFolderScanner{}
	if sourceIndexPath == nil || len(*sourceIndexPath) == 0 {
		sourceFolderScanner.scan(sourceFolderPath, pathFilter, fs)
	}

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	compressionType, err := getCompressionType(compressionAlgorithm)
	if err != nil {
		return storeStats, timeStats, err
	}
	hashIdentifier, err := getHashIdentifier(hashAlgorithm)
	if err != nil {
		return storeStats, timeStats, err
	}

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, timeStat{"Setup", setupTime})

	sourceIndexReader := asyncVersionIndexReader{}
	sourceIndexReader.read(sourceFolderPath,
		sourceIndexPath,
		targetChunkSize,
		compressionType,
		hashIdentifier,
		pathFilter,
		fs,
		jobs,
		hashRegistry,
		&sourceFolderScanner)

	remoteStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, targetBlockSize, maxChunksPerBlock, longtailstorelib.ReadWrite)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer remoteStore.Dispose()

	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	indexStore := longtaillib.CreateCompressBlockStore(remoteStore, creg)
	defer indexStore.Dispose()

	vindex, hash, readSourceIndexTime, err := sourceIndexReader.get()
	if err != nil {
		return storeStats, timeStats, err
	}
	defer vindex.Dispose()
	timeStats = append(timeStats, timeStat{"Read source index", readSourceIndexTime})

	getMissingContentStartTime := time.Now()
	existingRemoteStoreIndex, errno := getExistingStoreIndexSync(indexStore, vindex.GetChunkHashes(), minBlockUsagePercent)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.getExistingStoreIndexSync(%s) failed", blobStoreURI)
	}
	defer existingRemoteStoreIndex.Dispose()

	versionMissingStoreIndex, errno := longtaillib.CreateMissingContent(
		hash,
		existingRemoteStoreIndex,
		vindex,
		targetBlockSize,
		maxChunksPerBlock)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.CreateMissingContent(%s) failed", sourceFolderPath)
	}
	defer versionMissingStoreIndex.Dispose()

	getMissingContentTime := time.Since(getMissingContentStartTime)
	timeStats = append(timeStats, timeStat{"Get content index", getMissingContentTime})

	writeContentStartTime := time.Now()
	if versionMissingStoreIndex.GetBlockCount() > 0 {
		writeContentProgress := CreateProgress("Writing content blocks")
		defer writeContentProgress.Dispose()

		errno = longtaillib.WriteContent(
			fs,
			indexStore,
			jobs,
			&writeContentProgress,
			versionMissingStoreIndex,
			vindex,
			normalizePath(sourceFolderPath))
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.WriteContent(%s) failed", sourceFolderPath)
		}
	}
	writeContentTime := time.Since(writeContentStartTime)
	timeStats = append(timeStats, timeStat{"Write version content", writeContentTime})

	flushStartTime := time.Now()

	indexStoreFlushComplete := &flushCompletionAPI{}
	indexStoreFlushComplete.wg.Add(1)
	errno = indexStore.Flush(longtaillib.CreateAsyncFlushAPI(indexStoreFlushComplete))
	if errno != 0 {
		indexStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: indexStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	remoteStoreFlushComplete := &flushCompletionAPI{}
	remoteStoreFlushComplete.wg.Add(1)
	errno = remoteStore.Flush(longtaillib.CreateAsyncFlushAPI(remoteStoreFlushComplete))
	if errno != 0 {
		remoteStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	indexStoreFlushComplete.wg.Wait()
	if indexStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: indexStore.Flush: Failed for `%s` failed", blobStoreURI)
	}
	remoteStoreFlushComplete.wg.Wait()
	if remoteStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, timeStat{"Flush", flushTime})

	indexStoreStats, errno := indexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Compress", indexStoreStats})
	}
	remoteStoreStats, errno := remoteStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Remote", remoteStoreStats})
	}

	writeVersionIndexStartTime := time.Now()
	vbuffer, errno := longtaillib.WriteVersionIndexToBuffer(vindex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.WriteVersionIndexToBuffer() failed")
	}

	err = longtailstorelib.WriteToURI(targetFilePath, vbuffer)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: longtaillib.longtailstorelib.WriteToURL() failed")
	}
	writeVersionIndexTime := time.Since(writeVersionIndexStartTime)
	timeStats = append(timeStats, timeStat{"Write version index", writeVersionIndexTime})

	if versionLocalStoreIndexPath != nil && len(*versionLocalStoreIndexPath) > 0 {
		writeVersionLocalStoreIndexStartTime := time.Now()
		versionLocalStoreIndex, errno := longtaillib.MergeStoreIndex(existingRemoteStoreIndex, versionMissingStoreIndex)
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "upSyncVersion: longtaillib.MergeStoreIndex() failed")
		}
		defer versionLocalStoreIndex.Dispose()
		versionLocalStoreIndexBuffer, errno := longtaillib.WriteStoreIndexToBuffer(versionLocalStoreIndex)
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "upSyncVersion: longtaillib.WriteStoreIndexToBuffer() failed")
		}
		err = longtailstorelib.WriteToURI(*versionLocalStoreIndexPath, versionLocalStoreIndexBuffer)
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: longtailstorelib.WriteToURL() failed")
		}
		writeVersionLocalStoreIndexTime := time.Since(writeVersionLocalStoreIndexStartTime)
		timeStats = append(timeStats, timeStat{"Write version store index", writeVersionLocalStoreIndexTime})
	}

	if getConfigPath != nil && len(*getConfigPath) > 0 {
		writeGetConfigStartTime := time.Now()

		v := viper.New()
		v.SetConfigType("json")
		v.Set("storage-uri", blobStoreURI)
		v.Set("source-path", targetFilePath)
		if versionLocalStoreIndexPath != nil && len(*versionLocalStoreIndexPath) > 0 {
			v.Set("version-local-store-index-path", *versionLocalStoreIndexPath)
		}
		tmpFile, err := ioutil.TempFile(os.TempDir(), "longtail-")
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: ioutil.TempFile() failed")
		}
		tmpFilePath := tmpFile.Name()
		tmpFile.Close()
		fmt.Printf("tmp file: %s", tmpFilePath)
		err = v.WriteConfigAs(tmpFilePath)
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: v.WriteConfigAs() failed")
		}

		bytes, err := ioutil.ReadFile(tmpFilePath)
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: ioutil.ReadFile(%s) failed", tmpFilePath)
		}
		os.Remove(tmpFilePath)

		err = longtailstorelib.WriteToURI(*getConfigPath, bytes)
		if err != nil {
			return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: longtailstorelib.WriteToURI(%s) failed", *getConfigPath)
		}

		writeGetConfigTime := time.Since(writeGetConfigStartTime)
		timeStats = append(timeStats, timeStat{"Write get config", writeGetConfigTime})
	}

	return storeStats, timeStats, nil
}

func getVersion(
	getConfigPath string,
	targetFolderPath *string,
	targetIndexPath *string,
	localCachePath *string,
	retainPermissions bool,
	validate bool,
	includeFilterRegEx *string,
	excludeFilterRegEx *string) ([]storeStat, []timeStat, error) {

	storeStats := []storeStat{}
	timeStats := []timeStat{}

	readGetConfigStartTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(getConfigPath)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, "getVersion: longtailstorelib.ReadFromURI() failed")
	}

	v := viper.New()
	v.SetConfigType("json")
	err = v.ReadConfig(bytes.NewBuffer(vbuffer))
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, "getVersion: v.ReadConfig() failed")
	}

	blobStoreURI := v.GetString("storage-uri")
	if blobStoreURI == "" {
		return storeStats, timeStats, fmt.Errorf("getVersion: missing storage-uri in get-config")
	}
	sourceFilePath := v.GetString("source-path")
	if sourceFilePath == "" {
		return storeStats, timeStats, fmt.Errorf("getVersion: missing source-path in get-config")
	}
	var versionLocalStoreIndexPath string
	if v.IsSet("version-local-store-index-path") {
		versionLocalStoreIndexPath = v.GetString("version-local-store-index-path")
	}

	readGetConfigTime := time.Since(readGetConfigStartTime)
	timeStats = append(timeStats, timeStat{"Read get config", readGetConfigTime})

	downSyncStoreStats, downSyncTimeStats, err := downSyncVersion(
		blobStoreURI,
		sourceFilePath,
		targetFolderPath,
		targetIndexPath,
		localCachePath,
		retainPermissions,
		validate,
		&versionLocalStoreIndexPath,
		includeFilterRegEx,
		excludeFilterRegEx)

	storeStats = append(storeStats, downSyncStoreStats...)
	timeStats = append(timeStats, downSyncTimeStats...)

	return storeStats, timeStats, err
}

func downSyncVersion(
	blobStoreURI string,
	sourceFilePath string,
	targetFolderPath *string,
	targetIndexPath *string,
	localCachePath *string,
	retainPermissions bool,
	validate bool,
	versionLocalStoreIndexPath *string,
	includeFilterRegEx *string,
	excludeFilterRegEx *string) ([]storeStat, []timeStat, error) {

	storeStats := []storeStat{}
	timeStats := []timeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	var pathFilter longtaillib.Longtail_PathFilterAPI

	if includeFilterRegEx != nil || excludeFilterRegEx != nil {
		regexPathFilter := &regexPathFilter{}
		if includeFilterRegEx != nil {
			compiledIncludeRegexes, err := splitRegexes(*includeFilterRegEx)
			if err != nil {
				return storeStats, timeStats, err
			}
			regexPathFilter.compiledIncludeRegexes = compiledIncludeRegexes
		}
		if excludeFilterRegEx != nil {
			compiledExcludeRegexes, err := splitRegexes(*excludeFilterRegEx)
			if err != nil {
				return storeStats, timeStats, err
			}
			regexPathFilter.compiledExcludeRegexes = compiledExcludeRegexes
		}
		if len(regexPathFilter.compiledIncludeRegexes) > 0 || len(regexPathFilter.compiledExcludeRegexes) > 0 {
			pathFilter = longtaillib.CreatePathFilterAPI(regexPathFilter)
		}
	}

	resolvedTargetFolderPath := ""
	if targetFolderPath == nil || len(*targetFolderPath) == 0 {
		urlSplit := strings.Split(normalizePath(sourceFilePath), "/")
		sourceName := urlSplit[len(urlSplit)-1]
		sourceNameSplit := strings.Split(sourceName, ".")
		resolvedTargetFolderPath = sourceNameSplit[0]
		if resolvedTargetFolderPath == "" {
			return storeStats, timeStats, fmt.Errorf("downSyncVersion: unable to resolve target path using `%s` as base", sourceFilePath)
		}
	} else {
		resolvedTargetFolderPath = *targetFolderPath
	}

	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()

	targetFolderScanner := asyncFolderScanner{}
	if targetIndexPath == nil || len(*targetIndexPath) == 0 {
		targetFolderScanner.scan(resolvedTargetFolderPath, pathFilter, fs)
	}

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	readSourceStartTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(sourceFilePath)
	if err != nil {
		return storeStats, timeStats, err
	}
	sourceVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer sourceVersionIndex.Dispose()

	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, timeStat{"Read source index", readSourceTime})

	hashIdentifier := sourceVersionIndex.GetHashIdentifier()
	targetChunkSize := sourceVersionIndex.GetTargetChunkSize()

	targetIndexReader := asyncVersionIndexReader{}
	targetIndexReader.read(resolvedTargetFolderPath,
		targetIndexPath,
		targetChunkSize,
		noCompressionType,
		hashIdentifier,
		pathFilter,
		fs,
		jobs,
		hashRegistry,
		&targetFolderScanner)

	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	localFS := longtaillib.CreateFSStorageAPI()
	defer localFS.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, *versionLocalStoreIndexPath, jobs, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer remoteIndexStore.Dispose()

	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI
	var compressBlockStore longtaillib.Longtail_BlockStoreAPI

	if localCachePath != nil && len(*localCachePath) > 0 {
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, normalizePath(*localCachePath), 8388608, 1024)

		cacheBlockStore = longtaillib.CreateCacheBlockStore(jobs, localIndexStore, remoteIndexStore)

		compressBlockStore = longtaillib.CreateCompressBlockStore(cacheBlockStore, creg)
	} else {
		compressBlockStore = longtaillib.CreateCompressBlockStore(remoteIndexStore, creg)
	}

	defer cacheBlockStore.Dispose()
	defer localIndexStore.Dispose()
	defer compressBlockStore.Dispose()

	lruBlockStore := longtaillib.CreateLRUBlockStoreAPI(compressBlockStore, 32)
	defer lruBlockStore.Dispose()
	indexStore := longtaillib.CreateShareBlockStore(lruBlockStore)
	defer indexStore.Dispose()

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.GetHashAPI() failed")
	}

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, timeStat{"Setup", setupTime})

	targetVersionIndex, hash, readTargetIndexTime, err := targetIndexReader.get()
	if err != nil {
		return storeStats, timeStats, err
	}
	defer targetVersionIndex.Dispose()
	timeStats = append(timeStats, timeStat{"Read target index", readTargetIndexTime})

	getExistingContentStartTime := time.Now()
	versionDiff, errno := longtaillib.CreateVersionDiff(
		hash,
		targetVersionIndex,
		sourceVersionIndex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.CreateVersionDiff() failed")
	}
	defer versionDiff.Dispose()

	chunkHashes, errno := longtaillib.GetRequiredChunkHashes(
		sourceVersionIndex,
		versionDiff)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.GetRequiredChunkHashes() failed")
	}

	retargettedVersionStoreIndex, errno := getExistingStoreIndexSync(indexStore, chunkHashes, 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: getExistingStoreIndexSync(indexStore, chunkHashes) failed")
	}
	defer retargettedVersionStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, timeStat{"Get content index", getExistingContentTime})

	changeVersionStartTime := time.Now()
	changeVersionProgress := CreateProgress("Updating version")
	defer changeVersionProgress.Dispose()
	errno = longtaillib.ChangeVersion(
		indexStore,
		fs,
		hash,
		jobs,
		&changeVersionProgress,
		retargettedVersionStoreIndex,
		targetVersionIndex,
		sourceVersionIndex,
		versionDiff,
		normalizePath(resolvedTargetFolderPath),
		retainPermissions)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ChangeVersion() failed")
	}

	changeVersionTime := time.Since(changeVersionStartTime)
	timeStats = append(timeStats, timeStat{"Change version", changeVersionTime})

	flushStartTime := time.Now()

	indexStoreFlushComplete := &flushCompletionAPI{}
	indexStoreFlushComplete.wg.Add(1)
	errno = indexStore.Flush(longtaillib.CreateAsyncFlushAPI(indexStoreFlushComplete))
	if errno != 0 {
		indexStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: indexStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	lruStoreFlushComplete := &flushCompletionAPI{}
	lruStoreFlushComplete.wg.Add(1)
	errno = lruBlockStore.Flush(longtaillib.CreateAsyncFlushAPI(lruStoreFlushComplete))
	if errno != 0 {
		lruStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: lruBlockStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	compressStoreFlushComplete := &flushCompletionAPI{}
	compressStoreFlushComplete.wg.Add(1)
	errno = compressBlockStore.Flush(longtaillib.CreateAsyncFlushAPI(compressStoreFlushComplete))
	if errno != 0 {
		compressStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: compressStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	cacheStoreFlushComplete := &flushCompletionAPI{}
	cacheStoreFlushComplete.wg.Add(1)
	errno = cacheBlockStore.Flush(longtaillib.CreateAsyncFlushAPI(cacheStoreFlushComplete))
	if errno != 0 {
		cacheStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: cacheStore.Flush: Failed for `%s` failed", *localCachePath)
	}

	localStoreFlushComplete := &flushCompletionAPI{}
	localStoreFlushComplete.wg.Add(1)
	errno = localIndexStore.Flush(longtaillib.CreateAsyncFlushAPI(localStoreFlushComplete))
	if errno != 0 {
		localStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: localStore.Flush: Failed for `%s` failed", *localCachePath)
	}

	remoteStoreFlushComplete := &flushCompletionAPI{}
	remoteStoreFlushComplete.wg.Add(1)
	errno = remoteIndexStore.Flush(longtaillib.CreateAsyncFlushAPI(remoteStoreFlushComplete))
	if errno != 0 {
		remoteStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	indexStoreFlushComplete.wg.Wait()
	if indexStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: indexStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	lruStoreFlushComplete.wg.Wait()
	if lruStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: lruStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	compressStoreFlushComplete.wg.Wait()
	if compressStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: compressStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	cacheStoreFlushComplete.wg.Wait()
	if cacheStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: cacheStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	localStoreFlushComplete.wg.Wait()
	if localStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: localStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	remoteStoreFlushComplete.wg.Wait()
	if remoteStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, timeStat{"Flush", flushTime})

	shareStoreStats, errno := indexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Share", shareStoreStats})
	}
	lruStoreStats, errno := lruBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"LRU", lruStoreStats})
	}
	compressStoreStats, errno := compressBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Compress", compressStoreStats})
	}
	cacheStoreStats, errno := cacheBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Cache", cacheStoreStats})
	}
	localStoreStats, errno := localIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Local", localStoreStats})
	}
	remoteStoreStats, errno := remoteIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Remote", remoteStoreStats})
	}

	if validate {
		validateStartTime := time.Now()
		validateFileInfos, errno := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			normalizePath(resolvedTargetFolderPath))
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.GetFilesRecursively() failed")
		}
		defer validateFileInfos.Dispose()

		chunker := longtaillib.CreateHPCDCChunkerAPI()
		defer chunker.Dispose()

		createVersionIndexProgress := CreateProgress("Validating version")
		defer createVersionIndexProgress.Dispose()
		validateVersionIndex, errno := longtaillib.CreateVersionIndex(
			fs,
			hash,
			chunker,
			jobs,
			&createVersionIndexProgress,
			normalizePath(resolvedTargetFolderPath),
			validateFileInfos,
			nil,
			targetChunkSize)
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.CreateVersionIndex() failed")
		}
		defer validateVersionIndex.Dispose()
		if validateVersionIndex.GetAssetCount() != sourceVersionIndex.GetAssetCount() {
			return storeStats, timeStats, fmt.Errorf("downSyncVersion: failed validation: asset count mismatch")
		}
		validateAssetSizes := validateVersionIndex.GetAssetSizes()
		validateAssetHashes := validateVersionIndex.GetAssetHashes()

		sourceAssetSizes := sourceVersionIndex.GetAssetSizes()
		sourceAssetHashes := sourceVersionIndex.GetAssetHashes()

		assetSizeLookup := map[string]uint64{}
		assetHashLookup := map[string]uint64{}
		assetPermissionLookup := map[string]uint16{}

		for i, s := range sourceAssetSizes {
			path := sourceVersionIndex.GetAssetPath(uint32(i))
			assetSizeLookup[path] = s
			assetHashLookup[path] = sourceAssetHashes[i]
			assetPermissionLookup[path] = sourceVersionIndex.GetAssetPermissions(uint32(i))
		}
		for i, validateSize := range validateAssetSizes {
			validatePath := validateVersionIndex.GetAssetPath(uint32(i))
			validateHash := validateAssetHashes[i]
			size, exists := assetSizeLookup[validatePath]
			hash := assetHashLookup[validatePath]
			if !exists {
				return storeStats, timeStats, fmt.Errorf("downSyncVersion: failed validation: invalid path %s", validatePath)
			}
			if size != validateSize {
				return storeStats, timeStats, fmt.Errorf("downSyncVersion: failed validation: asset %d size mismatch", i)
			}
			if hash != validateHash {
				return storeStats, timeStats, fmt.Errorf("downSyncVersion: failed validation: asset %d hash mismatch", i)
			}
			if retainPermissions {
				validatePermissions := validateVersionIndex.GetAssetPermissions(uint32(i))
				permissions := assetPermissionLookup[validatePath]
				if permissions != validatePermissions {
					return storeStats, timeStats, fmt.Errorf("downSyncVersion: failed validation: asset %d permission mismatch", i)
				}
			}
		}
		validateTime := time.Since(validateStartTime)
		timeStats = append(timeStats, timeStat{"Validate", validateTime})
	}

	return storeStats, timeStats, nil
}

func hashIdentifierToString(hashIdentifier uint32) string {
	if hashIdentifier == longtaillib.GetBlake2HashIdentifier() {
		return "blake2"
	}
	if hashIdentifier == longtaillib.GetBlake3HashIdentifier() {
		return "blake3"
	}
	if hashIdentifier == longtaillib.GetMeowHashIdentifier() {
		return "meow"
	}
	return fmt.Sprintf("%d", hashIdentifier)
}

func validateVersion(
	blobStoreURI string,
	versionIndexPath string,
	targetBlockSize uint32,
	maxChunksPerBlock uint32) ([]storeStat, []timeStat, error) {

	storeStats := []storeStat{}
	timeStats := []timeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	indexStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer indexStore.Dispose()
	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, timeStat{"Setup", setupTime})

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, timeStat{"Read source index", readSourceTime})

	getExistingContentStartTime := time.Now()
	remoteStoreIndex, errno := getExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(), 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: getExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(): Failed for `%s` failed", blobStoreURI)
	}
	defer remoteStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, timeStat{"Get content index", getExistingContentTime})

	validateStartTime := time.Now()
	errno = longtaillib.ValidateStore(remoteStoreIndex, versionIndex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: longtaillib.ValidateContent() failed")
	}
	validateTime := time.Since(validateStartTime)
	timeStats = append(timeStats, timeStat{"Validate", validateTime})

	return storeStats, timeStats, nil
}

func showVersionIndex(versionIndexPath string, compact bool) ([]storeStat, []timeStat, error) {
	storeStats := []storeStat{}
	timeStats := []timeStat{}

	readSourceStartTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, timeStat{"Read source index", readSourceTime})

	var smallestChunkSize uint32
	var largestChunkSize uint32
	var averageChunkSize uint32
	var totalAssetSize uint64
	var totalChunkSize uint64
	totalAssetSize = 0
	totalChunkSize = 0
	chunkSizes := versionIndex.GetChunkSizes()
	if len(chunkSizes) > 0 {
		smallestChunkSize = uint32(chunkSizes[0])
		largestChunkSize = uint32(chunkSizes[0])
	} else {
		smallestChunkSize = 0
		largestChunkSize = 0
	}
	for i := uint32(0); i < uint32(len(chunkSizes)); i++ {
		chunkSize := uint32(chunkSizes[i])
		if chunkSize < smallestChunkSize {
			smallestChunkSize = chunkSize
		}
		if chunkSize > largestChunkSize {
			largestChunkSize = chunkSize
		}
		totalChunkSize = totalChunkSize + uint64(chunkSize)
	}
	if len(chunkSizes) > 0 {
		averageChunkSize = uint32(totalChunkSize / uint64(len(chunkSizes)))
	} else {
		averageChunkSize = 0
	}
	assetSizes := versionIndex.GetAssetSizes()
	for i := uint32(0); i < uint32(len(assetSizes)); i++ {
		assetSize := uint64(assetSizes[i])
		totalAssetSize = totalAssetSize + uint64(assetSize)
	}

	if compact {
		fmt.Printf("%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			versionIndexPath,
			versionIndex.GetVersion(),
			hashIdentifierToString(versionIndex.GetHashIdentifier()),
			versionIndex.GetTargetChunkSize(),
			versionIndex.GetAssetCount(),
			totalAssetSize,
			versionIndex.GetChunkCount(),
			totalChunkSize,
			averageChunkSize,
			smallestChunkSize,
			largestChunkSize)
	} else {
		fmt.Printf("Version:             %d\n", versionIndex.GetVersion())
		fmt.Printf("Hash Identifier:     %s\n", hashIdentifierToString(versionIndex.GetHashIdentifier()))
		fmt.Printf("Target Chunk Size:   %d\n", versionIndex.GetTargetChunkSize())
		fmt.Printf("Asset Count:         %d   (%s)\n", versionIndex.GetAssetCount(), byteCountDecimal(uint64(versionIndex.GetAssetCount())))
		fmt.Printf("Asset Total Size:    %d   (%s)\n", totalAssetSize, byteCountBinary(totalAssetSize))
		fmt.Printf("Chunk Count:         %d   (%s)\n", versionIndex.GetChunkCount(), byteCountDecimal(uint64(versionIndex.GetChunkCount())))
		fmt.Printf("Chunk Total Size:    %d   (%s)\n", totalChunkSize, byteCountBinary(totalChunkSize))
		fmt.Printf("Average Chunk Size:  %d   (%s)\n", averageChunkSize, byteCountBinary(uint64(averageChunkSize)))
		fmt.Printf("Smallest Chunk Size: %d   (%s)\n", smallestChunkSize, byteCountBinary(uint64(smallestChunkSize)))
		fmt.Printf("Largest Chunk Size:  %d   (%s)\n", largestChunkSize, byteCountBinary(uint64(largestChunkSize)))
	}

	return storeStats, timeStats, nil
}

func showStoreIndex(storeIndexPath string, compact bool) ([]storeStat, []timeStat, error) {
	storeStats := []storeStat{}
	timeStats := []timeStat{}

	readStoreIndexStartTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(storeIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	storeIndex, errno := longtaillib.ReadStoreIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "showStoreIndex: longtaillib.ReadStoreIndexFromBuffer() failed")
	}
	defer storeIndex.Dispose()
	readStoreIndexTime := time.Since(readStoreIndexStartTime)
	timeStats = append(timeStats, timeStat{"Read store index", readStoreIndexTime})

	if compact {
		fmt.Printf("%s\t%d\t%s\t%d\t%d\n",
			storeIndexPath,
			storeIndex.GetVersion(),
			hashIdentifierToString(storeIndex.GetHashIdentifier()),
			storeIndex.GetBlockCount(),
			storeIndex.GetChunkCount())
	} else {
		fmt.Printf("Version:             %d\n", storeIndex.GetVersion())
		fmt.Printf("Hash Identifier:     %s\n", hashIdentifierToString(storeIndex.GetHashIdentifier()))
		fmt.Printf("Block Count:         %d   (%s)\n", storeIndex.GetBlockCount(), byteCountDecimal(uint64(storeIndex.GetBlockCount())))
		fmt.Printf("Chunk Count:         %d   (%s)\n", storeIndex.GetChunkCount(), byteCountDecimal(uint64(storeIndex.GetChunkCount())))
	}

	return storeStats, timeStats, nil
}

func getDetailsString(path string, size uint64, permissions uint16, isDir bool, sizePadding int) string {
	sizeString := fmt.Sprintf("%d", size)
	sizeString = strings.Repeat(" ", sizePadding-len(sizeString)) + sizeString
	bits := ""
	if isDir {
		bits += "d"
		path = strings.TrimRight(path, "/")
	} else {
		bits += "-"
	}
	if (permissions & 0400) == 0 {
		bits += "-"
	} else {
		bits += "r"
	}
	if (permissions & 0200) == 0 {
		bits += "-"
	} else {
		bits += "w"
	}
	if (permissions & 0100) == 0 {
		bits += "-"
	} else {
		bits += "x"
	}

	if (permissions & 0040) == 0 {
		bits += "-"
	} else {
		bits += "r"
	}
	if (permissions & 0020) == 0 {
		bits += "-"
	} else {
		bits += "w"
	}
	if (permissions & 0010) == 0 {
		bits += "-"
	} else {
		bits += "x"
	}

	if (permissions & 0004) == 0 {
		bits += "-"
	} else {
		bits += "r"
	}
	if (permissions & 0002) == 0 {
		bits += "-"
	} else {
		bits += "w"
	}
	if (permissions & 0001) == 0 {
		bits += "-"
	} else {
		bits += "x"
	}

	return fmt.Sprintf("%s %s %s", bits, sizeString, path)
}

func dumpVersionIndex(versionIndexPath string, showDetails bool) ([]storeStat, []timeStat, error) {
	storeStats := []storeStat{}
	timeStats := []timeStat{}

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, timeStat{"Read source index", readSourceTime})

	assetCount := versionIndex.GetAssetCount()

	var biggestAsset uint64
	biggestAsset = 0
	for i := uint32(0); i < assetCount; i++ {
		assetSize := versionIndex.GetAssetSize(i)
		if assetSize > biggestAsset {
			biggestAsset = assetSize
		}
	}

	sizePadding := len(fmt.Sprintf("%d", biggestAsset))

	for i := uint32(0); i < assetCount; i++ {
		path := versionIndex.GetAssetPath(i)
		if showDetails {
			isDir := strings.HasSuffix(path, "/")
			assetSize := versionIndex.GetAssetSize(i)
			permissions := versionIndex.GetAssetPermissions(i)
			detailsString := getDetailsString(path, assetSize, permissions, isDir, sizePadding)
			fmt.Printf("%s\n", detailsString)
		} else {
			fmt.Printf("%s\n", path)
		}
	}

	return storeStats, timeStats, nil
}

func cpVersionIndex(
	blobStoreURI string,
	versionIndexPath string,
	localCachePath *string,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	sourcePath string,
	targetPath string) ([]storeStat, []timeStat, error) {

	storeStats := []storeStat{}
	timeStats := []timeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()
	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer remoteIndexStore.Dispose()

	localFS := longtaillib.CreateFSStorageAPI()
	defer localFS.Dispose()

	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI
	var compressBlockStore longtaillib.Longtail_BlockStoreAPI

	if localCachePath != nil && len(*localCachePath) > 0 {
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, normalizePath(*localCachePath), 8388608, 1024)

		cacheBlockStore = longtaillib.CreateCacheBlockStore(jobs, localIndexStore, remoteIndexStore)

		compressBlockStore = longtaillib.CreateCompressBlockStore(cacheBlockStore, creg)
	} else {
		compressBlockStore = longtaillib.CreateCompressBlockStore(remoteIndexStore, creg)
	}

	defer cacheBlockStore.Dispose()
	defer localIndexStore.Dispose()
	defer compressBlockStore.Dispose()

	lruBlockStore := longtaillib.CreateLRUBlockStoreAPI(compressBlockStore, 32)
	defer lruBlockStore.Dispose()
	indexStore := longtaillib.CreateShareBlockStore(lruBlockStore)
	defer indexStore.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, timeStat{"Setup", setupTime})

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, timeStat{"Read source index", readSourceTime})

	hashIdentifier := versionIndex.GetHashIdentifier()

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: hashRegistry.GetHashAPI() failed")
	}

	getExistingContentStartTime := time.Now()
	storeIndex, errno := getExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(), 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: getExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(): Failed for `%s` failed", blobStoreURI)
	}
	defer storeIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, timeStat{"Get store index", getExistingContentTime})

	createBlockStoreFSStartTime := time.Now()
	blockStoreFS := longtaillib.CreateBlockStoreStorageAPI(
		hash,
		jobs,
		indexStore,
		storeIndex,
		versionIndex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: hashRegistry.CreateBlockStoreStorageAPI() failed")
	}
	defer blockStoreFS.Dispose()
	createBlockStoreFSTime := time.Since(createBlockStoreFSStartTime)
	timeStats = append(timeStats, timeStat{"Create Blockstore FS", createBlockStoreFSTime})

	copyFileStartTime := time.Now()
	// Only support writing to regular file path for now
	outFile, err := os.Create(targetPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer outFile.Close()

	inFile, errno := blockStoreFS.OpenReadFile(sourcePath)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: hashRegistry.OpenReadFile() failed")
	}
	defer blockStoreFS.CloseFile(inFile)

	size, errno := blockStoreFS.GetSize(inFile)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: blockStoreFS.GetSize() failed")
	}

	offset := uint64(0)
	for offset < size {
		left := size - offset
		if left > 128*1024*1024 {
			left = 128 * 1024 * 1024
		}
		data, errno := blockStoreFS.Read(inFile, offset, left)
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: hashRegistry.Read() failed")
		}
		outFile.Write(data)
		offset += left
	}
	copyFileTime := time.Since(copyFileStartTime)
	timeStats = append(timeStats, timeStat{"Copy file", copyFileTime})

	flushStartTime := time.Now()

	indexStoreFlushComplete := &flushCompletionAPI{}
	indexStoreFlushComplete.wg.Add(1)
	errno = indexStore.Flush(longtaillib.CreateAsyncFlushAPI(indexStoreFlushComplete))
	if errno != 0 {
		indexStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: indexStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	lruStoreFlushComplete := &flushCompletionAPI{}
	lruStoreFlushComplete.wg.Add(1)
	errno = lruBlockStore.Flush(longtaillib.CreateAsyncFlushAPI(lruStoreFlushComplete))
	if errno != 0 {
		lruStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: lruStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	compressStoreFlushComplete := &flushCompletionAPI{}
	compressStoreFlushComplete.wg.Add(1)
	errno = compressBlockStore.Flush(longtaillib.CreateAsyncFlushAPI(compressStoreFlushComplete))
	if errno != 0 {
		compressStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: compressStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	cacheStoreFlushComplete := &flushCompletionAPI{}
	cacheStoreFlushComplete.wg.Add(1)
	errno = cacheBlockStore.Flush(longtaillib.CreateAsyncFlushAPI(cacheStoreFlushComplete))
	if errno != 0 {
		cacheStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: cacheStore.Flush: Failed for `%s` failed", *localCachePath)
	}

	localStoreFlushComplete := &flushCompletionAPI{}
	localStoreFlushComplete.wg.Add(1)
	errno = localIndexStore.Flush(longtaillib.CreateAsyncFlushAPI(localStoreFlushComplete))
	if errno != 0 {
		localStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: localStore.Flush: Failed for `%s` failed", *localCachePath)
	}

	remoteStoreFlushComplete := &flushCompletionAPI{}
	remoteStoreFlushComplete.wg.Add(1)
	errno = remoteIndexStore.Flush(longtaillib.CreateAsyncFlushAPI(remoteStoreFlushComplete))
	if errno != 0 {
		remoteStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	indexStoreFlushComplete.wg.Wait()
	if indexStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: indexStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	lruStoreFlushComplete.wg.Wait()
	if lruStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: lruStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	compressStoreFlushComplete.wg.Wait()
	if compressStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: compressStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	cacheStoreFlushComplete.wg.Wait()
	if cacheStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: cacheStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	localStoreFlushComplete.wg.Wait()
	if localStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: localStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	remoteStoreFlushComplete.wg.Wait()
	if remoteStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}
	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, timeStat{"Flush", flushTime})

	shareStoreStats, errno := indexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Share", shareStoreStats})
	}
	lruStoreStats, errno := lruBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"LRU", lruStoreStats})
	}
	compressStoreStats, errno := compressBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Compress", compressStoreStats})
	}
	cacheStoreStats, errno := cacheBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Cache", cacheStoreStats})
	}
	localStoreStats, errno := localIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Local", localStoreStats})
	}
	remoteStoreStats, errno := remoteIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Remote", remoteStoreStats})
	}

	return storeStats, timeStats, nil
}

func initRemoteStore(
	blobStoreURI string,
	hashAlgorithm *string) ([]storeStat, []timeStat, error) {

	storeStats := []storeStat{}
	timeStats := []timeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, 8388608, 1024, longtailstorelib.Init)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer remoteIndexStore.Dispose()
	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, timeStat{"Setup", setupTime})

	getExistingContentStartTime := time.Now()
	retargetStoreIndex, errno := getExistingStoreIndexSync(remoteIndexStore, []uint64{}, 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "initRemoteStore: getExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(): Failed for `%s` failed", blobStoreURI)
	}
	defer retargetStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, timeStat{"Get store index", getExistingContentTime})

	flushStartTime := time.Now()

	remoteStoreFlushComplete := &flushCompletionAPI{}
	remoteStoreFlushComplete.wg.Add(1)
	errno = remoteIndexStore.Flush(longtaillib.CreateAsyncFlushAPI(remoteStoreFlushComplete))
	if errno != 0 {
		remoteStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "initRemoteStore: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	remoteStoreFlushComplete.wg.Wait()
	if remoteStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "initRemoteStore: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}
	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, timeStat{"Flush", flushTime})

	remoteStoreStats, errno := remoteIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Remote", remoteStoreStats})
	}

	return storeStats, timeStats, nil
}

func lsVersionIndex(
	versionIndexPath string,
	commandLSVersionDir *string) ([]storeStat, []timeStat, error) {
	storeStats := []storeStat{}
	timeStats := []timeStat{}

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, timeStat{"Read source index", readSourceTime})

	setupStartTime := time.Now()
	hashIdentifier := versionIndex.GetHashIdentifier()

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: hashRegistry.GetHashAPI() failed")
	}

	fakeBlockStoreFS := longtaillib.CreateInMemStorageAPI()
	defer fakeBlockStoreFS.Dispose()

	fakeBlockStore := longtaillib.CreateFSBlockStore(jobs, fakeBlockStoreFS, "store", 1024*1024*1024, 1024)
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
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: hashRegistry.CreateBlockStoreStorageAPI() failed")
	}
	defer blockStoreFS.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, timeStat{"Setup", setupTime})

	searchDir := ""
	if commandLSVersionDir != nil && *commandLSVersionDir != "." {
		searchDir = *commandLSVersionDir
	}

	iterator, errno := blockStoreFS.StartFind(searchDir)
	if errno == longtaillib.ENOENT {
		return storeStats, timeStats, nil
	}
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: hashRegistry.StartFind() failed")
	}
	defer blockStoreFS.CloseFind(iterator)
	for {
		properties, errno := blockStoreFS.GetEntryProperties(iterator)
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: GetEntryProperties.GetEntryProperties() failed")
		}
		detailsString := getDetailsString(properties.Name, properties.Size, properties.Permissions, properties.IsDir, 16)
		fmt.Printf("%s\n", detailsString)

		errno = blockStoreFS.FindNext(iterator)
		if errno == longtaillib.ENOENT {
			break
		}
		if errno != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: GetEntryProperties.FindNext() failed")
		}
	}
	return storeStats, timeStats, nil
}

func stats(
	blobStoreURI string,
	versionIndexPath string,
	localCachePath *string) ([]storeStat, []timeStat, error) {

	storeStats := []storeStat{}
	timeStats := []timeStat{}

	setupStartTime := time.Now()
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	var indexStore longtaillib.Longtail_BlockStoreAPI

	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer remoteIndexStore.Dispose()

	var localFS longtaillib.Longtail_StorageAPI

	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI

	if localCachePath != nil && len(*localCachePath) > 0 {
		localFS = longtaillib.CreateFSStorageAPI()
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, normalizePath(*localCachePath), 8388608, 1024)

		cacheBlockStore = longtaillib.CreateCacheBlockStore(jobs, localIndexStore, remoteIndexStore)

		indexStore = cacheBlockStore
	} else {
		indexStore = remoteIndexStore
	}

	defer cacheBlockStore.Dispose()
	defer localIndexStore.Dispose()
	defer localFS.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, timeStat{"Setup", setupTime})

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(versionIndexPath)
	if err != nil {
		return storeStats, timeStats, err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "stats: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, timeStat{"Read source index", readSourceTime})

	getExistingContentStartTime := time.Now()
	existingStoreIndex, errno := getExistingStoreIndexSync(indexStore, versionIndex.GetChunkHashes(), 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "stats: getExistingStoreIndexSync() failed")
	}
	defer existingStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, timeStat{"Get store index", getExistingContentTime})

	blockLookup := make(map[uint64]uint64)

	blockChunkCount := uint32(0)

	fetchingBlocksStartTime := time.Now()

	progress := CreateProgress("Fetching blocks")
	defer progress.Dispose()

	blockHashes := existingStoreIndex.GetBlockHashes()
	maxBatchSize := int(numWorkerCount)
	for i := 0; i < len(blockHashes); {
		batchSize := len(blockHashes) - i
		if batchSize > maxBatchSize {
			batchSize = maxBatchSize
		}
		completions := make([]getStoredBlockCompletionAPI, batchSize)
		for offset := 0; offset < batchSize; offset++ {
			completions[offset].wg.Add(1)
			go func(startIndex int, offset int) {
				blockHash := blockHashes[startIndex+offset]
				indexStore.GetStoredBlock(blockHash, longtaillib.CreateAsyncGetStoredBlockAPI(&completions[offset]))
			}(i, offset)
		}

		for offset := 0; offset < batchSize; offset++ {
			completions[offset].wg.Wait()
			if completions[offset].err != 0 {
				return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "stats: remoteStoreIndex.GetStoredBlock() failed")
			}
			blockIndex := completions[offset].storedBlock.GetBlockIndex()
			for _, chunkHash := range blockIndex.GetChunkHashes() {
				blockLookup[chunkHash] = blockHashes[i+offset]
			}
			blockChunkCount += uint32(len(blockIndex.GetChunkHashes()))
		}

		i += batchSize
		progress.OnProgress(uint32(len(blockHashes)), uint32(i))
	}

	fetchingBlocksTime := time.Since(fetchingBlocksStartTime)
	timeStats = append(timeStats, timeStat{"Fetching blocks", fetchingBlocksTime})

	blockUsage := uint32(100)
	if blockChunkCount > 0 {
		blockUsage = uint32((100 * existingStoreIndex.GetChunkCount()) / blockChunkCount)
	}

	var assetFragmentCount uint64
	chunkHashes := versionIndex.GetChunkHashes()
	assetChunkCounts := versionIndex.GetAssetChunkCounts()
	assetChunkIndexStarts := versionIndex.GetAssetChunkIndexStarts()
	assetChunkIndexes := versionIndex.GetAssetChunkIndexes()
	for a := uint32(0); a < versionIndex.GetAssetCount(); a++ {
		uniqueBlockCount := uint64(0)
		chunkCount := assetChunkCounts[a]
		chunkIndexOffset := assetChunkIndexStarts[a]
		lastBlockIndex := ^uint64(0)
		for c := chunkIndexOffset; c < chunkIndexOffset+chunkCount; c++ {
			chunkIndex := assetChunkIndexes[c]
			chunkHash := chunkHashes[chunkIndex]
			blockIndex := blockLookup[chunkHash]
			if blockIndex != lastBlockIndex {
				uniqueBlockCount++
				lastBlockIndex = blockIndex
				assetFragmentCount++
			}
		}
	}
	assetFragmentation := uint32(0)
	if versionIndex.GetAssetCount() > 0 {
		assetFragmentation = uint32((100*(assetFragmentCount))/uint64(versionIndex.GetAssetCount()) - 100)
	}

	fmt.Printf("Block Usage:          %d%%\n", blockUsage)
	fmt.Printf("Asset Fragmentation:  %d%%\n", assetFragmentation)

	flushStartTime := time.Now()

	cacheStoreFlushComplete := &flushCompletionAPI{}
	cacheStoreFlushComplete.wg.Add(1)
	errno = cacheBlockStore.Flush(longtaillib.CreateAsyncFlushAPI(cacheStoreFlushComplete))
	if errno != 0 {
		cacheStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "stats: cacheStore.Flush: Failed for `%s` failed", *localCachePath)
	}

	localStoreFlushComplete := &flushCompletionAPI{}
	localStoreFlushComplete.wg.Add(1)
	errno = localIndexStore.Flush(longtaillib.CreateAsyncFlushAPI(localStoreFlushComplete))
	if errno != 0 {
		localStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "stats: localStore.Flush: Failed for `%s` failed", *localCachePath)
	}

	remoteStoreFlushComplete := &flushCompletionAPI{}
	remoteStoreFlushComplete.wg.Add(1)
	errno = remoteIndexStore.Flush(longtaillib.CreateAsyncFlushAPI(remoteStoreFlushComplete))
	if errno != 0 {
		remoteStoreFlushComplete.wg.Done()
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "stats: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	cacheStoreFlushComplete.wg.Wait()
	if cacheStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "stats: cacheStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	localStoreFlushComplete.wg.Wait()
	if localStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "stats: localStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	remoteStoreFlushComplete.wg.Wait()
	if remoteStoreFlushComplete.err != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "stats: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	flushTime := time.Since(flushStartTime)
	timeStats = append(timeStats, timeStat{"Flush", flushTime})

	cacheStoreStats, errno := cacheBlockStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Cache", cacheStoreStats})
	}
	localStoreStats, errno := localIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Local", localStoreStats})
	}
	remoteStoreStats, errno := remoteIndexStore.GetStats()
	if errno == 0 {
		storeStats = append(storeStats, storeStat{"Remote", remoteStoreStats})
	}
	return storeStats, timeStats, nil
}

func createVersionStoreIndex(
	blobStoreURI string,
	sourceFilePath string,
	versionLocalStoreIndexPath string) ([]storeStat, []timeStat, error) {
	storeStats := []storeStat{}
	timeStats := []timeStat{}

	setupStartTime := time.Now()

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	indexStore, err := createBlockStoreForURI(blobStoreURI, "", jobs, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer indexStore.Dispose()

	setupTime := time.Since(setupStartTime)
	timeStats = append(timeStats, timeStat{"Setup", setupTime})

	readSourceStartTime := time.Now()
	vbuffer, err := longtailstorelib.ReadFromURI(sourceFilePath)
	if err != nil {
		return storeStats, timeStats, err
	}
	sourceVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer sourceVersionIndex.Dispose()
	readSourceTime := time.Since(readSourceStartTime)
	timeStats = append(timeStats, timeStat{"Read source index", readSourceTime})

	getExistingContentStartTime := time.Now()
	chunkHashes := sourceVersionIndex.GetChunkHashes()

	retargettedVersionStoreIndex, errno := getExistingStoreIndexSync(indexStore, chunkHashes, 0)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: getExistingStoreIndexSync(indexStore, chunkHashes) failed")
	}
	defer retargettedVersionStoreIndex.Dispose()
	getExistingContentTime := time.Since(getExistingContentStartTime)
	timeStats = append(timeStats, timeStat{"Get content index", getExistingContentTime})

	writeVersionLocalStoreIndexStartTime := time.Now()
	versionLocalStoreIndexBuffer, errno := longtaillib.WriteStoreIndexToBuffer(retargettedVersionStoreIndex)
	if errno != 0 {
		return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "upSyncVersion: longtaillib.WriteStoreIndexToBuffer() failed")
	}
	err = longtailstorelib.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer)
	if err != nil {
		return storeStats, timeStats, errors.Wrapf(err, "upSyncVersion: longtaillib.longtailstorelib.WriteToURL() failed")
	}
	writeVersionLocalStoreIndexTime := time.Since(writeVersionLocalStoreIndexStartTime)
	timeStats = append(timeStats, timeStat{"Write version store index", writeVersionLocalStoreIndexTime})

	return storeStats, timeStats, nil
}

func cloneStore(
	sourceStoreURI string,
	targetStoreURI string,
	localCachePath string,
	targetPath string,
	sourcePaths string,
	sourceZipPaths string,
	targetPaths string,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	retainPermissions bool,
	createVersionLocalStoreIndex bool,
	hashing string,
	compression string,
	minBlockUsagePercent uint32) ([]storeStat, []timeStat, error) {

	storeStats := []storeStat{}
	timeStats := []timeStat{}

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()

	fs := longtaillib.CreateFSStorageAPI()
	defer fs.Dispose()

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	localFS := longtaillib.CreateFSStorageAPI()
	defer localFS.Dispose()

	sourceRemoteIndexStore, err := createBlockStoreForURI(sourceStoreURI, "", jobs, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer sourceRemoteIndexStore.Dispose()
	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI
	var sourceCompressBlockStore longtaillib.Longtail_BlockStoreAPI

	if len(localCachePath) > 0 {
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, normalizePath(localCachePath), 8388608, 1024)

		cacheBlockStore = longtaillib.CreateCacheBlockStore(jobs, localIndexStore, sourceRemoteIndexStore)

		sourceCompressBlockStore = longtaillib.CreateCompressBlockStore(cacheBlockStore, creg)
	} else {
		sourceCompressBlockStore = longtaillib.CreateCompressBlockStore(sourceRemoteIndexStore, creg)
	}

	defer localIndexStore.Dispose()
	defer cacheBlockStore.Dispose()
	defer sourceCompressBlockStore.Dispose()

	sourceLRUBlockStore := longtaillib.CreateLRUBlockStoreAPI(sourceCompressBlockStore, 32)
	defer sourceLRUBlockStore.Dispose()
	sourceStore := longtaillib.CreateShareBlockStore(sourceLRUBlockStore)
	defer sourceStore.Dispose()

	targetRemoteStore, err := createBlockStoreForURI(targetStoreURI, "", jobs, targetBlockSize, maxChunksPerBlock, longtailstorelib.ReadWrite)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer targetRemoteStore.Dispose()
	targetStore := longtaillib.CreateCompressBlockStore(targetRemoteStore, creg)
	defer targetStore.Dispose()

	sourcesFile, err := os.Open(sourcePaths)
	if err != nil {
		log.Fatal(err)
	}
	defer sourcesFile.Close()

	sourcesZipFile, err := os.Open(sourceZipPaths)
	if err != nil {
		log.Fatal(err)
	}
	defer sourcesZipFile.Close()

	targetsFile, err := os.Open(targetPaths)
	if err != nil {
		log.Fatal(err)
	}
	defer targetsFile.Close()

	sourcesScanner := bufio.NewScanner(sourcesFile)
	sourcesZipScanner := bufio.NewScanner(sourcesZipFile)
	targetsScanner := bufio.NewScanner(targetsFile)

	var pathFilter longtaillib.Longtail_PathFilterAPI

	for sourcesScanner.Scan() {
		if !targetsScanner.Scan() {
			break
		}
		if !sourcesZipScanner.Scan() {
			break
		}
		targetFolderScanner := asyncFolderScanner{}
		targetFolderScanner.scan(targetPath, pathFilter, fs)

		sourceFilePath := sourcesScanner.Text()
		sourceFileZipPath := sourcesZipScanner.Text()
		targetFilePath := targetsScanner.Text()

		tbuffer, err := longtailstorelib.ReadFromURI(targetFilePath)
		if err == nil {
			fmt.Printf("Validating `%s` as `%s`\n", sourceFilePath, targetFilePath)
			targetVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(tbuffer)
			tbuffer = nil
			if errno == 0 {
				targetStoreIndex, errno := getExistingStoreIndexSync(targetStore, targetVersionIndex.GetChunkHashes(), 0)
				if errno == 0 {
					errno = longtaillib.ValidateStore(targetStoreIndex, targetVersionIndex)
					targetStoreIndex.Dispose()
					targetVersionIndex.Dispose()
					if errno == 0 {
						fmt.Printf("Skipping `%s`, valid version stored as `%s`\n", sourceFilePath, targetFilePath)
						continue
					}
					targetStoreIndex.Dispose()
				}
				targetVersionIndex.Dispose()
			}
			fmt.Printf("Validation failed, rebuilding `%s` as `%s`\n", sourceFilePath, targetFilePath)
		}

		fmt.Printf("`%s` -> `%s`\n", sourceFilePath, targetFilePath)

		vbuffer, err := longtailstorelib.ReadFromURI(sourceFilePath)
		if err != nil {
			fileInfos, _, _ := targetFolderScanner.get()
			fileInfos.Dispose()
			continue
		}
		sourceVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
		if errno != 0 {
			fileInfos, _, _ := targetFolderScanner.get()
			fileInfos.Dispose()
			continue
		}

		hashIdentifier := sourceVersionIndex.GetHashIdentifier()
		targetChunkSize := sourceVersionIndex.GetTargetChunkSize()

		targetIndexReader := asyncVersionIndexReader{}
		targetIndexReader.read(targetPath,
			nil,
			targetChunkSize,
			noCompressionType,
			hashIdentifier,
			pathFilter,
			fs,
			jobs,
			hashRegistry,
			&targetFolderScanner)

		targetVersionIndex, hash, _, err := targetIndexReader.get()
		if err != nil {
			sourceVersionIndex.Dispose()
			continue
		}

		versionDiff, errno := longtaillib.CreateVersionDiff(
			hash,
			targetVersionIndex,
			sourceVersionIndex)
		if errno != 0 {
			targetVersionIndex.Dispose()
			sourceVersionIndex.Dispose()
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.CreateVersionDiff() failed")
		}

		chunkHashes, errno := longtaillib.GetRequiredChunkHashes(
			sourceVersionIndex,
			versionDiff)
		if errno != 0 {
			targetVersionIndex.Dispose()
			sourceVersionIndex.Dispose()
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.GetRequiredChunkHashes() failed")
		}

		existingStoreIndex, errno := getExistingStoreIndexSync(sourceStore, chunkHashes, 0)
		if errno != 0 {
			targetVersionIndex.Dispose()
			sourceVersionIndex.Dispose()
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: getExistingStoreIndexSync() failed")
		}

		changeVersionProgress := CreateProgress("Updating version")
		errno = longtaillib.ChangeVersion(
			sourceStore,
			fs,
			hash,
			jobs,
			&changeVersionProgress,
			existingStoreIndex,
			targetVersionIndex,
			sourceVersionIndex,
			versionDiff,
			normalizePath(targetPath),
			retainPermissions)
		changeVersionProgress.Dispose()
		existingStoreIndex.Dispose()
		targetVersionIndex.Dispose()
		if errno != 0 {
			fmt.Printf("Falling back to reading ZIP source from `%s`\n", sourceFileZipPath)
			sourceVersionIndex.Dispose()
			zipBytes, err := longtailstorelib.ReadFromURI(sourceFileZipPath)
			if err != nil {
				sourceVersionIndex.Dispose()
				continue
			}
			err = ioutil.WriteFile("tmp.zip", zipBytes, 0644)
			if err != nil {
				sourceVersionIndex.Dispose()
				continue
			}

			r, err := zip.OpenReader("tmp.zip")
			if err != nil {
				return storeStats, timeStats, errors.Wrapf(err, "cloneStore: zip.OpenReader() failed")
			}
			os.RemoveAll(targetPath)
			os.MkdirAll(targetPath, 0755)
			// Closure to address file descriptors issue with all the deferred .Close() methods
			extractAndWriteFile := func(f *zip.File) error {
				rc, err := f.Open()
				if err != nil {
					return err
				}
				defer func() {
					if err := rc.Close(); err != nil {
						panic(err)
					}
				}()

				path := filepath.Join(targetPath, f.Name)
				fmt.Printf("Unzipping `%s`\n", path)

				// Check for ZipSlip (Directory traversal)
				if !strings.HasPrefix(path, filepath.Clean(targetPath)+string(os.PathSeparator)) {
					return fmt.Errorf("illegal file path: %s", path)
				}

				if f.FileInfo().IsDir() {
					os.MkdirAll(path, f.Mode())
				} else {
					os.MkdirAll(filepath.Dir(path), 0777)
					f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
					if err != nil {
						return err
					}
					defer func() {
						if err := f.Close(); err != nil {
							panic(err)
						}
					}()

					_, err = io.Copy(f, rc)
					if err != nil {
						return err
					}
				}
				return nil
			}

			for _, f := range r.File {
				err := extractAndWriteFile(f)
				if err != nil {
					r.Close()
					return storeStats, timeStats, err
				}
			}

			r.Close()

			fileInfos, errno := longtaillib.GetFilesRecursively(
				fs,
				pathFilter,
				normalizePath(targetPath))
			if errno != 0 {
				return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.GetFilesRecursively() failed")
			}

			compressionTypes := getCompressionTypesForFiles(fileInfos, noCompressionType)

			hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
			if errno != 0 {
				fileInfos.Dispose()
				return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: hashRegistry.GetHashAPI() failed")
			}

			chunker := longtaillib.CreateHPCDCChunkerAPI()

			createVersionIndexProgress := CreateProgress("Indexing version")
			sourceVersionIndex, errno = longtaillib.CreateVersionIndex(
				fs,
				hash,
				chunker,
				jobs,
				&createVersionIndexProgress,
				normalizePath(targetPath),
				fileInfos,
				compressionTypes,
				targetChunkSize)
			createVersionIndexProgress.Dispose()
			chunker.Dispose()
			fileInfos.Dispose()
			if errno != 0 {
				return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.CreateVersionIndex() failed")
			}

			// Make sure to update binary for new version index
			vbuffer, errno = longtaillib.WriteVersionIndexToBuffer(sourceVersionIndex)
			if errno != 0 {
				sourceVersionIndex.Dispose()
				return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.WriteVersionIndexToBuffer() failed")
			}
		}

		existingStoreIndex, errno = getExistingStoreIndexSync(targetStore, sourceVersionIndex.GetChunkHashes(), minBlockUsagePercent)
		if errno != 0 {
			sourceVersionIndex.Dispose()
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: getExistingStoreIndexSync() failed")
		}

		versionMissingStoreIndex, errno := longtaillib.CreateMissingContent(
			hash,
			existingStoreIndex,
			sourceVersionIndex,
			targetBlockSize,
			maxChunksPerBlock)
		if errno != 0 {
			existingStoreIndex.Dispose()
			sourceVersionIndex.Dispose()
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: CreateMissingContent() failed")
		}

		if versionMissingStoreIndex.GetBlockCount() > 0 {
			writeContentProgress := CreateProgress("Writing content blocks")

			errno = longtaillib.WriteContent(
				fs,
				targetStore,
				jobs,
				&writeContentProgress,
				versionMissingStoreIndex,
				sourceVersionIndex,
				normalizePath(targetPath))
			writeContentProgress.Dispose()
			if errno != 0 {
				versionMissingStoreIndex.Dispose()
				existingStoreIndex.Dispose()
				sourceVersionIndex.Dispose()
				return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.WriteContent() failed")
			}
		}

		targetStoreFlushComplete := &flushCompletionAPI{}
		targetStoreFlushComplete.wg.Add(1)
		errno = targetRemoteStore.Flush(longtaillib.CreateAsyncFlushAPI(targetStoreFlushComplete))
		if errno != 0 {
			versionMissingStoreIndex.Dispose()
			existingStoreIndex.Dispose()
			sourceVersionIndex.Dispose()
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: indexStore.Flush: Failed for `%s` failed", targetStoreURI)
		}

		sourceStoreFlushComplete := &flushCompletionAPI{}
		sourceStoreFlushComplete.wg.Add(1)
		errno = sourceRemoteIndexStore.Flush(longtaillib.CreateAsyncFlushAPI(sourceStoreFlushComplete))
		if errno != 0 {
			versionMissingStoreIndex.Dispose()
			existingStoreIndex.Dispose()
			sourceVersionIndex.Dispose()
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: indexStore.Flush: Failed for `%s` failed", sourceStoreURI)
		}

		err = longtailstorelib.WriteToURI(targetFilePath, vbuffer)
		if err != nil {
			versionMissingStoreIndex.Dispose()
			existingStoreIndex.Dispose()
			sourceVersionIndex.Dispose()
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtailstorelib.WriteToURI() failed")
		}

		if createVersionLocalStoreIndex {
			versionLocalStoreIndex, errno := longtaillib.MergeStoreIndex(existingStoreIndex, versionMissingStoreIndex)
			if errno != 0 {
				versionMissingStoreIndex.Dispose()
				existingStoreIndex.Dispose()
				sourceVersionIndex.Dispose()
				return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.MergeStoreIndex() failed")
			}
			versionLocalStoreIndexBuffer, errno := longtaillib.WriteStoreIndexToBuffer(versionLocalStoreIndex)
			versionLocalStoreIndex.Dispose()
			if errno != 0 {
				versionMissingStoreIndex.Dispose()
				existingStoreIndex.Dispose()
				sourceVersionIndex.Dispose()
				return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.WriteStoreIndexToBuffer() failed")
			}
			versionLocalStoreIndexPath := strings.Replace(targetFilePath, ".lvi", ".lsi", -1) // TODO: This should use a file with path names instead of this rename hack!
			err = longtailstorelib.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer)
			if err != nil {
				versionMissingStoreIndex.Dispose()
				existingStoreIndex.Dispose()
				sourceVersionIndex.Dispose()
				return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtailstorelib.WriteToURI() failed")
			}
		}

		versionMissingStoreIndex.Dispose()
		existingStoreIndex.Dispose()
		sourceVersionIndex.Dispose()

		targetStoreFlushComplete.wg.Wait()
		if targetStoreFlushComplete.err != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: indexStore.Flush: Failed for `%s` failed", targetStoreURI)
		}
		sourceStoreFlushComplete.wg.Wait()
		if sourceStoreFlushComplete.err != 0 {
			return storeStats, timeStats, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: indexStore.Flush: Failed for `%s` failed", sourceStoreURI)
		}
	}

	if err := sourcesScanner.Err(); err != nil {
		log.Fatal(err)
	}
	if err := sourcesZipScanner.Err(); err != nil {
		log.Fatal(err)
	}
	if err := targetsScanner.Err(); err != nil {
		log.Fatal(err)
	}

	return storeStats, timeStats, nil
}

type Context struct {
	StoreStats []storeStat
	TimeStats  []timeStat
}

type CompressionOption struct {
	Compression string `name:"compression-algorithm" help:"Compression algorithm [none brotli brotli_min brotli_max brotli_text brotli_text_min brotli_text_max lz4 zstd zstd_min zstd_max]" enum:"none,brotli,brotli_min,brotli_max,brotli_text,brotli_text_min,brotli_text_max,lz4,zstd,zstd_min,zstd_max" default:"zstd"`
}

type HashingOption struct {
	Hashing string `name:"hash-algorithm" help:"Hash algorithm [meow blake2 blake3]" enum:"meow,blake2,blake3" default:"blake3"`
}

type IncludeRegExOption struct {
	IncludeFilterRegEx string `name:"include-filter-regex" help:"Optional include regex filter for assets in --source-path on upsync and --target-path on downsync. Separate regexes with **"`
}

type ExcludeRegExOption struct {
	ExcludeFilterRegEx string `name:"exclude-filter-regex" help:"Optional exclude regex filter for assets in --source-path on upsync and --target-path on downsync. Separate regexes with **"`
}

type UpsyncCmd struct {
	StorageURI                 string `name:"storage-uri" help"Storage URI (local file system, GCS and S3 bucket URI supported)"`
	TargetChunkSize            uint32 `name:"target-chunk-size" help:"Target chunk size" default:"32768"`
	TargetBlockSize            uint32 `name:"target-block-size" help:"Target block size" default:"8388608"`
	MaxChunksPerBlock          uint32 `name:"max-chunks-per-block" help:"Max chunks per block" default:"1024"`
	SourcePath                 string `name:"source-path" help:"Source folder path" required:""`
	SourceIndexPath            string `name:"source-index-path" help:"Optional pre-computed index of source-path"`
	TargetPath                 string `name:"target-path" help:"Target file uri" required:""`
	MinBlockUsagePercent       uint32 `name:"min-block-usage-percent" help:"Minimum percent of block content than must match for it to be considered \"existing\". Default is zero = use all" default:"0"`
	VersionLocalStoreIndexPath string `name:"version-local-store-index-path" help:"Generate an store index optimized for this particular version"`
	GetConfigPath              string `name:"get-config-path" help:"File uri for json formatted get-config file"`
	CompressionOption
	HashingOption
	IncludeRegExOption
	ExcludeRegExOption
}

func (r *UpsyncCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := upSyncVersion(
		r.StorageURI,
		r.SourcePath,
		&r.SourceIndexPath,
		r.TargetPath,
		r.TargetChunkSize,
		r.TargetBlockSize,
		r.MaxChunksPerBlock,
		&r.Compression,
		&r.Hashing,
		&r.IncludeFilterRegEx,
		&r.ExcludeFilterRegEx,
		r.MinBlockUsagePercent,
		&r.VersionLocalStoreIndexPath,
		&r.GetConfigPath)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}

var cli struct {
	LogLevel       string    `name:"log-level" help:"Log level [debug, info, warn, error]" enum:"debug, info, warn, error" default:"warn" `
	ShowStats      bool      `name:"show-stats" help:"Output brief stats summary"`
	ShowStoreStats bool      `name:"show-store-stats" help:"Output detailed stats for block stores"`
	Upsync         UpsyncCmd `cmd:"upsync" help:"Upload a folder"`
}

var (
	logLevel           = kingpin.Flag("log-level", "Log level").Default("warn").Enum("debug", "info", "warn", "error")
	showStats          = kingpin.Flag("show-stats", "Output brief stats summary").Bool()
	showStoreStats     = kingpin.Flag("show-store-stats", "Output detailed stats for block stores").Bool()
	includeFilterRegEx = kingpin.Flag("include-filter-regex", "Optional include regex filter for assets in --source-path on upsync and --target-path on downsync. Separate regexes with **").String()
	excludeFilterRegEx = kingpin.Flag("exclude-filter-regex", "Optional exclude regex filter for assets in --source-path on upsync and --target-path on downsync. Separate regexes with **").String()
	memTrace           = kingpin.Flag("mem-trace", "Output summary memory statistics from longtail").Bool()
	memTraceDetailed   = kingpin.Flag("mem-trace-detailed", "Output detailed memory statistics from longtail").Bool()
	memTraceCSV        = kingpin.Flag("mem-trace-csv", "Output path for detailed memory statistics from longtail in csv format").String()
	workerCount        = kingpin.Flag("worker-count", "Limit number of workers created, defaults to match number of logical CPUs").Int()

	commandUpsync           = kingpin.Command("upsync", "Upload a folder")
	commandUpsyncStorageURI = commandUpsync.Flag("storage-uri", "Storage URI (local file system, GCS and S3 bucket URI supported)").Required().String()
	commandUpsyncHashing    = commandUpsync.Flag("hash-algorithm", "upsync hash algorithm: blake2, blake3, meow").
				Default("blake3").
				Enum("meow", "blake2", "blake3")
	commandUpsyncTargetChunkSize   = commandUpsync.Flag("target-chunk-size", "Target chunk size").Default("32768").Uint32()
	commandUpsyncTargetBlockSize   = commandUpsync.Flag("target-block-size", "Target block size").Default("8388608").Uint32()
	commandUpsyncMaxChunksPerBlock = commandUpsync.Flag("max-chunks-per-block", "Max chunks per block").Default("1024").Uint32()
	commandUpsyncSourcePath        = commandUpsync.Flag("source-path", "Source folder path").Required().String()
	commandUpsyncSourceIndexPath   = commandUpsync.Flag("source-index-path", "Optional pre-computed index of source-path").String()
	commandUpsyncTargetPath        = commandUpsync.Flag("target-path", "Target file uri").Required().String()
	commandUpsyncCompression       = commandUpsync.Flag("compression-algorithm", "compression algorithm: none, brotli[_min|_max], brotli_text[_min|_max], lz4, ztd[_min|_max]").
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
	commandUpsyncMinBlockUsagePercent       = commandUpsync.Flag("min-block-usage-percent", "Minimum percent of block content than must match for it to be considered \"existing\". Default is zero = use all").Default("0").Uint32()
	commandUpsyncVersionLocalStoreIndexPath = commandUpsync.Flag("version-local-store-index-path", "Generate an store index optimized for this particular version").String()
	commandUpsynceGetConfigPath             = commandUpsync.Flag("get-config-path", "File uri for json formatted get-config file").String()

	commandDownsync                           = kingpin.Command("downsync", "Download a folder")
	commandDownsyncStorageURI                 = commandDownsync.Flag("storage-uri", "Storage URI (local file system, GCS and S3 bucket URI supported)").Required().String()
	commandDownsyncCachePath                  = commandDownsync.Flag("cache-path", "Location for cached blocks").String()
	commandDownsyncTargetPath                 = commandDownsync.Flag("target-path", "Target folder path").String()
	commandDownsyncTargetIndexPath            = commandDownsync.Flag("target-index-path", "Optional pre-computed index of target-path").String()
	commandDownsyncSourcePath                 = commandDownsync.Flag("source-path", "Source file uri").Required().String()
	commandDownsyncNoRetainPermissions        = commandDownsync.Flag("no-retain-permissions", "Disable setting permission on file/directories from source").Bool()
	commandDownsyncValidate                   = commandDownsync.Flag("validate", "Validate target path once completed").Bool()
	commandDownsyncVersionLocalStoreIndexPath = commandDownsync.Flag("version-local-store-index-path", "Path to an optimized store index for this particular version. If the file can't be read it will fall back to the master store index").String()

	commandGet                           = kingpin.Command("get", "Download a folder using a get-config")
	commandGetConfigUriURI               = commandGet.Flag("get-config-path", "File uri for json formatted get-config file").Required().String()
	commandGetTargetPath                 = commandGet.Flag("target-path", "Target folder path").String()
	commandGetTargetIndexPath            = commandGet.Flag("target-index-path", "Optional pre-computed index of target-path").String()
	commandGetCachePath                  = commandGet.Flag("cache-path", "Location for cached blocks").String()
	commandGetNoRetainPermissions        = commandGet.Flag("no-retain-permissions", "Disable setting permission on file/directories from source").Bool()
	commandGetValidate                   = commandGet.Flag("validate", "Validate target path once completed").Bool()
	commandGetVersionLocalStoreIndexPath = commandGet.Flag("version-local-store-index-path", "Path to an optimized store index for this particular version. If the file can't be read it will fall back to the master store index").String()

	commandValidate                         = kingpin.Command("validate", "Validate a version index against a content store")
	commandValidateStorageURI               = commandValidate.Flag("storage-uri", "Storage URI (local file system, GCS and S3 bucket URI supported)").Required().String()
	commandValidateVersionIndexPath         = commandValidate.Flag("version-index-path", "Path to a version index file").Required().String()
	commandValidateVersionTargetBlockSize   = commandValidate.Flag("target-block-size", "Target block size").Default("8388608").Uint32()
	commandValidateVersionMaxChunksPerBlock = commandValidate.Flag("max-chunks-per-block", "Max chunks per block").Default("1024").Uint32()

	commandPrintVersionIndex        = kingpin.Command("printVersionIndex", "Print info about a file")
	commandPrintVersionIndexPath    = commandPrintVersionIndex.Flag("version-index-path", "Path to a version index file").Required().String()
	commandPrintVersionIndexCompact = commandPrintVersionIndex.Flag("compact", "Show info in compact layout").Bool()

	commandPrintStoreIndex        = kingpin.Command("printStoreIndex", "Print info about a file")
	commandPrintStoreIndexPath    = commandPrintStoreIndex.Flag("store-index-path", "Path to a store index file").Required().String()
	commandPrintStoreIndexCompact = commandPrintStoreIndex.Flag("compact", "Show info in compact layout").Bool()

	commandDump                 = kingpin.Command("dump", "Dump the asset paths inside a version index")
	commandDumpVersionIndexPath = commandDump.Flag("version-index-path", "Path to a version index file").Required().String()
	commandDumpDetails          = commandDump.Flag("details", "Show details about assets").Bool()

	commandLSVersion          = kingpin.Command("ls", "list the content of a path inside a version index")
	commandLSVersionIndexPath = commandLSVersion.Flag("version-index-path", "Path to a version index file").Required().String()
	commandLSVersionDir       = commandLSVersion.Arg("path", "path inside the version index to list").String()

	commandCPVersion           = kingpin.Command("cp", "list the content of a path inside a version index")
	commandCPVersionIndexPath  = commandCPVersion.Flag("version-index-path", "Path to a version index file").Required().String()
	commandCPStorageURI        = commandCPVersion.Flag("storage-uri", "Storage URI (local file system, GCS and S3 bucket URI supported)").Required().String()
	commandCPCachePath         = commandCPVersion.Flag("cache-path", "Location for cached blocks").String()
	commandCPSourcePath        = commandCPVersion.Arg("source path", "source path inside the version index to list").String()
	commandCPTargetPath        = commandCPVersion.Arg("target path", "target uri path").String()
	commandCPTargetBlockSize   = commandCPVersion.Flag("target-block-size", "Target block size").Default("8388608").Uint32()
	commandCPMaxChunksPerBlock = commandCPVersion.Flag("max-chunks-per-block", "Max chunks per block").Default("1024").Uint32()

	commandInitRemoteStore           = kingpin.Command("init", "open/create a remote store and force rebuild the store index")
	commandInitRemoteStoreStorageURI = commandInitRemoteStore.Flag("storage-uri", "Storage URI (local file system, GCS and S3 bucket URI supported)").Required().String()
	commandInitRemoteStoreHashing    = commandInitRemoteStore.Flag("hash-algorithm", "upsync hash algorithm: blake2, blake3, meow").
						Default("blake3").
						Enum("meow", "blake2", "blake3")

	commandStats                 = kingpin.Command("stats", "Show fragmenation stats about a version index")
	commandStatsStorageURI       = commandStats.Flag("storage-uri", "Storage URI (local file system, GCS and S3 bucket URI supported)").Required().String()
	commandStatsVersionIndexPath = commandStats.Flag("version-index-path", "Path to a version index file").Required().String()
	commandStatsCachePath        = commandStats.Flag("cache-path", "Location for cached blocks").String()

	commandCreateVersionStoreIndex           = kingpin.Command("createVersionStoreIndex", "Create a store index optimized for a version index")
	commandCreateVersionStoreIndexStorageURI = commandCreateVersionStoreIndex.Flag("storage-uri", "Storage URI (local file system, GCS and S3 bucket URI supported)").Required().String()
	commandCreateVersionStoreIndexSourcePath = commandCreateVersionStoreIndex.Flag("source-path", "Source file uri").Required().String()
	commandCreateVersionStoreIndexPath       = commandCreateVersionStoreIndex.Flag("version-local-store-index-path", "Generate an store index optimized for this particular version").String()

	commandCloneStore                             = kingpin.Command("cloneStore", "Clone all the data needed to cover a set of versions from one store into a new store")
	commandCloneStoreSourceStoreURI               = commandCloneStore.Flag("source-storage-uri", "Source storage URI (local file system, GCS and S3 bucket URI supported)").Required().String()
	commandCloneStoreTargetStoreURI               = commandCloneStore.Flag("target-storage-uri", "Target storage URI (local file system, GCS and S3 bucket URI supported)").Required().String()
	ommandCloneStoreCachePath                     = commandCloneStore.Flag("cache-path", "Location for cached blocks").String()
	commandCloneStoreTargetPath                   = commandCloneStore.Flag("target-path", "Target folder path").Required().String()
	commandCloneStoreSourcePaths                  = commandCloneStore.Flag("source-paths", "File containing list of source longtail uris").Required().String()
	commandCloneStoreSourceZipPaths               = commandCloneStore.Flag("source-zip-paths", "File containing list of source zip uris").Required().String()
	commandCloneStoreTargetPaths                  = commandCloneStore.Flag("target-paths", "File containing list of target longtail uris").Required().String()
	commandCloneStoreTargetBlockSize              = commandCloneStore.Flag("target-block-size", "Target block size").Default("8388608").Uint32()
	commandCloneStoreMaxChunksPerBlock            = commandCloneStore.Flag("max-chunks-per-block", "Max chunks per block").Default("1024").Uint32()
	commandCloneStoreNoRetainPermissions          = commandCloneStore.Flag("no-retain-permissions", "Disable setting permission on file/directories from source").Bool()
	commandCloneStoreCreateVersionLocalStoreIndex = commandCloneStore.Flag("create-version-local-store-index", "Path to an optimized store index for this particular version. If the file can't be read it will fall back to the master store index").Bool()
	commandCloneStoreHashing                      = commandCloneStore.Flag("hash-algorithm", "upsync hash algorithm: blake2, blake3, meow").
							Default("blake3").
							Enum("meow", "blake2", "blake3")
	commandCloneStoreCompression = commandCloneStore.Flag("compression-algorithm", "compression algorithm: none, brotli[_min|_max], brotli_text[_min|_max], lz4, ztd[_min|_max]").
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
	commandCloneStoreMinBlockUsagePercent = commandCloneStore.Flag("min-block-usage-percent", "Minimum percent of block content than must match for it to be considered \"existing\". Default is zero = use all").Default("0").Uint32()
)

func main() {
	executionStartTime := time.Now()
	initStartTime := executionStartTime

	context := &Context{}

	defer func() {
		executionTime := time.Since(executionStartTime)
		context.TimeStats = append(context.TimeStats, timeStat{"Execution", executionTime})

		if cli.ShowStoreStats {
			for _, s := range context.StoreStats {
				printStats(s.name, s.stats)
			}
		}

		if cli.ShowStats {
			maxLen := 0
			for _, s := range context.TimeStats {
				if len(s.name) > maxLen {
					maxLen = len(s.name)
				}
			}
			for _, s := range context.TimeStats {
				name := fmt.Sprintf("%s:", s.name)
				log.Printf("%-*s %s", maxLen+1, name, s.dur)
			}
		}
	}()

	ctx := kong.Parse(&cli)

	longtailLogLevel, err := parseLevel(cli.LogLevel)
	if err != nil {
		log.Fatal(err)
	}

	longtaillib.SetLogger(&loggerData{})
	defer longtaillib.SetLogger(nil)
	longtaillib.SetLogLevel(longtailLogLevel)

	longtaillib.SetAssert(&assertData{})
	defer longtaillib.SetAssert(nil)

	if *workerCount != 0 {
		numWorkerCount = *workerCount
	}
	/*
		if cli.MemTrace || cli.MemTraceDetailed || cli.MemTraceCSV != "" {
			longtaillib.EnableMemtrace()
			defer func() {
				memTraceLogLevel := longtaillib.MemTraceSummary
				if *memTraceDetailed {
					memTraceLogLevel = longtaillib.MemTraceDetailed
				}
				if *memTraceCSV != "" {
					longtaillib.MemTraceDumpStats(*memTraceCSV)
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
	*/
	initTime := time.Since(initStartTime)

	err = ctx.Run(context)

	context.TimeStats = append([]timeStat{{"Init", initTime}}, context.TimeStats...)

	ctx.FatalIfErrorf(err)
	return
	/*
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

		p := kingpin.Parse()

		if *memTrace || *memTraceDetailed || *memTraceCSV != "" {
			longtaillib.EnableMemtrace()
			defer func() {
				memTraceLogLevel := longtaillib.MemTraceSummary
				if *memTraceDetailed {
					memTraceLogLevel = longtaillib.MemTraceDetailed
				}
				if *memTraceCSV != "" {
					longtaillib.MemTraceDumpStats(*memTraceCSV)
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

		if *workerCount != 0 {
			numWorkerCount = *workerCount
		}

		initTime := time.Since(initStartTime)

		switch p {
		case commandUpsync.FullCommand():
			commandStoreStat, commandTimeStat, err = upSyncVersion(
				*commandUpsyncStorageURI,
				*commandUpsyncSourcePath,
				commandUpsyncSourceIndexPath,
				*commandUpsyncTargetPath,
				*commandUpsyncTargetChunkSize,
				*commandUpsyncTargetBlockSize,
				*commandUpsyncMaxChunksPerBlock,
				commandUpsyncCompression,
				commandUpsyncHashing,
				includeFilterRegEx,
				excludeFilterRegEx,
				*commandUpsyncMinBlockUsagePercent,
				commandUpsyncVersionLocalStoreIndexPath,
				commandUpsynceGetConfigPath)
		case commandDownsync.FullCommand():
			commandStoreStat, commandTimeStat, err = downSyncVersion(
				*commandDownsyncStorageURI,
				*commandDownsyncSourcePath,
				commandDownsyncTargetPath,
				commandDownsyncTargetIndexPath,
				commandDownsyncCachePath,
				!(*commandDownsyncNoRetainPermissions),
				*commandDownsyncValidate,
				commandDownsyncVersionLocalStoreIndexPath,
				includeFilterRegEx,
				excludeFilterRegEx)
		case commandGet.FullCommand():
			commandStoreStat, commandTimeStat, err = getVersion(
				*commandGetConfigUriURI,
				commandGetTargetPath,
				commandGetTargetIndexPath,
				commandGetCachePath,
				!(*commandGetNoRetainPermissions),
				*commandGetValidate,
				includeFilterRegEx,
				excludeFilterRegEx)
		case commandValidate.FullCommand():
			commandStoreStat, commandTimeStat, err = validateVersion(
				*commandValidateStorageURI,
				*commandValidateVersionIndexPath,
				*commandValidateVersionTargetBlockSize,
				*commandValidateVersionMaxChunksPerBlock)
		case commandPrintVersionIndex.FullCommand():
			commandStoreStat, commandTimeStat, err = showVersionIndex(*commandPrintVersionIndexPath, *commandPrintVersionIndexCompact)
		case commandPrintStoreIndex.FullCommand():
			commandStoreStat, commandTimeStat, err = showStoreIndex(*commandPrintStoreIndexPath, *commandPrintStoreIndexCompact)
		case commandDump.FullCommand():
			commandStoreStat, commandTimeStat, err = dumpVersionIndex(*commandDumpVersionIndexPath, *commandDumpDetails)
		case commandLSVersion.FullCommand():
			commandStoreStat, commandTimeStat, err = lsVersionIndex(*commandLSVersionIndexPath, commandLSVersionDir)
		case commandCPVersion.FullCommand():
			commandStoreStat, commandTimeStat, err = cpVersionIndex(
				*commandCPStorageURI,
				*commandCPVersionIndexPath,
				commandCPCachePath,
				*commandCPTargetBlockSize,
				*commandCPMaxChunksPerBlock,
				*commandCPSourcePath,
				*commandCPTargetPath)
		case commandInitRemoteStore.FullCommand():
			commandStoreStat, commandTimeStat, err = initRemoteStore(
				*commandInitRemoteStoreStorageURI,
				commandInitRemoteStoreHashing)
		case commandStats.FullCommand():
			commandStoreStat, commandTimeStat, err = stats(
				*commandStatsStorageURI,
				*commandStatsVersionIndexPath,
				commandStatsCachePath)
		case commandCreateVersionStoreIndex.FullCommand():
			commandStoreStat, commandTimeStat, err = createVersionStoreIndex(
				*commandCreateVersionStoreIndexStorageURI,
				*commandCreateVersionStoreIndexSourcePath,
				*commandCreateVersionStoreIndexPath)
		case commandCloneStore.FullCommand():
			commandStoreStat, commandTimeStat, err = cloneStore(
				*commandCloneStoreSourceStoreURI,
				*commandCloneStoreTargetStoreURI,
				*ommandCloneStoreCachePath,
				*commandCloneStoreTargetPath,
				*commandCloneStoreSourcePaths,
				*commandCloneStoreSourceZipPaths,
				*commandCloneStoreTargetPaths,
				*commandCloneStoreTargetBlockSize,
				*commandCloneStoreMaxChunksPerBlock,
				!(*commandCloneStoreNoRetainPermissions),
				*commandCloneStoreCreateVersionLocalStoreIndex,
				*commandCloneStoreHashing,
				*commandCloneStoreCompression,
				*commandCloneStoreMinBlockUsagePercent)
		}

		commandTimeStat = append([]timeStat{{"Init", initTime}}, commandTimeStat...)

		if err != nil {
			log.Fatal(err)
		}
	*/
}
