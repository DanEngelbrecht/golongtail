package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/pkg/errors"

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

	return -1, errors.Wrapf(longtaillib.ErrnoToError(longtaillib.EIO, longtaillib.ErrEIO), "not a valid log Level: %d", lvl)
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

type getExistingContentCompletionAPI struct {
	wg           sync.WaitGroup
	contentIndex longtaillib.Longtail_ContentIndex
	err          int
}

func (a *getExistingContentCompletionAPI) OnComplete(contentIndex longtaillib.Longtail_ContentIndex, err int) {
	a.err = err
	a.contentIndex = contentIndex
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

func getExistingContentIndexSync(indexStore longtaillib.Longtail_BlockStoreAPI, chunkHashes []uint64) (longtaillib.Longtail_ContentIndex, int) {
	getExistingContentComplete := &getExistingContentCompletionAPI{}
	getExistingContentComplete.wg.Add(1)
	errno := indexStore.GetExistingContent(chunkHashes, longtaillib.CreateAsyncGetExistingContentAPI(getExistingContentComplete))
	if errno != 0 {
		getExistingContentComplete.wg.Done()
		return longtaillib.Longtail_ContentIndex{}, errno
	}
	getExistingContentComplete.wg.Wait()
	return getExistingContentComplete.contentIndex, getExistingContentComplete.err
}

func createBlockStoreForURI(uri string, jobAPI longtaillib.Longtail_JobAPI, targetBlockSize uint32, maxChunksPerBlock uint32) (longtaillib.Longtail_BlockStoreAPI, error) {
	blobStoreURL, err := url.Parse(uri)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			gcsBlobStore, err := longtailstorelib.NewGCSBlobStore(blobStoreURL)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, err
			}
			gcsBlockStore, err := longtailstorelib.NewRemoteBlockStore(
				jobAPI,
				gcsBlobStore,
				targetBlockSize,
				maxChunksPerBlock)
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
				targetBlockSize,
				maxChunksPerBlock)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, err
			}
			return longtaillib.CreateBlockStoreAPI(s3BlockStore), nil
		case "abfs":
			return longtaillib.Longtail_BlockStoreAPI{}, fmt.Errorf("Azure Gen1 storage not yet implemented")
		case "abfss":
			return longtaillib.Longtail_BlockStoreAPI{}, fmt.Errorf("Azure Gen2 storage not yet implemented")
		case "file":
			return longtaillib.CreateFSBlockStore(jobAPI, longtaillib.CreateFSStorageAPI(), blobStoreURL.Path[1:], targetBlockSize, maxChunksPerBlock), nil
		}
	}
	return longtaillib.CreateFSBlockStore(jobAPI, longtaillib.CreateFSStorageAPI(), uri, targetBlockSize, maxChunksPerBlock), nil
}

func createBlobStoreForURI(uri string) (longtailstorelib.BlobStore, error) {
	blobStoreURL, err := url.Parse(uri)
	if err == nil {
		switch blobStoreURL.Scheme {
		case "gs":
			return longtailstorelib.NewGCSBlobStore(blobStoreURL)
		case "s3":
			return longtailstorelib.NewS3BlobStore(blobStoreURL)
		case "abfs":
			return nil, fmt.Errorf("Azure Gen1 storage not yet implemented")
		case "abfss":
			return nil, fmt.Errorf("Azure Gen2 storage not yet implemented")
		case "file":
			return longtailstorelib.NewFSBlobStore(blobStoreURL.Path[1:])
		}
	}

	return longtailstorelib.NewFSBlobStore(uri)
}

func splitURI(uri string) (string, string) {
	i := strings.LastIndex(uri, "/")
	if i == -1 {
		i = strings.LastIndex(uri, "\\")
	}
	if i == -1 {
		return "", uri
	}
	return uri[:i], uri[i+1:]
}

func readFromURI(uri string) ([]byte, error) {
	uriParent, uriName := splitURI(uri)
	blobStore, err := createBlobStoreForURI(uriParent)
	if err != nil {
		return nil, err
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		return nil, err
	}
	defer client.Close()
	object, err := client.NewObject(uriName)
	if err != nil {
		return nil, err
	}
	vbuffer, err := object.Read()
	if err != nil {
		return nil, err
	}
	return vbuffer, nil
}

func writeToURI(uri string, data []byte) error {
	uriParent, uriName := splitURI(uri)
	blobStore, err := createBlobStoreForURI(uriParent)
	if err != nil {
		return err
	}
	client, err := blobStore.NewClient(context.Background())
	if err != nil {
		return err
	}
	defer client.Close()
	object, err := client.NewObject(uriName)
	if err != nil {
		return err
	}
	_, err = object.Write(data)
	if err != nil {
		return err
	}
	return nil
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
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
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
	versionContentIndexPath *string,
	targetChunkSize uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	compressionAlgorithm *string,
	hashAlgorithm *string,
	includeFilterRegEx *string,
	excludeFilterRegEx *string,
	showStats bool) error {

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
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	remoteStore, err := createBlockStoreForURI(blobStoreURI, jobs, targetBlockSize, maxChunksPerBlock)
	if err != nil {
		return err
	}
	defer remoteStore.Dispose()

	indexStore := longtaillib.CreateCompressBlockStore(remoteStore, creg)
	defer indexStore.Dispose()

	var hash longtaillib.Longtail_HashAPI

	var vindex longtaillib.Longtail_VersionIndex
	defer vindex.Dispose()

	if sourceIndexPath == nil || len(*sourceIndexPath) == 0 {
		fileInfos, errno := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			normalizePath(sourceFolderPath))
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.GetFilesRecursively(%s) failed", sourceFolderPath)
		}
		defer fileInfos.Dispose()

		compressionType, err := getCompressionType(compressionAlgorithm)
		if err != nil {
			return err
		}
		compressionTypes := getCompressionTypesForFiles(fileInfos, compressionType)

		hashIdentifier, err := getHashIdentifier(hashAlgorithm)
		if err != nil {
			return err
		}

		hash, errno = hashRegistry.GetHashAPI(hashIdentifier)
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: hashRegistry.GetHashAPI(%d) failed", hashIdentifier)
		}

		chunker := longtaillib.CreateHPCDCChunkerAPI()
		defer chunker.Dispose()

		createVersionIndexProgress := longtaillib.CreateProgressAPI(&progressData{task: "Indexing version"})
		defer createVersionIndexProgress.Dispose()
		vindex, errno = longtaillib.CreateVersionIndex(
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
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.CreateVersionIndex(%s)", sourceFolderPath)
		}
	} else {
		vbuffer, err := readFromURI(*sourceIndexPath)
		if err != nil {
			return err
		}
		var errno int
		vindex, errno = longtaillib.ReadVersionIndexFromBuffer(vbuffer)
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.ReadVersionIndexFromBuffer(%s) failed", sourceIndexPath)
		}

		hashIdentifier := vindex.GetHashIdentifier()

		hash, errno = hashRegistry.GetHashAPI(hashIdentifier)
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: hashRegistry.GetHashAPI(%d) failed", hashIdentifier)
		}
	}

	existingRemoteContentIndex, errno := getExistingContentIndexSync(indexStore, vindex.GetChunkHashes())
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.getExistingContentIndexSync(%s) failed", blobStoreURI)
	}
	defer existingRemoteContentIndex.Dispose()

	versionMissingContentIndex, errno := longtaillib.CreateMissingContent(
		hash,
		existingRemoteContentIndex,
		vindex,
		existingRemoteContentIndex.GetMaxBlockSize(),
		existingRemoteContentIndex.GetMaxChunksPerBlock())
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.CreateMissingContent(%s) failed with %s", sourceFolderPath)
	}
	defer versionMissingContentIndex.Dispose()

	if versionMissingContentIndex.GetBlockCount() > 0 {
		writeContentProgress := longtaillib.CreateProgressAPI(&progressData{task: "Writing content blocks"})
		defer writeContentProgress.Dispose()

		errno = longtaillib.WriteContent(
			fs,
			indexStore,
			jobs,
			&writeContentProgress,
			versionMissingContentIndex,
			vindex,
			normalizePath(sourceFolderPath))
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.WriteContent(%s) failed with %s", sourceFolderPath)
		}
	}

	indexStoreFlushComplete := &flushCompletionAPI{}
	indexStoreFlushComplete.wg.Add(1)
	errno = indexStore.Flush(longtaillib.CreateAsyncFlushAPI(indexStoreFlushComplete))
	if errno != 0 {
		indexStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: indexStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	remoteStoreFlushComplete := &flushCompletionAPI{}
	remoteStoreFlushComplete.wg.Add(1)
	errno = remoteStore.Flush(longtaillib.CreateAsyncFlushAPI(remoteStoreFlushComplete))
	if errno != 0 {
		remoteStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	indexStoreFlushComplete.wg.Wait()
	if indexStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: indexStore.Flush: Failed for `%s` failed", blobStoreURI)
	}
	remoteStoreFlushComplete.wg.Wait()
	if remoteStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	indexStoreStats, errno := indexStore.GetStats()
	remoteStoreStats, errno := remoteStore.GetStats()

	if versionContentIndexPath != nil && len(*versionContentIndexPath) > 0 {
		versionLocalContentIndex, errno := longtaillib.AddContentIndex(
			existingRemoteContentIndex,
			versionMissingContentIndex)
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.MergeContentIndex() failed")
		}
		defer versionLocalContentIndex.Dispose()

		cbuffer, errno := longtaillib.WriteContentIndexToBuffer(versionLocalContentIndex)
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.WriteContentIndexToBuffer() failed")
		}

		err = writeToURI(*versionContentIndexPath, cbuffer)
		if err != nil {
			return err
		}
	}

	vbuffer, errno := longtaillib.WriteVersionIndexToBuffer(vindex)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: longtaillib.WriteVersionIndexToBuffer() failed")
	}
	err = writeToURI(targetFilePath, vbuffer)

	if showStats {
		printStats("Compress", indexStoreStats)
		printStats("Remote", remoteStoreStats)
	}

	return nil
}

func downSyncVersion(
	blobStoreURI string,
	sourceFilePath string,
	targetFolderPath string,
	targetIndexPath *string,
	localCachePath *string,
	targetChunkSize uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	retainPermissions bool,
	validate bool,
	includeFilterRegEx *string,
	excludeFilterRegEx *string,
	showStats bool) error {

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
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, jobs, 8388608, 1024)
	if err != nil {
		return err
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

	errno := 0
	var sourceVersionIndex longtaillib.Longtail_VersionIndex

	vbuffer, err := readFromURI(sourceFilePath)
	if err != nil {
		return err
	}
	sourceVersionIndex, errno = longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer sourceVersionIndex.Dispose()

	targetChunkSize = sourceVersionIndex.GetTargetChunkSize()

	hashIdentifier := sourceVersionIndex.GetHashIdentifier()

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.GetHashAPI() failed")
	}

	var targetVersionIndex longtaillib.Longtail_VersionIndex
	if targetIndexPath == nil || len(*targetIndexPath) == 0 {
		fileInfos, errno := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			normalizePath(targetFolderPath))
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.GetFilesRecursively() failed")
		}
		defer fileInfos.Dispose()

		compressionTypes := getCompressionTypesForFiles(fileInfos, noCompressionType)

		chunker := longtaillib.CreateHPCDCChunkerAPI()
		defer chunker.Dispose()

		createVersionIndexProgress := longtaillib.CreateProgressAPI(&progressData{task: "Indexing version"})
		defer createVersionIndexProgress.Dispose()
		targetVersionIndex, errno = longtaillib.CreateVersionIndex(
			fs,
			hash,
			chunker,
			jobs,
			&createVersionIndexProgress,
			normalizePath(targetFolderPath),
			fileInfos,
			compressionTypes,
			targetChunkSize)
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.CreateVersionIndex() failed")
		}
	} else {
		vbuffer, err := readFromURI(*targetIndexPath)
		if err != nil {
			return err
		}
		targetVersionIndex, errno = longtaillib.ReadVersionIndexFromBuffer(vbuffer)
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
		}
	}
	defer targetVersionIndex.Dispose()

	versionDiff, errno := longtaillib.CreateVersionDiff(
		hash,
		targetVersionIndex,
		sourceVersionIndex)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.CreateVersionDiff() failed")
	}
	defer versionDiff.Dispose()

	chunkHashes, errno := longtaillib.GetRequiredChunkHashes(
		sourceVersionIndex,
		versionDiff)

	retargettedVersionContentIndex, errno := getExistingContentIndexSync(indexStore, chunkHashes)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: getExistingContentIndexSync(indexStore, chunkHashes) failed")
	}
	defer retargettedVersionContentIndex.Dispose()

	changeVersionProgress := longtaillib.CreateProgressAPI(&progressData{task: "Updating version"})
	defer changeVersionProgress.Dispose()
	errno = longtaillib.ChangeVersion(
		indexStore,
		fs,
		hash,
		jobs,
		&changeVersionProgress,
		retargettedVersionContentIndex,
		targetVersionIndex,
		sourceVersionIndex,
		versionDiff,
		normalizePath(targetFolderPath),
		retainPermissions)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ChangeVersion() failed")
	}

	indexStoreFlushComplete := &flushCompletionAPI{}
	indexStoreFlushComplete.wg.Add(1)
	errno = indexStore.Flush(longtaillib.CreateAsyncFlushAPI(indexStoreFlushComplete))
	if errno != 0 {
		indexStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: indexStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	lruStoreFlushComplete := &flushCompletionAPI{}
	lruStoreFlushComplete.wg.Add(1)
	errno = lruBlockStore.Flush(longtaillib.CreateAsyncFlushAPI(lruStoreFlushComplete))
	if errno != 0 {
		lruStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: lruBlockStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	compressStoreFlushComplete := &flushCompletionAPI{}
	compressStoreFlushComplete.wg.Add(1)
	errno = compressBlockStore.Flush(longtaillib.CreateAsyncFlushAPI(compressStoreFlushComplete))
	if errno != 0 {
		compressStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: compressStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	cacheStoreFlushComplete := &flushCompletionAPI{}
	cacheStoreFlushComplete.wg.Add(1)
	errno = cacheBlockStore.Flush(longtaillib.CreateAsyncFlushAPI(cacheStoreFlushComplete))
	if errno != 0 {
		cacheStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: cacheStore.Flush: Failed for `%s` failed", localCachePath)
	}

	localStoreFlushComplete := &flushCompletionAPI{}
	localStoreFlushComplete.wg.Add(1)
	errno = localIndexStore.Flush(longtaillib.CreateAsyncFlushAPI(localStoreFlushComplete))
	if errno != 0 {
		localStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: localStore.Flush: Failed for `%s` failed", localCachePath)
	}

	remoteStoreFlushComplete := &flushCompletionAPI{}
	remoteStoreFlushComplete.wg.Add(1)
	errno = remoteIndexStore.Flush(longtaillib.CreateAsyncFlushAPI(remoteStoreFlushComplete))
	if errno != 0 {
		remoteStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	indexStoreFlushComplete.wg.Wait()
	if indexStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: indexStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	lruStoreFlushComplete.wg.Wait()
	if lruStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: lruStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	compressStoreFlushComplete.wg.Wait()
	if compressStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: compressStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	cacheStoreFlushComplete.wg.Wait()
	if cacheStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: cacheStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	localStoreFlushComplete.wg.Wait()
	if localStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: localStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	remoteStoreFlushComplete.wg.Wait()
	if remoteStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	shareStoreStats, shareStoreStatsErrno := indexStore.GetStats()
	lruStoreStats, lruStoreStatsErrno := lruBlockStore.GetStats()
	compressStoreStats, compressStoreStatsErrno := compressBlockStore.GetStats()
	cacheStoreStats, cacheStoreStatsErrno := cacheBlockStore.GetStats()
	localStoreStats, localStoreStatsErrno := localIndexStore.GetStats()
	remoteStoreStats, remoteStoreStatsErrno := remoteIndexStore.GetStats()

	if validate {
		validateFileInfos, errno := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			normalizePath(targetFolderPath))
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.GetFilesRecursively() failed")
		}
		defer validateFileInfos.Dispose()

		chunker := longtaillib.CreateHPCDCChunkerAPI()
		defer chunker.Dispose()

		createVersionIndexProgress := longtaillib.CreateProgressAPI(&progressData{task: "Validating version"})
		defer createVersionIndexProgress.Dispose()
		validateVersionIndex, errno := longtaillib.CreateVersionIndex(
			fs,
			hash,
			chunker,
			jobs,
			&createVersionIndexProgress,
			normalizePath(targetFolderPath),
			validateFileInfos,
			nil,
			targetChunkSize)
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.CreateVersionIndex() failed")
		}
		defer validateVersionIndex.Dispose()
		if validateVersionIndex.GetAssetCount() != sourceVersionIndex.GetAssetCount() {
			return fmt.Errorf("downSyncVersion: failed validation: asset count mismatch")
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
			hash, _ := assetHashLookup[validatePath]
			if !exists {
				return fmt.Errorf("downSyncVersion: failed validation: invalid path %s", validatePath)
			}
			if size != validateSize {
				return fmt.Errorf("downSyncVersion: failed validation: asset %d size mismatch", i)
			}
			if hash != validateHash {
				return fmt.Errorf("downSyncVersion: failed validation: asset %d hash mismatch", i)
			}
			if retainPermissions {
				validatePermissions := validateVersionIndex.GetAssetPermissions(uint32(i))
				permissions := assetPermissionLookup[validatePath]
				if permissions != validatePermissions {
					return fmt.Errorf("downSyncVersion: failed validation: asset %d permission mismatch", i)
				}
			}
		}
	}

	if showStats {
		if shareStoreStatsErrno == 0 {
			printStats("Share", shareStoreStats)
		}
		if lruStoreStatsErrno == 0 {
			printStats("LRU", lruStoreStats)
		}
		if compressStoreStatsErrno == 0 {
			printStats("Compress", compressStoreStats)
		}
		if cacheStoreStatsErrno == 0 {
			printStats("Cache", cacheStoreStats)
		}
		if localStoreStatsErrno == 0 {
			printStats("Local", localStoreStats)
		}
		if remoteStoreStatsErrno == 0 {
			printStats("Remote", remoteStoreStats)
		}
	}

	return nil
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
	maxChunksPerBlock uint32) error {

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	indexStore, err := createBlockStoreForURI(blobStoreURI, jobs, 8388608, 1024)
	if err != nil {
		return err
	}
	defer indexStore.Dispose()

	vbuffer, err := readFromURI(versionIndexPath)
	if err != nil {
		return err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()

	remoteContentIndex, errno := getExistingContentIndexSync(indexStore, versionIndex.GetChunkHashes())
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: getExistingContentIndexSync(indexStore, versionIndex.GetChunkHashes(): Failed for `%s` failed", blobStoreURI)
	}
	defer remoteContentIndex.Dispose()

	errno = longtaillib.ValidateContent(remoteContentIndex, versionIndex)

	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateVersion: longtaillib.ValidateContent() failed")
	}

	return nil
}

func showVersionIndex(versionIndexPath string, compact bool) error {
	vbuffer, err := readFromURI(versionIndexPath)
	if err != nil {
		return err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()

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

	return nil
}

func showContentIndex(contentIndexPath string, compact bool) error {
	vbuffer, err := readFromURI(contentIndexPath)
	if err != nil {
		return err
	}
	contentIndex, errno := longtaillib.ReadContentIndexFromBuffer(vbuffer)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ReadContentIndexFromBuffer() failed")
	}
	defer contentIndex.Dispose()

	if compact {
		fmt.Printf("%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			contentIndexPath,
			contentIndex.GetVersion(),
			hashIdentifierToString(contentIndex.GetHashIdentifier()),
			contentIndex.GetMaxBlockSize(),
			contentIndex.GetMaxChunksPerBlock(),
			contentIndex.GetBlockCount(),
			contentIndex.GetChunkCount())
	} else {
		fmt.Printf("Version:             %d\n", contentIndex.GetVersion())
		fmt.Printf("Hash Identifier:     %s\n", hashIdentifierToString(contentIndex.GetHashIdentifier()))
		fmt.Printf("Max Block Size:      %d\n", contentIndex.GetMaxBlockSize())
		fmt.Printf("Max Chunks Per Block %d\n", contentIndex.GetMaxChunksPerBlock())
		fmt.Printf("Block Count:         %d   (%s)\n", contentIndex.GetBlockCount(), byteCountDecimal(uint64(contentIndex.GetBlockCount())))
		fmt.Printf("Chunk Count:         %d   (%s)\n", contentIndex.GetChunkCount(), byteCountDecimal(uint64(contentIndex.GetChunkCount())))
	}

	return nil
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

func dumpVersionIndex(versionIndexPath string, showDetails bool) error {
	vbuffer, err := readFromURI(versionIndexPath)
	if err != nil {
		return err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()

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
			sizeString := fmt.Sprintf("%d", assetSize)
			sizeString = strings.Repeat(" ", sizePadding-len(sizeString)) + sizeString
			fmt.Printf("%s\n", detailsString)
		} else {
			fmt.Printf("%s\n", path)
		}
	}

	return nil
}

func cpVersionIndex(
	blobStoreURI string,
	versionIndexPath string,
	localCachePath *string,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	sourcePath string,
	targetPath string,
	showStats bool) error {
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, jobs, 8388608, 1024)
	if err != nil {
		return err
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

	vbuffer, err := readFromURI(versionIndexPath)
	if err != nil {
		return err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()

	hashIdentifier := versionIndex.GetHashIdentifier()

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: hashRegistry.GetHashAPI() failed")
	}

	contentIndex, errno := getExistingContentIndexSync(indexStore, versionIndex.GetChunkHashes())
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: getExistingContentIndexSync(indexStore, versionIndex.GetChunkHashes(): Failed for `%s` failed", blobStoreURI)
	}
	defer contentIndex.Dispose()

	blockStoreFS := longtaillib.CreateBlockStoreStorageAPI(
		hash,
		jobs,
		indexStore,
		contentIndex,
		versionIndex)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: hashRegistry.CreateBlockStoreStorageAPI() failed")
	}
	defer blockStoreFS.Dispose()

	// Only support writing to regular file path for now
	outFile, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	inFile, errno := blockStoreFS.OpenReadFile(sourcePath)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: hashRegistry.OpenReadFile() failed")
	}
	defer blockStoreFS.CloseFile(inFile)

	size, errno := blockStoreFS.GetSize(inFile)

	offset := uint64(0)
	for offset < size {
		left := size - offset
		if left > 128*1024*1024 {
			left = 128 * 1024 * 1024
		}
		data, errno := blockStoreFS.Read(inFile, offset, left)
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: hashRegistry.Read() failed")
		}
		outFile.Write(data)
		offset += left
	}

	indexStoreFlushComplete := &flushCompletionAPI{}
	indexStoreFlushComplete.wg.Add(1)
	errno = indexStore.Flush(longtaillib.CreateAsyncFlushAPI(indexStoreFlushComplete))
	if errno != 0 {
		indexStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: indexStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	lruStoreFlushComplete := &flushCompletionAPI{}
	lruStoreFlushComplete.wg.Add(1)
	errno = lruBlockStore.Flush(longtaillib.CreateAsyncFlushAPI(lruStoreFlushComplete))
	if errno != 0 {
		lruStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: lruStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	compressStoreFlushComplete := &flushCompletionAPI{}
	compressStoreFlushComplete.wg.Add(1)
	errno = compressBlockStore.Flush(longtaillib.CreateAsyncFlushAPI(compressStoreFlushComplete))
	if errno != 0 {
		compressStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: compressStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	cacheStoreFlushComplete := &flushCompletionAPI{}
	cacheStoreFlushComplete.wg.Add(1)
	errno = cacheBlockStore.Flush(longtaillib.CreateAsyncFlushAPI(cacheStoreFlushComplete))
	if errno != 0 {
		cacheStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: cacheStore.Flush: Failed for `%s` failed", localCachePath)
	}

	localStoreFlushComplete := &flushCompletionAPI{}
	localStoreFlushComplete.wg.Add(1)
	errno = localIndexStore.Flush(longtaillib.CreateAsyncFlushAPI(localStoreFlushComplete))
	if errno != 0 {
		localStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: localStore.Flush: Failed for `%s` failed", localCachePath)
	}

	remoteStoreFlushComplete := &flushCompletionAPI{}
	remoteStoreFlushComplete.wg.Add(1)
	errno = remoteIndexStore.Flush(longtaillib.CreateAsyncFlushAPI(remoteStoreFlushComplete))
	if errno != 0 {
		remoteStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	indexStoreFlushComplete.wg.Wait()
	if indexStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: indexStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	lruStoreFlushComplete.wg.Wait()
	if lruStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: lruStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	compressStoreFlushComplete.wg.Wait()
	if compressStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: compressStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	cacheStoreFlushComplete.wg.Wait()
	if cacheStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: cacheStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	localStoreFlushComplete.wg.Wait()
	if localStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: localStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	remoteStoreFlushComplete.wg.Wait()
	if remoteStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cpVersionIndex: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}

	shareStoreStats, shareStoreStatsErrno := indexStore.GetStats()
	lruStoreStats, lruStoreStatsErrno := lruBlockStore.GetStats()
	compressStoreStats, compressStoreStatsErrno := compressBlockStore.GetStats()
	cacheStoreStats, cacheStoreStatsErrno := cacheBlockStore.GetStats()
	localStoreStats, localStoreStatsErrno := localIndexStore.GetStats()
	remoteStoreStats, remoteStoreStatsErrno := remoteIndexStore.GetStats()

	if showStats {
		if shareStoreStatsErrno == 0 {
			printStats("Share", shareStoreStats)
		}
		if lruStoreStatsErrno == 0 {
			printStats("LRU", lruStoreStats)
		}
		if compressStoreStatsErrno == 0 {
			printStats("Compress", compressStoreStats)
		}
		if cacheStoreStatsErrno == 0 {
			printStats("Cache", cacheStoreStats)
		}
		if localStoreStatsErrno == 0 {
			printStats("Local", localStoreStats)
		}
		if remoteStoreStatsErrno == 0 {
			printStats("Remote", remoteStoreStats)
		}
	}

	return nil
}

func initRemoteStore(
	blobStoreURI string,
	hashAlgorithm *string,
	showStats bool) error {
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	hashIdentifier, err := getHashIdentifier(hashAlgorithm)
	if err != nil {
		return err
	}

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "initRemoteStore: hashRegistry.GetHashAPI() failed")
	}

	remoteIndexStore, err := createBlockStoreForURI(blobStoreURI, jobs, 8388608, 1024)
	if err != nil {
		return err
	}
	defer remoteIndexStore.Dispose()

	contentIndex, errno := longtaillib.CreateContentIndexRaw(
		hash,
		nil,
		nil,
		nil,
		8388608, 1024)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "initRemoteStore: CreateContentIndexRaw() failed")
	}
	defer contentIndex.Dispose()

	retargetContentIndex, errno := getExistingContentIndexSync(remoteIndexStore, []uint64{})
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "initRemoteStore: getExistingContentIndexSync(indexStore, versionIndex.GetChunkHashes(): Failed for `%s` failed", blobStoreURI)
	}
	defer retargetContentIndex.Dispose()

	remoteStoreFlushComplete := &flushCompletionAPI{}
	remoteStoreFlushComplete.wg.Add(1)
	errno = remoteIndexStore.Flush(longtaillib.CreateAsyncFlushAPI(remoteStoreFlushComplete))
	if errno != 0 {
		remoteStoreFlushComplete.wg.Done()
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "initRemoteStore: remoteStore.Flush: Failed for `%s` failed with with %s", blobStoreURI)
	}

	remoteStoreFlushComplete.wg.Wait()
	if remoteStoreFlushComplete.err != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "initRemoteStore: remoteStore.Flush: Failed for `%s` failed", blobStoreURI)
	}
	remoteStoreStats, remoteStoreStatsErrno := remoteIndexStore.GetStats()

	if showStats {
		if remoteStoreStatsErrno == 0 {
			printStats("Remote", remoteStoreStats)
		}
	}

	return nil
}

func lsVersionIndex(
	versionIndexPath string,
	commandLSVersionDir *string) error {
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	vbuffer, err := readFromURI(versionIndexPath)
	if err != nil {
		return err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer versionIndex.Dispose()

	hashIdentifier := versionIndex.GetHashIdentifier()

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: hashRegistry.GetHashAPI() failed")
	}

	fakeBlockStoreFS := longtaillib.CreateInMemStorageAPI()
	defer fakeBlockStoreFS.Dispose()

	fakeBlockStore := longtaillib.CreateFSBlockStore(jobs, fakeBlockStoreFS, "store", 1024*1024*1024, 1024)
	defer fakeBlockStoreFS.Dispose()

	contentIndex, errno := longtaillib.CreateContentIndex(
		hash,
		versionIndex,
		1024*1024*1024,
		1024)

	blockStoreFS := longtaillib.CreateBlockStoreStorageAPI(
		hash,
		jobs,
		fakeBlockStore,
		contentIndex,
		versionIndex)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: hashRegistry.CreateBlockStoreStorageAPI() failed")
	}
	defer blockStoreFS.Dispose()

	searchDir := ""
	if commandLSVersionDir != nil && *commandLSVersionDir != "." {
		searchDir = *commandLSVersionDir
	}

	iterator, errno := blockStoreFS.StartFind(searchDir)
	if errno == longtaillib.ENOENT {
		return nil
	}
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: hashRegistry.StartFind() failed")
	}
	defer blockStoreFS.CloseFind(iterator)
	for true {
		properties, errno := blockStoreFS.GetEntryProperties(iterator)
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "lsVersionIndex: GetEntryProperties.GetEntryProperties() failed")
		}
		detailsString := getDetailsString(properties.Name, properties.Size, properties.Permissions, properties.IsDir, 16)
		fmt.Printf("%s\n", detailsString)

		errno = blockStoreFS.FindNext(iterator)
		if errno == longtaillib.ENOENT {
			break
		}
		if errno != 0 {
			return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "upSyncVersion: GetEntryProperties.FindNext() failed")
		}
	}
	return nil
}

var (
	logLevel           = kingpin.Flag("log-level", "Log level").Default("warn").Enum("debug", "info", "warn", "error")
	showStats          = kingpin.Flag("show-stats", "Output brief stats summary").Bool()
	includeFilterRegEx = kingpin.Flag("include-filter-regex", "Optional include regex filter for assets in --source-path on upsync and --target-path on downsync. Separate regexes with **").String()
	excludeFilterRegEx = kingpin.Flag("exclude-filter-regex", "Optional exclude regex filter for assets in --source-path on upsync and --target-path on downsync. Separate regexes with **").String()

	commandUpsync           = kingpin.Command("upsync", "Upload a folder")
	commandUpsyncStorageURI = commandUpsync.Flag("storage-uri", "Storage URI (only local file system and GCS bucket URI supported)").Required().String()
	commandUpsyncHashing    = commandUpsync.Flag("hash-algorithm", "upsync hash algorithm: blake2, blake3, meow").
				Default("blake3").
				Enum("meow", "blake2", "blake3")
	commandUpsyncTargetChunkSize         = commandUpsync.Flag("target-chunk-size", "Target chunk size").Default("32768").Uint32()
	commandUpsyncTargetBlockSize         = commandUpsync.Flag("target-block-size", "Target block size").Default("8388608").Uint32()
	commandUpsyncMaxChunksPerBlock       = commandUpsync.Flag("max-chunks-per-block", "Max chunks per block").Default("1024").Uint32()
	commandUpsyncSourcePath              = commandUpsync.Flag("source-path", "Source folder path").Required().String()
	commandUpsyncSourceIndexPath         = commandUpsync.Flag("source-index-path", "Optional pre-computed index of source-path").String()
	commandUpsyncTargetPath              = commandUpsync.Flag("target-path", "Target file uri").Required().String()
	commandUpsyncVersionContentIndexPath = commandUpsync.Flag("version-content-index-path", "Version local content index file uri").String()
	commandUpsyncCompression             = commandUpsync.Flag("compression-algorithm", "compression algorithm: none, brotli[_min|_max], brotli_text[_min|_max], lz4, ztd[_min|_max]").
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

	commandDownsync                    = kingpin.Command("downsync", "Download a folder")
	commandDownsyncStorageURI          = commandDownsync.Flag("storage-uri", "Storage URI (only local file system and GCS bucket URI supported)").Required().String()
	commandDownsyncCachePath           = commandDownsync.Flag("cache-path", "Location for cached blocks").String()
	commandDownsyncTargetPath          = commandDownsync.Flag("target-path", "Target folder path").Required().String()
	commandDownsyncTargetIndexPath     = commandDownsync.Flag("target-index-path", "Optional pre-computed index of target-path").String()
	commandDownsyncSourcePath          = commandDownsync.Flag("source-path", "Source file uri").Required().String()
	commandDownsyncTargetChunkSize     = commandDownsync.Flag("target-chunk-size", "Target chunk size").Default("32768").Uint32()
	commandDownsyncTargetBlockSize     = commandDownsync.Flag("target-block-size", "Target block size").Default("8388608").Uint32()
	commandDownsyncMaxChunksPerBlock   = commandDownsync.Flag("max-chunks-per-block", "Max chunks per block").Default("1024").Uint32()
	commandDownsyncNoRetainPermissions = commandDownsync.Flag("no-retain-permissions", "Disable setting permission on file/directories from source").Bool()
	commandDownsyncValidate            = commandDownsync.Flag("validate", "Validate target path once completed").Bool()

	commandValidate                         = kingpin.Command("validate", "Validate a version index against a content store")
	commandValidateStorageURI               = commandValidate.Flag("storage-uri", "Storage URI (only local file system and GCS bucket URI supported)").Required().String()
	commandValidateVersionIndexPath         = commandValidate.Flag("version-index-path", "Path to a version index file").Required().String()
	commandValidateVersionTargetBlockSize   = commandValidate.Flag("target-block-size", "Target block size").Default("8388608").Uint32()
	commandValidateVersionMaxChunksPerBlock = commandValidate.Flag("max-chunks-per-block", "Max chunks per block").Default("1024").Uint32()

	commandPrintVersionIndex        = kingpin.Command("commandPrintVersionIndex", "Print info about a file")
	commandPrintVersionIndexPath    = commandPrintVersionIndex.Flag("version-index-path", "Path to a version index file").Required().String()
	commandPrintVersionIndexCompact = commandPrintVersionIndex.Flag("compact", "Show info in compact layout").Bool()

	commandPrintContentIndex        = kingpin.Command("commandPrintContentIndex", "Print info about a file")
	commandPrintContentIndexPath    = commandPrintContentIndex.Flag("content-index-path", "Path to a content index file").Required().String()
	commandPrintContentIndexCompact = commandPrintContentIndex.Flag("compact", "Show info in compact layout").Bool()

	commandDump                 = kingpin.Command("dump", "Dump the asset paths inside a version index")
	commandDumpVersionIndexPath = commandDump.Flag("version-index-path", "Path to a version index file").Required().String()
	commandDumpDetails          = commandDump.Flag("details", "Show details about assets").Bool()

	commandLSVersion          = kingpin.Command("ls", "list the content of a path inside a version index")
	commandLSVersionIndexPath = commandLSVersion.Flag("version-index-path", "Path to a version index file").Required().String()
	commandLSVersionDir       = commandLSVersion.Arg("path", "path inside the version index to list").String()

	commandCPVersion           = kingpin.Command("cp", "list the content of a path inside a version index")
	commandCPVersionIndexPath  = commandCPVersion.Flag("version-index-path", "Path to a version index file").Required().String()
	commandCPStorageURI        = commandCPVersion.Flag("storage-uri", "Storage URI (only local file system and GCS bucket URI supported)").Required().String()
	commandCPCachePath         = commandCPVersion.Flag("cache-path", "Location for cached blocks").String()
	commandCPSourcePath        = commandCPVersion.Arg("source path", "source path inside the version index to list").String()
	commandCPTargetPath        = commandCPVersion.Arg("target path", "target uri path").String()
	commandCPTargetBlockSize   = commandCPVersion.Flag("target-block-size", "Target block size").Default("8388608").Uint32()
	commandCPMaxChunksPerBlock = commandCPVersion.Flag("max-chunks-per-block", "Max chunks per block").Default("1024").Uint32()

	commandInitRemoteStore           = kingpin.Command("init", "open a remote store triggering reindexing of store index is missing")
	commandInitRemoteStoreStorageURI = commandInitRemoteStore.Flag("storage-uri", "Storage URI (only local file system and GCS bucket URI supported)").Required().String()
	commandInitRemoteStoreHashing    = commandInitRemoteStore.Flag("hash-algorithm", "upsync hash algorithm: blake2, blake3, meow").
						Default("blake3").
						Enum("meow", "blake2", "blake3")
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
	case commandUpsync.FullCommand():
		err := upSyncVersion(
			*commandUpsyncStorageURI,
			*commandUpsyncSourcePath,
			commandUpsyncSourceIndexPath,
			*commandUpsyncTargetPath,
			commandUpsyncVersionContentIndexPath,
			*commandUpsyncTargetChunkSize,
			*commandUpsyncTargetBlockSize,
			*commandUpsyncMaxChunksPerBlock,
			commandUpsyncCompression,
			commandUpsyncHashing,
			includeFilterRegEx,
			excludeFilterRegEx,
			*showStats)
		if err != nil {
			log.Fatal(err)
		}
	case commandDownsync.FullCommand():
		err := downSyncVersion(
			*commandDownsyncStorageURI,
			*commandDownsyncSourcePath,
			*commandDownsyncTargetPath,
			commandDownsyncTargetIndexPath,
			commandDownsyncCachePath,
			*commandDownsyncTargetChunkSize,
			*commandDownsyncTargetBlockSize,
			*commandDownsyncMaxChunksPerBlock,
			!(*commandDownsyncNoRetainPermissions),
			*commandDownsyncValidate,
			includeFilterRegEx,
			excludeFilterRegEx,
			*showStats)
		if err != nil {
			log.Fatal(err)
		}
	case commandValidate.FullCommand():
		err := validateVersion(
			*commandValidateStorageURI,
			*commandValidateVersionIndexPath,
			*commandValidateVersionTargetBlockSize,
			*commandValidateVersionMaxChunksPerBlock)
		if err != nil {
			log.Fatal(err)
		}
	case commandPrintVersionIndex.FullCommand():
		err := showVersionIndex(*commandPrintVersionIndexPath, *commandPrintVersionIndexCompact)
		if err != nil {
			log.Fatal(err)
		}
	case commandPrintContentIndex.FullCommand():
		err := showContentIndex(*commandPrintContentIndexPath, *commandPrintContentIndexCompact)
		if err != nil {
			log.Fatal(err)
		}
	case commandDump.FullCommand():
		err := dumpVersionIndex(*commandDumpVersionIndexPath, *commandDumpDetails)
		if err != nil {
			log.Fatal(err)
		}
	case commandLSVersion.FullCommand():
		err := lsVersionIndex(*commandLSVersionIndexPath, commandLSVersionDir)
		if err != nil {
			log.Fatal(err)
		}
	case commandCPVersion.FullCommand():
		err := cpVersionIndex(
			*commandCPStorageURI,
			*commandCPVersionIndexPath,
			commandCPCachePath,
			*commandCPTargetBlockSize,
			*commandCPMaxChunksPerBlock,
			*commandCPSourcePath,
			*commandCPTargetPath,
			*showStats)
		if err != nil {
			log.Fatal(err)
		}
	case commandInitRemoteStore.FullCommand():
		err := initRemoteStore(
			*commandInitRemoteStoreStorageURI,
			commandInitRemoteStoreHashing,
			*showStats)
		if err != nil {
			log.Fatal(err)
		}
	}
}
