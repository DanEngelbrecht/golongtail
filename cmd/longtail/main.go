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

type retargetContentIndexCompletionAPI struct {
	wg           sync.WaitGroup
	contentIndex longtaillib.Longtail_ContentIndex
	err          int
}

func (a *retargetContentIndexCompletionAPI) OnComplete(contentIndex longtaillib.Longtail_ContentIndex, err int) {
	a.err = err
	a.contentIndex = contentIndex
	a.wg.Done()
}

func retargetContentIndexSync(indexStore longtaillib.Longtail_BlockStoreAPI, contentIndex longtaillib.Longtail_ContentIndex) (longtaillib.Longtail_ContentIndex, int) {
	retargetContentComplete := &retargetContentIndexCompletionAPI{}
	retargetContentComplete.wg.Add(1)
	errno := indexStore.RetargetContent(contentIndex, longtaillib.CreateAsyncRetargetContentAPI(retargetContentComplete))
	if errno != 0 {
		retargetContentComplete.wg.Done()
		return longtaillib.Longtail_ContentIndex{}, errno
	}
	retargetContentComplete.wg.Wait()
	return retargetContentComplete.contentIndex, retargetContentComplete.err
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
			fsBlockStore, err := longtailstorelib.NewFSBlockStore(blobStoreURL.Path[1:], jobAPI, targetBlockSize, maxChunksPerBlock)
			if err != nil {
				return longtaillib.Longtail_BlockStoreAPI{}, err
			}
			return longtaillib.CreateBlockStoreAPI(fsBlockStore), nil
		}
	}
	return longtaillib.CreateFSBlockStore(longtaillib.CreateFSStorageAPI(), uri, targetBlockSize, maxChunksPerBlock), nil
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
	versionContentIndexPath *string,
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
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()
	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	remoteStore, err := createBlockStoreForURI(blobStoreURI, jobs, targetBlockSize, maxChunksPerBlock, outFinalStats)
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
			sourceFolderPath)
		if errno != 0 {
			return fmt.Errorf("upSyncVersion: longtaillib.GetFilesRecursively() failed with %s", longtaillib.ErrNoToDescription(errno))
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
			return fmt.Errorf("upSyncVersion: hashRegistry.GetHashAPI() failed with %s", longtaillib.ErrNoToDescription(errno))
		}

		createVersionIndexProgress := longtaillib.CreateProgressAPI(&progressData{task: "Indexing version"})
		defer createVersionIndexProgress.Dispose()
		vindex, errno = longtaillib.CreateVersionIndex(
			fs,
			hash,
			jobs,
			&createVersionIndexProgress,
			sourceFolderPath,
			fileInfos,
			compressionTypes,
			targetChunkSize)
		if errno != 0 {
			return fmt.Errorf("upSyncVersion: longtaillib.CreateVersionIndex() failed with %s", longtaillib.ErrNoToDescription(errno))
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
		var errno int
		vindex, errno = longtaillib.ReadVersionIndexFromBuffer(vbuffer)
		if errno != 0 {
			return fmt.Errorf("upSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
		}

		hashIdentifier := vindex.GetHashIdentifier()

		hash, errno = hashRegistry.GetHashAPI(hashIdentifier)
		if errno != 0 {
			return fmt.Errorf("upSyncVersion: hashRegistry.GetHashAPI() failed with %s", longtaillib.ErrNoToDescription(errno))
		}
	}

	versionContentIndex, errno := longtaillib.CreateContentIndex(
		hash,
		vindex,
		targetBlockSize,
		maxChunksPerBlock)
	if errno != 0 {
		return fmt.Errorf("upSyncVersion: hashRegistry.CreateContentIndex() failed with %s", longtaillib.ErrNoToDescription(errno))
	}
	defer versionContentIndex.Dispose()

	existingRemoteContentIndex, errno := retargetContentIndexSync(indexStore, versionContentIndex)
	defer existingRemoteContentIndex.Dispose()

	versionMissingContentIndex, errno := longtaillib.CreateMissingContent(
		hash,
		existingRemoteContentIndex,
		vindex,
		targetBlockSize,
		maxChunksPerBlock)
	if errno != 0 {
		return fmt.Errorf("upSyncVersion: longtaillib.CreateMissingContent() failed with %s", longtaillib.ErrNoToDescription(errno))
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
			existingRemoteContentIndex,
			versionMissingContentIndex,
			vindex,
			sourceFolderPath)
		if errno != 0 {
			return fmt.Errorf("upSyncVersion: longtaillib.WriteContent() failed with %s", longtaillib.ErrNoToDescription(errno))
		}
	}

	// Explicitly close the block stores so they flush their index
	indexStore.Dispose()
	remoteStore.Dispose()

	if versionContentIndexPath != nil && len(*versionContentIndexPath) > 0 {
		versionLocalContentIndex, errno := longtaillib.MergeContentIndex(
			existingRemoteContentIndex,
			versionMissingContentIndex)
		if errno != 0 {
			return fmt.Errorf("upSyncVersion: longtaillib.MergeContentIndex() failed with %s", longtaillib.ErrNoToDescription(errno))
		}
		defer versionLocalContentIndex.Dispose()

		versionContentIndexStorage, err := createFileStorageForURI(*versionContentIndexPath)
		if err != nil {
			return nil
		}
		defer versionContentIndexStorage.Close()
		cbuffer, errno := longtaillib.WriteContentIndexToBuffer(versionLocalContentIndex)
		if errno != 0 {
			return fmt.Errorf("upSyncVersion: longtaillib.WriteContentIndexToBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
		}
		err = versionContentIndexStorage.WriteToPath(context.Background(), *versionContentIndexPath, cbuffer)
		if err != nil {
			return err
		}
	}

	vbuffer, errno := longtaillib.WriteVersionIndexToBuffer(vindex)
	if errno != 0 {
		return fmt.Errorf("upSyncVersion: longtaillib.WriteVersionIndexToBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
	}
	versionIndexFileStorage, err := createFileStorageForURI(targetFilePath)
	if err != nil {
		return nil
	}
	defer versionIndexFileStorage.Close()
	err = versionIndexFileStorage.WriteToPath(context.Background(), targetFilePath, vbuffer)
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
	localCachePath *string,
	targetChunkSize uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
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
	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
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

	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI
	var compressBlockStore longtaillib.Longtail_BlockStoreAPI
	var indexStore longtaillib.Longtail_BlockStoreAPI

	if localCachePath != nil {
		localIndexStore = longtaillib.CreateFSBlockStore(localFS, *localCachePath, 8388608, 1024)

		cacheBlockStore = longtaillib.CreateCacheBlockStore(localIndexStore, remoteIndexStore)

		compressBlockStore = longtaillib.CreateCompressBlockStore(cacheBlockStore, creg)
	} else {
		compressBlockStore = longtaillib.CreateCompressBlockStore(remoteIndexStore, creg)
	}

	defer cacheBlockStore.Dispose()
	defer localIndexStore.Dispose()
	defer compressBlockStore.Dispose()

	indexStore = longtaillib.CreateShareBlockStore(compressBlockStore)
	defer indexStore.Dispose()

	errno := 0
	var sourceVersionIndex longtaillib.Longtail_VersionIndex

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
		sourceVersionIndex, errno = longtaillib.ReadVersionIndexFromBuffer(vbuffer)
		if errno != 0 {
			return fmt.Errorf("downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
		}
	}
	defer sourceVersionIndex.Dispose()

	targetChunkSize = sourceVersionIndex.GetTargetChunkSize()

	hashIdentifier := sourceVersionIndex.GetHashIdentifier()

	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return fmt.Errorf("downSyncVersion: longtaillib.GetHashAPI() failed with %s", longtaillib.ErrNoToDescription(errno))
	}

	var targetVersionIndex longtaillib.Longtail_VersionIndex
	if targetIndexPath == nil || len(*targetIndexPath) == 0 {
		fileInfos, errno := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			targetFolderPath)
		if errno != 0 {
			return fmt.Errorf("downSyncVersion: longtaillib.GetFilesRecursively() failed with %s", longtaillib.ErrNoToDescription(errno))
		}
		defer fileInfos.Dispose()

		compressionTypes := getCompressionTypesForFiles(fileInfos, noCompressionType)

		createVersionIndexProgress := longtaillib.CreateProgressAPI(&progressData{task: "Indexing version"})
		defer createVersionIndexProgress.Dispose()
		targetVersionIndex, errno = longtaillib.CreateVersionIndex(
			fs,
			hash,
			jobs,
			&createVersionIndexProgress,
			targetFolderPath,
			fileInfos,
			compressionTypes,
			targetChunkSize)
		if errno != 0 {
			return fmt.Errorf("downSyncVersion: longtaillib.CreateVersionIndex() failed with %s", longtaillib.ErrNoToDescription(errno))
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
		targetVersionIndex, errno = longtaillib.ReadVersionIndexFromBuffer(vbuffer)
		if errno != 0 {
			return fmt.Errorf("downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
		}
	}
	defer targetVersionIndex.Dispose()

	versionDiff, errno := longtaillib.CreateVersionDiff(targetVersionIndex, sourceVersionIndex)
	if errno != 0 {
		return fmt.Errorf("downSyncVersion: longtaillib.CreateVersionDiff() failed with %s", longtaillib.ErrNoToDescription(errno))
	}
	defer versionDiff.Dispose()

	sourceVersionContentIndex, errno := longtaillib.CreateContentIndex(
		hash,
		sourceVersionIndex,
		targetBlockSize,
		maxChunksPerBlock)
	if errno != 0 {
		return fmt.Errorf("downSyncVersion: longtaillib.CreateContentIndex() failed with %s", longtaillib.ErrNoToDescription(errno))
	}
	defer sourceVersionContentIndex.Dispose()

	retargettedVersionContentIndex, errno := retargetContentIndexSync(indexStore, sourceVersionContentIndex)
	if errno != 0 {
		return fmt.Errorf("downSyncVersion: indexStore.RetargetContent() failed with %s", longtaillib.ErrNoToDescription(errno))
	}
	defer retargettedVersionContentIndex.Dispose()

	errno = longtaillib.ValidateContent(retargettedVersionContentIndex, sourceVersionIndex)
	if errno != 0 {
		return fmt.Errorf("downSyncVersion: indexStore.ValidateContent() failed with %s", longtaillib.ErrNoToDescription(errno))
	}

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
		targetFolderPath,
		retainPermissions)
	if errno != 0 {
		return fmt.Errorf("downSyncVersion: longtaillib.ChangeVersion() failed with %s", longtaillib.ErrNoToDescription(errno))
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
	versionIndexPath string) error {

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(runtime.NumCPU()), 0)
	defer jobs.Dispose()

	// MaxBlockSize and MaxChunksPerBlock are just temporary values until we get the remote index settings
	indexStore, err := createBlockStoreForURI(blobStoreURI, jobs, 8388608, 1024, nil)
	if err != nil {
		return err
	}
	defer indexStore.Dispose()

	getIndexComplete := &getIndexCompletionAPI{}
	getIndexComplete.wg.Add(1)
	errno := indexStore.GetIndex(longtaillib.CreateAsyncGetIndexAPI(getIndexComplete))
	if errno != 0 {
		getIndexComplete.wg.Done()
		return fmt.Errorf("validateVersion: indexStore.GetIndex: Failed for `%s` failed with with %s", blobStoreURI, longtaillib.ErrNoToDescription(errno))
	}
	getIndexComplete.wg.Wait()
	if getIndexComplete.err != 0 {
		return fmt.Errorf("validateVersion: indexStore.GetIndex: Failed for `%s` failed with with %s", blobStoreURI, longtaillib.ErrNoToDescription(errno))
	}
	remoteContentIndex := getIndexComplete.contentIndex
	defer remoteContentIndex.Dispose()

	fileStorage, err := createFileStorageForURI(versionIndexPath)
	if err != nil {
		return nil
	}
	defer fileStorage.Close()
	vbuffer, err := fileStorage.ReadFromPath(context.Background(), versionIndexPath)
	if err != nil {
		return err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return fmt.Errorf("validateVersion: longtaillib.ReadVersionIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
	}
	defer versionIndex.Dispose()

	errno = longtaillib.ValidateContent(remoteContentIndex, versionIndex)

	if errno != 0 {
		return fmt.Errorf("validateVersion: longtaillib.ValidateContent() failed with %s", longtaillib.ErrNoToDescription(errno))
	}

	return nil
}

func showVersionIndex(versionIndexPath string, compact bool) error {

	fileStorage, err := createFileStorageForURI(versionIndexPath)
	if err != nil {
		return nil
	}
	defer fileStorage.Close()
	vbuffer, err := fileStorage.ReadFromPath(context.Background(), versionIndexPath)
	if err != nil {
		return err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return fmt.Errorf("downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
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

	fileStorage, err := createFileStorageForURI(contentIndexPath)
	if err != nil {
		return nil
	}
	defer fileStorage.Close()
	vbuffer, err := fileStorage.ReadFromPath(context.Background(), contentIndexPath)
	if err != nil {
		return err
	}
	contentIndex, errno := longtaillib.ReadContentIndexFromBuffer(vbuffer)
	if errno != 0 {
		return fmt.Errorf("downSyncVersion: longtaillib.ReadContentIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
	}
	defer contentIndex.Dispose()

	var smallestChunkSize uint32
	var largestChunkSize uint32
	var averageChunkSize uint32
	var totalChunkSize uint64
	totalChunkSize = 0
	chunkSizes := contentIndex.GetChunkSizes()
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
	if compact {
		fmt.Printf("%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			contentIndexPath,
			contentIndex.GetVersion(),
			hashIdentifierToString(contentIndex.GetHashIdentifier()),
			contentIndex.GetMaxBlockSize(),
			contentIndex.GetMaxChunksPerBlock(),
			contentIndex.GetBlockCount(),
			contentIndex.GetChunkCount(),
			totalChunkSize,
			averageChunkSize,
			smallestChunkSize,
			largestChunkSize)
	} else {
		fmt.Printf("Version:             %d\n", contentIndex.GetVersion())
		fmt.Printf("Hash Identifier:     %s\n", hashIdentifierToString(contentIndex.GetHashIdentifier()))
		fmt.Printf("Max Block Size:      %d\n", contentIndex.GetMaxBlockSize())
		fmt.Printf("Max Chunks Per Block %d\n", contentIndex.GetMaxChunksPerBlock())
		fmt.Printf("Block Count:         %d   (%s)\n", contentIndex.GetBlockCount(), byteCountDecimal(uint64(contentIndex.GetBlockCount())))
		fmt.Printf("Chunk Count:         %d   (%s)\n", contentIndex.GetChunkCount(), byteCountDecimal(uint64(contentIndex.GetChunkCount())))
		fmt.Printf("Chunk Total Size:    %d   (%s)\n", totalChunkSize, byteCountBinary(totalChunkSize))
		fmt.Printf("Average Chunk Size:  %d   (%s)\n", averageChunkSize, byteCountBinary(uint64(averageChunkSize)))
		fmt.Printf("Smallest Chunk Size: %d   (%s)\n", smallestChunkSize, byteCountBinary(uint64(smallestChunkSize)))
		fmt.Printf("Largest Chunk Size:  %d   (%s)\n", largestChunkSize, byteCountBinary(uint64(largestChunkSize)))
	}

	return nil
}

func listVersionIndex(versionIndexPath string, showDetails bool) error {
	fileStorage, err := createFileStorageForURI(versionIndexPath)
	if err != nil {
		return nil
	}
	defer fileStorage.Close()
	vbuffer, err := fileStorage.ReadFromPath(context.Background(), versionIndexPath)
	if err != nil {
		return err
	}
	versionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return fmt.Errorf("downSyncVersion: longtaillib.ReadVersionIndexFromBuffer() failed with %s", longtaillib.ErrNoToDescription(errno))
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
			assetSize := versionIndex.GetAssetSize(i)
			sizeString := fmt.Sprintf("%d", assetSize)
			sizeString = strings.Repeat(" ", sizePadding-len(sizeString)) + sizeString
			permissions := versionIndex.GetAssetPermissions(i)
			bits := ""
			if strings.HasSuffix(path, "/") {
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

			fmt.Printf("%s %s %s\n", bits, sizeString, path)
		} else {
			fmt.Printf("%s\n", path)
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
	commandUpsyncCompression             = commandUpsync.Flag("commandUpsyncCompression-algorithm", "commandUpsyncCompression algorithm: none, brotli[_min|_max], brotli_text[_min|_max], lz4, ztd[_min|_max]").
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

	commandValidate                 = kingpin.Command("validate", "Validate a version index against a content store")
	commandValidateStorageURI       = commandValidate.Flag("storage-uri", "Storage URI (only local file system and GCS bucket URI supported)").Required().String()
	commandValidateVersionIndexPath = commandValidate.Flag("version-index-path", "Path to a version index file").Required().String()

	commandPrintVersionIndex        = kingpin.Command("commandPrintVersionIndex", "Print info about a file")
	commandPrintVersionIndexPath    = commandPrintVersionIndex.Flag("version-index-path", "Path to a version index file").Required().String()
	commandPrintVersionIndexCompact = commandPrintVersionIndex.Flag("compact", "Show info in compact layout").Bool()

	commandPrintContentIndex        = kingpin.Command("commandPrintContentIndex", "Print info about a file")
	commandPrintContentIndexPath    = commandPrintContentIndex.Flag("content-index-path", "Path to a content index file").Required().String()
	commandPrintContentIndexCompact = commandPrintContentIndex.Flag("compact", "Show info in compact layout").Bool()

	commandList                 = kingpin.Command("ls", "List the asset paths inside a version index")
	commandListVersionIndexPath = commandList.Flag("version-index-path", "Path to a version index file").Required().String()
	commandListDetails          = commandList.Flag("details", "Show details about assets").Bool()
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
			&stats)
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
			includeFilterRegEx,
			excludeFilterRegEx,
			&stats)
		if err != nil {
			log.Fatal(err)
		}
	case commandValidate.FullCommand():
		err := validateVersion(*commandValidateStorageURI, *commandValidateVersionIndexPath)
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
	case commandList.FullCommand():
		err := listVersionIndex(*commandListVersionIndexPath, *commandListDetails)
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
