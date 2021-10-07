package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
)

func validateOneVersion(
	targetStore longtaillib.Longtail_BlockStoreAPI,
	targetFilePath string,
	skipValidate bool) error {
	tbuffer, err := longtailstorelib.ReadFromURI(targetFilePath)
	if err != nil {
		return err
	}
	if skipValidate {
		fmt.Printf("Skipping `%s`\n", targetFilePath)
		return nil
	}
	fmt.Printf("Validating `%s`\n", targetFilePath)
	targetVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(tbuffer)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateOneVersion: longtaillib.ReadVersionIndexFromBuffer() failed")
	}
	defer targetVersionIndex.Dispose()

	targetStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(targetStore, targetVersionIndex.GetChunkHashes(), 0)
	defer targetStoreIndex.Dispose()
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateOneVersion: longtailutils.GetExistingStoreIndexSync() failed")
	}
	defer targetStoreIndex.Dispose()

	errno = longtaillib.ValidateStore(targetStoreIndex, targetVersionIndex)
	if errno != 0 {
		return errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "validateOneVersion: longtaillib.ValidateStore() failed")
	}
	return nil
}

func Clone(v longtaillib.Longtail_VersionIndex) longtaillib.Longtail_VersionIndex {
	if !v.IsValid() {
		return longtaillib.Longtail_VersionIndex{}
	}
	vbuffer, errno := longtaillib.WriteVersionIndexToBuffer(v)
	if errno != 0 {
		return longtaillib.Longtail_VersionIndex{}
	}
	copy, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return longtaillib.Longtail_VersionIndex{}
	}
	return copy
}

func cloneOneVersion(
	targetPath string,
	jobs longtaillib.Longtail_JobAPI,
	hashRegistry longtaillib.Longtail_HashRegistryAPI,
	fs longtaillib.Longtail_StorageAPI,
	pathFilter longtaillib.Longtail_PathFilterAPI,
	retainPermissions bool,
	createVersionLocalStoreIndex bool,
	skipValidate bool,
	minBlockUsagePercent uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	sourceStore longtaillib.Longtail_BlockStoreAPI,
	targetStore longtaillib.Longtail_BlockStoreAPI,
	sourceRemoteIndexStore longtaillib.Longtail_BlockStoreAPI,
	targetRemoteStore longtaillib.Longtail_BlockStoreAPI,
	targetFilePath string,
	sourceFilePath string,
	sourceFileZipPath string,
	currentVersionIndex longtaillib.Longtail_VersionIndex) (longtaillib.Longtail_VersionIndex, error) {

	targetFolderScanner := asyncFolderScanner{}
	targetFolderScanner.scan(targetPath, pathFilter, fs)

	err := validateOneVersion(targetStore, targetFilePath, skipValidate)
	if err == nil {
		return Clone(currentVersionIndex), nil
	}

	fmt.Printf("`%s` -> `%s`\n", sourceFilePath, targetFilePath)

	vbuffer, err := longtailstorelib.ReadFromURI(sourceFilePath)
	if err != nil {
		fileInfos, _, _ := targetFolderScanner.get()
		fileInfos.Dispose()
		return Clone(currentVersionIndex), err
	}
	sourceVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		fileInfos, _, _ := targetFolderScanner.get()
		fileInfos.Dispose()
		return Clone(currentVersionIndex), err
	}

	hashIdentifier := sourceVersionIndex.GetHashIdentifier()
	targetChunkSize := sourceVersionIndex.GetTargetChunkSize()

	var hash longtaillib.Longtail_HashAPI
	var targetVersionIndex longtaillib.Longtail_VersionIndex

	if currentVersionIndex.IsValid() {
		hash, errno = hashRegistry.GetHashAPI(hashIdentifier)
		if errno != 0 {
			return Clone(currentVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "hashRegistry.GetHashAPI(%d) failed", hashIdentifier)
		}
		targetVersionIndex = Clone(currentVersionIndex)
	} else {
		targetIndexReader := asyncVersionIndexReader{}
		targetIndexReader.read(targetPath,
			"",
			targetChunkSize,
			noCompressionType,
			hashIdentifier,
			pathFilter,
			fs,
			jobs,
			hashRegistry,
			&targetFolderScanner)

		targetVersionIndex, hash, _, err = targetIndexReader.get()
		if err != nil {
			return Clone(currentVersionIndex), err
		}
	}
	defer targetVersionIndex.Dispose()

	versionDiff, errno := longtaillib.CreateVersionDiff(
		hash,
		targetVersionIndex,
		sourceVersionIndex)
	if errno != 0 {
		return Clone(currentVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.CreateVersionDiff() failed")
	}
	defer versionDiff.Dispose()

	chunkHashes, errno := longtaillib.GetRequiredChunkHashes(
		sourceVersionIndex,
		versionDiff)
	if errno != 0 {
		return Clone(currentVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.GetRequiredChunkHashes() failed")
	}

	existingStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(sourceStore, chunkHashes, 0)
	if errno != 0 {
		return Clone(currentVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtailutils.GetExistingStoreIndexSync() failed")
	}
	defer existingStoreIndex.Dispose()

	changeVersionProgress := longtailutils.CreateProgress("Updating version")
	defer changeVersionProgress.Dispose()

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

	var newVersionIndex longtaillib.Longtail_VersionIndex

	if errno == 0 {
		newVersionIndex = Clone(sourceVersionIndex)
	} else {
		if sourceFileZipPath == "" {
			fmt.Printf("Skipping `%s` - unable to download from longtail: %s\n", sourceFilePath, longtaillib.ErrnoToError(errno, longtaillib.ErrEIO).Error())
			return longtaillib.Longtail_VersionIndex{}, err
		}
		fmt.Printf("Falling back to reading ZIP source from `%s`\n", sourceFileZipPath)
		zipBytes, err := longtailstorelib.ReadFromURI(sourceFileZipPath)
		if err != nil {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtailstorelib.ReadFromURI() failed")
		}

		zipReader := bytes.NewReader(zipBytes)

		r, err := zip.NewReader(zipReader, int64(len(zipBytes)))
		if err != nil {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: zip.OpenReader()  failed")
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
				return longtaillib.Longtail_VersionIndex{}, err
			}
		}

		fileInfos, errno := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			normalizePath(targetPath))
		if errno != 0 {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.GetFilesRecursively() failed")
		}
		defer fileInfos.Dispose()

		compressionTypes := getCompressionTypesForFiles(fileInfos, noCompressionType)

		hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
		if errno != 0 {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: hashRegistry.GetHashAPI() failed")
		}

		chunker := longtaillib.CreateHPCDCChunkerAPI()
		defer chunker.Dispose()

		createVersionIndexProgress := longtailutils.CreateProgress("Indexing version")
		defer createVersionIndexProgress.Dispose()
		// Make sure to create an index of what we actually have on disk after update
		newVersionIndex, errno = longtaillib.CreateVersionIndex(
			fs,
			hash,
			chunker,
			jobs,
			&createVersionIndexProgress,
			normalizePath(targetPath),
			fileInfos,
			compressionTypes,
			targetChunkSize)
		if errno != 0 {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.CreateVersionIndex() failed")
		}

		chunkHashes = newVersionIndex.GetChunkHashes()

		// Make sure to update version binary for what we actually have on disk
		vbuffer, errno = longtaillib.WriteVersionIndexToBuffer(newVersionIndex)
		if errno != 0 {
			newVersionIndex.Dispose()
			return longtaillib.Longtail_VersionIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.WriteVersionIndexToBuffer() failed")
		}
	}
	defer newVersionIndex.Dispose()

	newExistingStoreIndex, errno := longtailutils.GetExistingStoreIndexSync(targetStore, newVersionIndex.GetChunkHashes(), minBlockUsagePercent)
	if errno != 0 {
		return Clone(sourceVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtailutils.GetExistingStoreIndexSync() failed")
	}
	defer newExistingStoreIndex.Dispose()

	versionMissingStoreIndex, errno := longtaillib.CreateMissingContent(
		hash,
		newExistingStoreIndex,
		newVersionIndex,
		targetBlockSize,
		maxChunksPerBlock)
	if errno != 0 {
		return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: CreateMissingContent() failed")
	}
	defer versionMissingStoreIndex.Dispose()

	if versionMissingStoreIndex.GetBlockCount() > 0 {
		writeContentProgress := longtailutils.CreateProgress("Writing content blocks")

		errno = longtaillib.WriteContent(
			fs,
			targetStore,
			jobs,
			&writeContentProgress,
			versionMissingStoreIndex,
			newVersionIndex,
			normalizePath(targetPath))
		writeContentProgress.Dispose()
		if errno != 0 {
			return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.WriteContent() failed")
		}
	}

	stores := []longtaillib.Longtail_BlockStoreAPI{
		targetRemoteStore,
		sourceRemoteIndexStore,
	}
	f, errno := longtailutils.FlushStores(stores)
	if errno != 0 {
		return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStores: Failed for `%v`", stores)
	}

	err = longtailstorelib.WriteToURI(targetFilePath, vbuffer)
	if err != nil {
		return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtailstorelib.WriteToURI() failed")
	}

	if createVersionLocalStoreIndex {
		versionLocalStoreIndex, errno := longtaillib.MergeStoreIndex(newExistingStoreIndex, versionMissingStoreIndex)
		if errno != 0 {
			return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.MergeStoreIndex() failed")
		}
		versionLocalStoreIndexBuffer, errno := longtaillib.WriteStoreIndexToBuffer(versionLocalStoreIndex)
		versionLocalStoreIndex.Dispose()
		if errno != 0 {
			return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.WriteStoreIndexToBuffer() failed")
		}
		versionLocalStoreIndexPath := strings.Replace(targetFilePath, ".lvi", ".lsi", -1) // TODO: This should use a file with path names instead of this rename hack!
		err = longtailstorelib.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer)
		if err != nil {
			return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtailstorelib.WriteToURI() failed")
		}
	}

	errno = f.Wait()
	if errno != 0 {
		return Clone(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils.FlushStores: Failed for `%v`", stores)
	}

	return Clone(newVersionIndex), nil
}

func cloneStore(
	numWorkerCount int,
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
	minBlockUsagePercent uint32,
	skipValidate bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

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

	sourceRemoteIndexStore, err := createBlockStoreForURI(sourceStoreURI, "", jobs, numWorkerCount, 8388608, 1024, longtailstorelib.ReadOnly)
	if err != nil {
		return storeStats, timeStats, err
	}
	defer sourceRemoteIndexStore.Dispose()
	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI
	var sourceCompressBlockStore longtaillib.Longtail_BlockStoreAPI

	if len(localCachePath) > 0 {
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, normalizePath(localCachePath))

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

	targetRemoteStore, err := createBlockStoreForURI(targetStoreURI, "", jobs, numWorkerCount, targetBlockSize, maxChunksPerBlock, longtailstorelib.ReadWrite)
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

	var sourcesZipScanner *bufio.Scanner
	if sourceZipPaths != "" {
		sourcesZipFile, err := os.Open(sourceZipPaths)
		if err != nil {
			log.Fatal(err)
		}
		sourcesZipScanner = bufio.NewScanner(sourcesZipFile)
		defer sourcesZipFile.Close()
	}

	targetsFile, err := os.Open(targetPaths)
	if err != nil {
		log.Fatal(err)
	}
	defer targetsFile.Close()

	sourcesScanner := bufio.NewScanner(sourcesFile)
	targetsScanner := bufio.NewScanner(targetsFile)

	var pathFilter longtaillib.Longtail_PathFilterAPI
	var currentVersionIndex longtaillib.Longtail_VersionIndex
	defer currentVersionIndex.Dispose()

	for sourcesScanner.Scan() {
		if !targetsScanner.Scan() {
			break
		}
		sourceFileZipPath := ""
		if sourcesZipScanner != nil {
			if !sourcesZipScanner.Scan() {
				break
			}
			sourceFileZipPath = sourcesZipScanner.Text()
		}

		sourceFilePath := sourcesScanner.Text()
		targetFilePath := targetsScanner.Text()

		newCurrentVersionIndex, err := cloneOneVersion(
			targetPath,
			jobs,
			hashRegistry,
			fs,
			pathFilter,
			retainPermissions,
			createVersionLocalStoreIndex,
			skipValidate,
			minBlockUsagePercent,
			targetBlockSize,
			maxChunksPerBlock,
			sourceStore,
			targetStore,
			sourceRemoteIndexStore,
			targetRemoteStore,
			targetFilePath,
			sourceFilePath,
			sourceFileZipPath,
			currentVersionIndex)
		currentVersionIndex.Dispose()
		currentVersionIndex = newCurrentVersionIndex

		if err != nil {
			return storeStats, timeStats, err
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

type CloneStoreCmd struct {
	SourceStorageURI             string `name:"source-storage-uri" help:"Source storage URI (local file system, GCS and S3 bucket URI supported)" required:""`
	TargetStorageURI             string `name:"target-storage-uri" help:"Target storage URI (local file system, GCS and S3 bucket URI supported)" required:""`
	TargetPath                   string `name:"target-path" help:"Target folder path" required:""`
	SourcePaths                  string `name:"source-paths" help:"File containing list of source longtail uris" required:""`
	SourceZipPaths               string `name:"source-zip-paths" help:"File containing list of source zip uris"`
	TargetPaths                  string `name:"target-paths" help:"File containing list of target longtail uris" required:""`
	CreateVersionLocalStoreIndex bool   `name:"create-version-local-store-index" help:"Generate an store index optimized for the versions"`
	SkipValidate                 bool   `name"skip-validate" help:"Skip validation of already cloned versions"`
	CachePathOption
	RetainPermissionsOption
	MaxChunksPerBlockOption
	TargetBlockSizeOption
	HashingOption
	CompressionOption
	MinBlockUsagePercentOption
}

func (r *CloneStoreCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := cloneStore(
		ctx.NumWorkerCount,
		r.SourceStorageURI,
		r.TargetStorageURI,
		r.CachePath,
		r.TargetPath,
		r.SourcePaths,
		r.SourceZipPaths,
		r.TargetPaths,
		r.TargetBlockSize,
		r.MaxChunksPerBlock,
		r.RetainPermissions,
		r.CreateVersionLocalStoreIndex,
		r.Hashing,
		r.Compression,
		r.MinBlockUsagePercent,
		r.SkipValidate)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
