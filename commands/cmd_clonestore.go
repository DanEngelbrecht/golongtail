package commands

import (
	"archive/zip"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/DanEngelbrecht/golongtail/remotestore"
	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"
)

func validateOneVersion(
	targetStore longtaillib.Longtail_BlockStoreAPI,
	targetFilePath string,
	skipValidate bool) error {
	log := logrus.WithFields(logrus.Fields{
		"targetFilePath": targetFilePath,
		"skipValidate":   skipValidate,
	})
	tbuffer, err := longtailutils.ReadFromURI(targetFilePath)
	if err != nil {
		return err
	}
	if tbuffer == nil {
		err = errors.Wrapf(longtaillib.ErrENOENT, "File does not exist: %s", targetFilePath)
		log.WithError(err).Debug("validateOneVersion")
		return err
	}

	if skipValidate {
		log.Infof("Skipping `%s`", targetFilePath)
		return nil
	}
	log.Infof("Validating `%s`", targetFilePath)
	targetVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(tbuffer)
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Can't parse version index from `%s`", targetFilePath))
		log.WithError(err).Info("validateOneVersion")
		return err
	}
	defer targetVersionIndex.Dispose()

	targetStoreIndex, err := longtailutils.GetExistingStoreIndexSync(
		targetStore,
		targetVersionIndex.GetChunkHashes(),
		0)
	if err != nil {
		err = errors.Wrapf(err, "Failed getting store index for target "+targetFilePath)
		log.WithError(err).Info("validateOneVersion")
		return err
	}
	defer targetStoreIndex.Dispose()

	errno = longtaillib.ValidateStore(targetStoreIndex, targetVersionIndex)
	if errno != 0 {
		err = longtailutils.MakeError(errno, fmt.Sprintf("Validate failed for version index `%s`", targetFilePath))
		log.WithError(err).Info("validateOneVersion")
		return err
	}
	return nil
}

func CloneVersionIndex(v longtaillib.Longtail_VersionIndex) longtaillib.Longtail_VersionIndex {
	if !v.IsValid() {
		return longtaillib.Longtail_VersionIndex{}
	}
	log := logrus.WithFields(logrus.Fields{
		"v": v,
	})
	vbuffer, errno := longtaillib.WriteVersionIndexToBuffer(v)
	if errno != 0 {
		err := longtailutils.MakeError(errno, "longtaillib.WriteVersionIndexToBuffer() failed")
		log.WithError(err).Info("CloneVersionIndex failed")
		return longtaillib.Longtail_VersionIndex{}
	}
	copy, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		err := longtailutils.MakeError(errno, "longtaillib.ReadVersionIndexFromBuffer() failed")
		log.WithError(err).Info("CloneVersionIndex failed")
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
	log := logrus.WithFields(logrus.Fields{
		"targetPath":                   targetPath,
		"retainPermissions":            retainPermissions,
		"createVersionLocalStoreIndex": createVersionLocalStoreIndex,
		"skipValidate":                 skipValidate,
		"minBlockUsagePercent":         minBlockUsagePercent,
		"targetBlockSize":              targetBlockSize,
		"maxChunksPerBlock":            maxChunksPerBlock,
		"targetFilePath":               targetFilePath,
		"sourceFilePath":               sourceFilePath,
		"sourceFileZipPath":            sourceFileZipPath,
	})

	targetFolderScanner := longtailutils.AsyncFolderScanner{}
	targetFolderScanner.Scan(targetPath, pathFilter, fs)

	err := validateOneVersion(targetStore, targetFilePath, skipValidate)
	if err == nil {
		return CloneVersionIndex(currentVersionIndex), nil
	}

	fmt.Printf("`%s` -> `%s`\n", sourceFilePath, targetFilePath)

	vbuffer, err := longtailutils.ReadFromURI(sourceFilePath)
	if err != nil {
		err = errors.Wrapf(longtaillib.ErrENOENT, "File does not exist: %s", sourceFilePath)
		fileInfos, _, _ := targetFolderScanner.Get()
		fileInfos.Dispose()
		return CloneVersionIndex(currentVersionIndex), err
	}
	if vbuffer == nil {
		fileInfos, _, _ := targetFolderScanner.Get()
		fileInfos.Dispose()
		return CloneVersionIndex(currentVersionIndex), err
	}
	sourceVersionIndex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		fileInfos, _, _ := targetFolderScanner.Get()
		fileInfos.Dispose()
		return CloneVersionIndex(currentVersionIndex), err
	}

	hashIdentifier := sourceVersionIndex.GetHashIdentifier()
	targetChunkSize := sourceVersionIndex.GetTargetChunkSize()

	var hash longtaillib.Longtail_HashAPI
	var targetVersionIndex longtaillib.Longtail_VersionIndex

	if currentVersionIndex.IsValid() {
		hash, errno = hashRegistry.GetHashAPI(hashIdentifier)
		if errno != 0 {
			return CloneVersionIndex(currentVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: hashRegistry.GetHashAPI(%d) failed", hashIdentifier)
		}
		targetVersionIndex = CloneVersionIndex(currentVersionIndex)
	} else {
		targetIndexReader := longtailutils.AsyncVersionIndexReader{}
		targetIndexReader.Read(targetPath,
			"",
			targetChunkSize,
			longtailutils.NoCompressionType,
			hashIdentifier,
			pathFilter,
			fs,
			jobs,
			hashRegistry,
			&targetFolderScanner)

		targetVersionIndex, hash, _, err = targetIndexReader.Get()
		if err != nil {
			log.WithError(err).Errorf("Failed targetIndexReader.read")
			return CloneVersionIndex(currentVersionIndex), err
		}
	}
	defer targetVersionIndex.Dispose()

	versionDiff, errno := longtaillib.CreateVersionDiff(
		hash,
		targetVersionIndex,
		sourceVersionIndex)
	if errno != 0 {
		return CloneVersionIndex(currentVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.CreateVersionDiff() failed")
	}
	defer versionDiff.Dispose()

	chunkHashes, errno := longtaillib.GetRequiredChunkHashes(
		sourceVersionIndex,
		versionDiff)
	if errno != 0 {
		return CloneVersionIndex(currentVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.GetRequiredChunkHashes() failed")
	}

	existingStoreIndex, err := longtailutils.GetExistingStoreIndexSync(
		sourceStore,
		chunkHashes,
		0)
	if err != nil {
		err = errors.Wrapf(err, "Failed getting store index for diff")
		return CloneVersionIndex(currentVersionIndex), err
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
		longtailutils.NormalizePath(targetPath),
		retainPermissions)

	var newVersionIndex longtaillib.Longtail_VersionIndex

	if errno == 0 {
		newVersionIndex = CloneVersionIndex(sourceVersionIndex)
	} else {
		if sourceFileZipPath == "" {
			fmt.Printf("Skipping `%s` - unable to download from longtail: %s\n", sourceFilePath, longtaillib.ErrnoToError(errno, longtaillib.ErrEIO).Error())
			return longtaillib.Longtail_VersionIndex{}, err
		}
		fmt.Printf("Falling back to reading ZIP source from `%s`\n", sourceFileZipPath)
		zipBytes, err := longtailutils.ReadFromURI(sourceFileZipPath)
		if err != nil {
			log.WithError(err).Errorf("Failed longtailutils.ReadFromURI")
			return longtaillib.Longtail_VersionIndex{}, err
		}

		if zipBytes == nil {
			err = errors.Wrapf(longtaillib.ErrENOENT, "File does not exist: %s", sourceFileZipPath)
			return longtaillib.Longtail_VersionIndex{}, err
		}

		zipReader := bytes.NewReader(zipBytes)

		r, err := zip.NewReader(zipReader, int64(len(zipBytes)))
		if err != nil {
			log.WithError(err).Errorf("Failed zip.NewReader")
			return longtaillib.Longtail_VersionIndex{}, err
		}
		os.RemoveAll(targetPath)
		os.MkdirAll(targetPath, 0755)
		// Closure to address file descriptors issue with all the deferred .Close() methods
		extractAndWriteFile := func(f *zip.File) error {
			rc, err := f.Open()
			if err != nil {
				log.WithError(err).Errorf("Failed f.Open()")
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
					log.WithError(err).Errorf("Failed os.OpenFile")
					return err
				}
				defer func() {
					if err := f.Close(); err != nil {
						panic(err)
					}
				}()

				_, err = io.Copy(f, rc)
				if err != nil {
					log.WithError(err).Errorf("Failed io.Copy")
					return err
				}
			}
			return nil
		}

		for _, f := range r.File {
			err := extractAndWriteFile(f)
			if err != nil {
				log.WithError(err).Errorf("Failed extractAndWriteFile")
				return longtaillib.Longtail_VersionIndex{}, err
			}
		}

		fileInfos, errno := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			longtailutils.NormalizePath(targetPath))
		if errno != 0 {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.GetFilesRecursively() failed")
		}
		defer fileInfos.Dispose()

		compressionTypes := longtailutils.GetCompressionTypesForFiles(fileInfos, longtailutils.NoCompressionType)

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
			longtailutils.NormalizePath(targetPath),
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

	newExistingStoreIndex, err := longtailutils.GetExistingStoreIndexSync(
		targetStore,
		newVersionIndex.GetChunkHashes(),
		minBlockUsagePercent)
	if err != nil {
		err = errors.Wrapf(err, "Failed getting store index for new version")
		return CloneVersionIndex(sourceVersionIndex), err
	}
	defer newExistingStoreIndex.Dispose()

	versionMissingStoreIndex, errno := longtaillib.CreateMissingContent(
		hash,
		newExistingStoreIndex,
		newVersionIndex,
		targetBlockSize,
		maxChunksPerBlock)
	if errno != 0 {
		return CloneVersionIndex(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: CreateMissingContent() failed")
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
			longtailutils.NormalizePath(targetPath))
		writeContentProgress.Dispose()
		if errno != 0 {
			return CloneVersionIndex(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.WriteContent() failed")
		}
	}

	stores := []longtaillib.Longtail_BlockStoreAPI{
		targetRemoteStore,
		sourceRemoteIndexStore,
	}
	f, err := longtailutils.FlushStores(stores)
	if err != nil {
		log.WithError(err).Errorf("Failed longtailutils.FlushStores")
		return CloneVersionIndex(newVersionIndex), err
	}

	err = longtailutils.WriteToURI(targetFilePath, vbuffer)
	if err != nil {
		log.WithError(err).Errorf("longtailutils longtailutils.WriteToURI")
		return CloneVersionIndex(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils: longtailutils.WriteToURI() failed")
	}

	if createVersionLocalStoreIndex {
		versionLocalStoreIndex, errno := longtaillib.MergeStoreIndex(newExistingStoreIndex, versionMissingStoreIndex)
		if errno != 0 {
			return CloneVersionIndex(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.MergeStoreIndex() failed")
		}
		versionLocalStoreIndexBuffer, errno := longtaillib.WriteStoreIndexToBuffer(versionLocalStoreIndex)
		versionLocalStoreIndex.Dispose()
		if errno != 0 {
			return CloneVersionIndex(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "cloneStore: longtaillib.WriteStoreIndexToBuffer() failed")
		}
		versionLocalStoreIndexPath := strings.Replace(targetFilePath, ".lvi", ".lsi", -1) // TODO: This should use a file with path names instead of this rename hack!
		err = longtailutils.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer)
		if err != nil {
			log.WithError(err).Errorf("longtailutils longtailutils.WriteToURI")
			return CloneVersionIndex(newVersionIndex), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtailutils: longtailutils.WriteToURI() failed")
		}
	}

	err = f.Wait()
	if err != nil {
		log.WithError(err).Errorf("Failed Wait for longtailutils.FlushStores")
		return CloneVersionIndex(newVersionIndex), err
	}

	return CloneVersionIndex(newVersionIndex), nil
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
	log := logrus.WithFields(logrus.Fields{
		"numWorkerCount":               numWorkerCount,
		"sourceStoreURI":               sourceStoreURI,
		"targetStoreURI":               targetStoreURI,
		"localCachePath":               localCachePath,
		"targetPath":                   targetPath,
		"sourcePaths":                  sourcePaths,
		"sourceZipPaths":               sourceZipPaths,
		"targetPaths":                  targetPaths,
		"targetBlockSize":              targetBlockSize,
		"maxChunksPerBlock":            maxChunksPerBlock,
		"retainPermissions":            retainPermissions,
		"createVersionLocalStoreIndex": createVersionLocalStoreIndex,
		"hashing":                      hashing,
		"compression":                  compression,
		"minBlockUsagePercent":         minBlockUsagePercent,
		"skipValidate":                 skipValidate,
	})
	log.Debug("clone-store")

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

	sourceRemoteIndexStore, err := remotestore.CreateBlockStoreForURI(sourceStoreURI, "", jobs, numWorkerCount, 8388608, 1024, remotestore.ReadOnly)
	if err != nil {
		log.WithError(err).Errorf("Failed remotestore.CreateBlockStoreForURI")
		return storeStats, timeStats, err
	}
	defer sourceRemoteIndexStore.Dispose()
	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI
	var sourceCompressBlockStore longtaillib.Longtail_BlockStoreAPI

	if len(localCachePath) > 0 {
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, longtailutils.NormalizePath(localCachePath))

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

	targetRemoteStore, err := remotestore.CreateBlockStoreForURI(targetStoreURI, "", jobs, numWorkerCount, targetBlockSize, maxChunksPerBlock, remotestore.ReadWrite)
	if err != nil {
		log.WithError(err).Errorf("Failed remotestore.CreateBlockStoreForURI")
		return storeStats, timeStats, err
	}
	defer targetRemoteStore.Dispose()
	targetStore := longtaillib.CreateCompressBlockStore(targetRemoteStore, creg)
	defer targetStore.Dispose()

	sourcesFile, err := os.Open(sourcePaths)
	if err != nil {
		log.WithError(err).Errorf("Failed os.Open")
		log.Fatal(err)
	}
	defer sourcesFile.Close()

	var sourcesZipScanner *bufio.Scanner
	if sourceZipPaths != "" {
		sourcesZipFile, err := os.Open(sourceZipPaths)
		if err != nil {
			log.WithError(err).Errorf("Failed os.Open")
			log.Fatal(err)
		}
		sourcesZipScanner = bufio.NewScanner(sourcesZipFile)
		defer sourcesZipFile.Close()
	}

	targetsFile, err := os.Open(targetPaths)
	if err != nil {
		log.WithError(err).Errorf("Failed os.Open")
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
			log.WithError(err).Errorf("Failed cloneOneVersion")
			return storeStats, timeStats, err
		}
	}

	if err := sourcesScanner.Err(); err != nil {
		log.WithError(err).Errorf("Failed sourcesScanner.Err()")
		log.Fatal(err)
	}
	if err := sourcesZipScanner.Err(); err != nil {
		log.WithError(err).Errorf("Failed sourcesZipScanner.Err()")
		log.Fatal(err)
	}
	if err := targetsScanner.Err(); err != nil {
		log.WithError(err).Errorf("Failed targetsScanner.Err()")
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
