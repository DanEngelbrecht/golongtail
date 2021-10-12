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
	const fname = "validateOneVersion"
	log := logrus.WithFields(logrus.Fields{
		"fname":          fname,
		"targetFilePath": targetFilePath,
		"skipValidate":   skipValidate,
	})
	tbuffer, err := longtailutils.ReadFromURI(targetFilePath)
	if err != nil {
		return errors.Wrap(err, fname)
	}

	if skipValidate {
		log.Infof("Skipping `%s`", targetFilePath)
		return nil
	}
	log.Infof("Validating `%s`", targetFilePath)
	targetVersionIndex, err := longtaillib.ReadVersionIndexFromBuffer(tbuffer)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Cant parse version index from `%s`", targetFilePath))
		return errors.Wrap(err, fname)
	}
	defer targetVersionIndex.Dispose()

	targetStoreIndex, err := longtailutils.GetExistingStoreIndexSync(
		targetStore,
		targetVersionIndex.GetChunkHashes(),
		0)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	defer targetStoreIndex.Dispose()

	err = longtaillib.ValidateStore(targetStoreIndex, targetVersionIndex)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Validate failed for version index `%s`", targetFilePath))
		return errors.Wrap(err, fname)
	}
	return nil
}

func cloneVersionIndex(v longtaillib.Longtail_VersionIndex) longtaillib.Longtail_VersionIndex {
	const fname = "cloneVersionIndex"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
	})
	if !v.IsValid() {
		return longtaillib.Longtail_VersionIndex{}
	}
	vbuffer, err := longtaillib.WriteVersionIndexToBuffer(v)
	if err != nil {
		err := errors.Wrap(err, "Failed serializing version index")
		log.WithError(err).Info(fname)
		return longtaillib.Longtail_VersionIndex{}
	}
	copy, err := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if err != nil {
		err := errors.Wrap(err, "longtaillib.ReadVersionIndexFromBuffer() failed")
		log.WithError(err).Info(fname)
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
	const fname = "cloneOneVersion"
	log := logrus.WithFields(logrus.Fields{
		"fname":                        fname,
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
		return cloneVersionIndex(currentVersionIndex), nil
	}

	if !errors.Is(err, os.ErrNotExist) {
		return cloneVersionIndex(currentVersionIndex), errors.Wrap(err, fname)
	}

	log.Infof("`%s` -> `%s`", sourceFilePath, targetFilePath)

	vbuffer, err := longtailutils.ReadFromURI(sourceFilePath)
	if err != nil {
		fileInfos, _, _ := targetFolderScanner.Get()
		fileInfos.Dispose()
		return cloneVersionIndex(currentVersionIndex), errors.Wrap(err, fname)
	}
	sourceVersionIndex, err := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if err != nil {
		err := errors.Wrap(err, "longtaillib.ReadVersionIndexFromBuffer() failed")
		fileInfos, _, _ := targetFolderScanner.Get()
		fileInfos.Dispose()
		return cloneVersionIndex(currentVersionIndex), errors.Wrap(err, fname)
	}

	hashIdentifier := sourceVersionIndex.GetHashIdentifier()
	targetChunkSize := sourceVersionIndex.GetTargetChunkSize()

	var hash longtaillib.Longtail_HashAPI
	var targetVersionIndex longtaillib.Longtail_VersionIndex

	if currentVersionIndex.IsValid() {
		hash, err = hashRegistry.GetHashAPI(hashIdentifier)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Unsupported hash identifier `%d`", hashIdentifier))
			return cloneVersionIndex(currentVersionIndex), errors.Wrap(err, fname)
		}
		targetVersionIndex = cloneVersionIndex(currentVersionIndex)
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
			return cloneVersionIndex(currentVersionIndex), err
		}
	}
	defer targetVersionIndex.Dispose()

	versionDiff, err := longtaillib.CreateVersionDiff(
		hash,
		targetVersionIndex,
		sourceVersionIndex)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Failed to create version diff. `%s` -> `%s`", targetFilePath, sourceFilePath))
		return cloneVersionIndex(currentVersionIndex), errors.Wrap(err, fname)
	}
	defer versionDiff.Dispose()

	chunkHashes, err := longtaillib.GetRequiredChunkHashes(
		sourceVersionIndex,
		versionDiff)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Failed to get required chunk hashes. `%s` -> `%s`", targetFilePath, sourceFilePath))
		return cloneVersionIndex(currentVersionIndex), errors.Wrap(err, fname)
	}

	existingStoreIndex, err := longtailutils.GetExistingStoreIndexSync(
		sourceStore,
		chunkHashes,
		0)
	if err != nil {
		return cloneVersionIndex(currentVersionIndex), errors.Wrap(err, fname)
	}
	defer existingStoreIndex.Dispose()

	changeVersionProgress := longtailutils.CreateProgress("Updating version")
	defer changeVersionProgress.Dispose()

	err = longtaillib.ChangeVersion(
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

	if err == nil {
		newVersionIndex = cloneVersionIndex(sourceVersionIndex)
	} else {
		if sourceFileZipPath == "" {
			err = errors.Wrap(err, fmt.Sprintf("Skipping, no zip file available for `%s`", sourceFilePath))
			return longtaillib.Longtail_VersionIndex{}, errors.Wrap(err, fname)
		}
		log.Infof("Falling back to reading ZIP source from `%s`", sourceFileZipPath)
		zipBytes, err := longtailutils.ReadFromURI(sourceFileZipPath)
		if err != nil {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrap(err, fname)
		}

		zipReader := bytes.NewReader(zipBytes)

		r, err := zip.NewReader(zipReader, int64(len(zipBytes)))
		if err != nil {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrap(err, fname)
		}
		err = os.RemoveAll(targetPath)
		if err != nil {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrap(err, fname)
		}
		err = os.MkdirAll(targetPath, 0755)
		if err != nil {
			return longtaillib.Longtail_VersionIndex{}, errors.Wrap(err, fname)
		}
		// Closure to address file descriptors issue with all the deferred .Close() methods
		extractAndWriteFile := func(f *zip.File) error {
			const fname = "extractAndWriteFile"
			log := logrus.WithFields(logrus.Fields{
				"fname": fname,
			})
			rc, err := f.Open()
			if err != nil {
				return errors.Wrap(err, fname)
			}
			defer func() {
				if err := rc.Close(); err != nil {
					log.WithError(err).Error(fname)
					panic(errors.Wrap(err, fname))
				}
			}()

			path := filepath.Join(targetPath, f.Name)
			log.Infof("Unzipping `%s`", path)

			// Check for ZipSlip (Directory traversal)
			if !strings.HasPrefix(path, filepath.Clean(targetPath)+string(os.PathSeparator)) {
				err := fmt.Errorf("Illegal file path: `%s`", path)
				return errors.Wrap(err, fname)
			}

			if f.FileInfo().IsDir() {
				err = os.MkdirAll(path, f.Mode())
				if err != nil {
					return errors.Wrap(err, fname)
				}
			} else {
				err = os.MkdirAll(filepath.Dir(path), 0777)
				if err != nil {
					return errors.Wrap(err, fname)
				}
				f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
				if err != nil {
					return errors.Wrap(err, fname)
				}
				defer func() {
					if err := f.Close(); err != nil {
						panic(err)
					}
				}()

				_, err = io.Copy(f, rc)
				if err != nil {
					return errors.Wrap(err, fname)
				}
			}
			return nil
		}

		for _, f := range r.File {
			err := extractAndWriteFile(f)
			if err != nil {
				return longtaillib.Longtail_VersionIndex{}, errors.Wrap(err, fname)
			}
		}

		fileInfos, err := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			longtailutils.NormalizePath(targetPath))
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Failed getting folder structure for `%s`", targetPath))
			return longtaillib.Longtail_VersionIndex{}, errors.Wrap(err, fname)
		}
		defer fileInfos.Dispose()

		compressionTypes := longtailutils.GetCompressionTypesForFiles(fileInfos, longtailutils.NoCompressionType)

		hash, err := hashRegistry.GetHashAPI(hashIdentifier)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Unsupported hash identifier `%d`", hashIdentifier))
			return longtaillib.Longtail_VersionIndex{}, errors.Wrap(err, fname)
		}

		chunker := longtaillib.CreateHPCDCChunkerAPI()
		defer chunker.Dispose()

		createVersionIndexProgress := longtailutils.CreateProgress("Indexing version")
		defer createVersionIndexProgress.Dispose()
		// Make sure to create an index of what we actually have on disk after update
		newVersionIndex, err = longtaillib.CreateVersionIndex(
			fs,
			hash,
			chunker,
			jobs,
			&createVersionIndexProgress,
			longtailutils.NormalizePath(targetPath),
			fileInfos,
			compressionTypes,
			targetChunkSize)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Failed creating version index for `%s`", targetPath))
			return longtaillib.Longtail_VersionIndex{}, errors.Wrap(err, fname)
		}

		chunkHashes = newVersionIndex.GetChunkHashes()

		// Make sure to update version binary for what we actually have on disk
		vbuffer, err = longtaillib.WriteVersionIndexToBuffer(newVersionIndex)
		if err != nil {
			newVersionIndex.Dispose()
			err = errors.Wrap(err, fmt.Sprintf("Failed serializing version index for `%s`", targetPath))
			return longtaillib.Longtail_VersionIndex{}, errors.Wrap(err, fname)
		}
	}
	defer newVersionIndex.Dispose()

	newExistingStoreIndex, err := longtailutils.GetExistingStoreIndexSync(
		targetStore,
		newVersionIndex.GetChunkHashes(),
		minBlockUsagePercent)
	if err != nil {
		return cloneVersionIndex(sourceVersionIndex), errors.Wrap(err, fname)
	}
	defer newExistingStoreIndex.Dispose()

	versionMissingStoreIndex, err := longtaillib.CreateMissingContent(
		hash,
		newExistingStoreIndex,
		newVersionIndex,
		targetBlockSize,
		maxChunksPerBlock)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Failed creating missing content store index for `%s`", targetPath))
		return cloneVersionIndex(newVersionIndex), errors.Wrap(err, fname)
	}
	defer versionMissingStoreIndex.Dispose()

	if versionMissingStoreIndex.GetBlockCount() > 0 {
		writeContentProgress := longtailutils.CreateProgress("Writing content blocks")

		err = longtaillib.WriteContent(
			fs,
			targetStore,
			jobs,
			&writeContentProgress,
			versionMissingStoreIndex,
			newVersionIndex,
			longtailutils.NormalizePath(targetPath))
		writeContentProgress.Dispose()
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Failed writing content from `%s`", targetPath))
			return cloneVersionIndex(newVersionIndex), errors.Wrap(err, fname)
		}
	}

	stores := []longtaillib.Longtail_BlockStoreAPI{
		targetRemoteStore,
		sourceRemoteIndexStore,
	}
	f, err := longtailutils.FlushStores(stores)
	if err != nil {
		return cloneVersionIndex(newVersionIndex), errors.Wrap(err, fname)
	}

	err = longtailutils.WriteToURI(targetFilePath, vbuffer)
	if err != nil {
		return cloneVersionIndex(newVersionIndex), errors.Wrap(err, fname)
	}

	if createVersionLocalStoreIndex {
		versionLocalStoreIndexPath := strings.Replace(targetFilePath, ".lvi", ".lsi", -1) // TODO: This should use a file with path names instead of this rename hack!
		versionLocalStoreIndex, err := longtaillib.MergeStoreIndex(newExistingStoreIndex, versionMissingStoreIndex)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Failed merging store index for `%s`", versionLocalStoreIndexPath))
			return cloneVersionIndex(newVersionIndex), errors.Wrap(err, fname)
		}
		versionLocalStoreIndexBuffer, err := longtaillib.WriteStoreIndexToBuffer(versionLocalStoreIndex)
		versionLocalStoreIndex.Dispose()
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Failed serializing store index for `%s`", versionLocalStoreIndexPath))
			return cloneVersionIndex(newVersionIndex), errors.Wrap(err, fname)
		}
		err = longtailutils.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer)
		if err != nil {
			return cloneVersionIndex(newVersionIndex), errors.Wrap(err, fname)
		}
	}

	err = f.Wait()
	if err != nil {
		return cloneVersionIndex(newVersionIndex), errors.Wrap(err, fname)
	}

	return cloneVersionIndex(newVersionIndex), nil
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
	const fname = "cloneStore"
	log := logrus.WithFields(logrus.Fields{
		"fname":                        fname,
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
	log.Debug(fname)

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
		return storeStats, timeStats, errors.Wrap(err, fname)
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
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer targetRemoteStore.Dispose()
	targetStore := longtaillib.CreateCompressBlockStore(targetRemoteStore, creg)
	defer targetStore.Dispose()

	sourcesFile, err := os.Open(sourcePaths)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer sourcesFile.Close()

	var sourcesZipScanner *bufio.Scanner
	if sourceZipPaths != "" {
		sourcesZipFile, err := os.Open(sourceZipPaths)
		if err != nil {
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
		sourcesZipScanner = bufio.NewScanner(sourcesZipFile)
		defer sourcesZipFile.Close()
	}

	targetsFile, err := os.Open(targetPaths)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
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
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
	}

	if err := sourcesScanner.Err(); err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	if sourcesZipScanner != nil {
		if err := sourcesZipScanner.Err(); err != nil {
			return storeStats, timeStats, errors.Wrap(err, fname)
		}
	}
	if err := targetsScanner.Err(); err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
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
