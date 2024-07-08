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
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/DanEngelbrecht/golongtail/remotestore"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func validateOneVersion(
	targetStore longtaillib.Longtail_BlockStoreAPI,
	targetEndpointResolverURI string,
	targetFilePath string,
	skipValidate bool) error {
	const fname = "validateOneVersion"
	log := logrus.WithFields(logrus.Fields{
		"fname":                     fname,
		"targetFilePath":            targetFilePath,
		"targetEndpointResolverURI": targetEndpointResolverURI,
		"skipValidate":              skipValidate,
	})
	log.Info(fname)
	tbuffer, err := longtailutils.ReadFromURI(targetFilePath, longtailutils.WithS3EndpointResolverURI(targetEndpointResolverURI))
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
	defer vbuffer.Dispose()
	copy, err := longtaillib.ReadVersionIndexFromBuffer(vbuffer.ToBuffer())
	if err != nil {
		err := errors.Wrap(err, "longtaillib.ReadVersionIndexFromBuffer() failed")
		log.WithError(err).Info(fname)
		return longtaillib.Longtail_VersionIndex{}
	}
	return copy
}

func downloadFromZip(targetPath string, sourceFileZipPath string, sourceEndpointResolverURI string) error {
	const fname = "downloadFromZip"
	log := logrus.WithFields(logrus.Fields{
		"targetPath":                targetPath,
		"sourceFileZipPath":         sourceFileZipPath,
		"sourceEndpointResolverURI": sourceEndpointResolverURI,
	})
	if sourceFileZipPath == "" {
		err := fmt.Errorf("skipping, no zip file available for `%s`", sourceFileZipPath)
		return errors.Wrap(err, fname)
	}
	log.Infof("falling back to reading ZIP source from `%s`", sourceFileZipPath)
	zipBytes, err := longtailutils.ReadFromURI(sourceFileZipPath, longtailutils.WithS3EndpointResolverURI(sourceEndpointResolverURI))
	if err != nil {
		return errors.Wrap(err, fname)
	}

	zipReader := bytes.NewReader(zipBytes)

	r, err := zip.NewReader(zipReader, int64(len(zipBytes)))
	if err != nil {
		return errors.Wrap(err, fname)
	}
	err = os.RemoveAll(targetPath)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	err = os.MkdirAll(targetPath, 0755)
	if err != nil {
		return errors.Wrap(err, fname)
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
				err = errors.Wrap(err, fname)
				log.WithError(err).Errorf("failed to close zip file")
			}
		}()

		path := filepath.Join(targetPath, f.Name)
		log.Debugf("Unzipping `%s`", path)

		// Check for ZipSlip (Directory traversal)
		if !strings.HasPrefix(path, filepath.Clean(targetPath)+string(os.PathSeparator)) {
			err := fmt.Errorf("illegal file path: `%s`", path)
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
					err = errors.Wrap(err, fname)
					log.WithError(err).Errorf("failed to close target file")
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
			return errors.Wrap(err, fname)
		}
	}
	return nil
}

func updateCurrentVersionFromLongtail(
	targetPath string,
	targetEndpointResolverURI string,
	targetPathVersionIndex longtaillib.Longtail_VersionIndex,
	jobs longtaillib.Longtail_JobAPI,
	hashRegistry longtaillib.Longtail_HashRegistryAPI,
	fs longtaillib.Longtail_StorageAPI,
	pathFilter longtaillib.Longtail_PathFilterAPI,
	retainPermissions bool,
	sourceStore longtaillib.Longtail_BlockStoreAPI,
	sourceEndpointResolverURI string,
	enableFileMapping bool,
	sourceFilePath string,
	sourceFileZipPath string,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	useLegacyWrite bool) (longtaillib.Longtail_VersionIndex, longtaillib.Longtail_HashAPI, error) {
	const fname = "updateCurrentVersionFromLongtail"

	var hash longtaillib.Longtail_HashAPI

	vbuffer, err := longtailutils.ReadFromURI(sourceFilePath, longtailutils.WithS3EndpointResolverURI(sourceEndpointResolverURI))
	if err != nil {
		err := errors.Wrap(err, "longtailutils.ReadFromURI() failed")
		return cloneVersionIndex(targetPathVersionIndex), hash, errors.Wrap(err, fname)
	}

	sourceVersionIndex, err := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if err != nil {
		err := errors.Wrap(err, "longtaillib.ReadVersionIndexFromBuffer() failed")
		return cloneVersionIndex(targetPathVersionIndex), hash, errors.Wrap(err, fname)
	}

	hashIdentifier := sourceVersionIndex.GetHashIdentifier()
	targetChunkSize := sourceVersionIndex.GetTargetChunkSize()

	localVersionIndex := longtaillib.Longtail_VersionIndex{}

	if targetPathVersionIndex.IsValid() {
		localVersionIndex = cloneVersionIndex(targetPathVersionIndex)
		hash, err = hashRegistry.GetHashAPI(hashIdentifier)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("unsupported hash identifier `%d`", hashIdentifier))
			return localVersionIndex, longtaillib.Longtail_HashAPI{}, errors.Wrap(err, fname)
		}
	} else {
		targetFolderScanner := longtailutils.AsyncFolderScanner{}
		targetFolderScanner.Scan(targetPath, pathFilter, fs, jobs)

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
			enableFileMapping,
			&targetFolderScanner)

		localVersionIndex, hash, _, err = targetIndexReader.Get()
		if err != nil {
			err := errors.Wrap(err, "failed scanning target path")
			return longtaillib.Longtail_VersionIndex{}, hash, errors.Wrap(err, fname)
		}
	}

	versionDiff, err := longtaillib.CreateVersionDiff(
		hash,
		localVersionIndex,
		sourceVersionIndex)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("failed to create version diff. `%s` -> `%s`", targetPath, sourceFilePath))
		return localVersionIndex, hash, errors.Wrap(err, fname)
	}
	defer versionDiff.Dispose()

	chunkHashes, err := longtaillib.GetRequiredChunkHashes(
		sourceVersionIndex,
		versionDiff)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("failed to get required chunk hashes. `%s` -> `%s`", targetPath, sourceFilePath))
		return localVersionIndex, hash, errors.Wrap(err, fname)
	}

	existingStoreIndex, err := longtailutils.GetExistingStoreIndexSync(
		sourceStore,
		chunkHashes,
		0)
	if err != nil {
		return localVersionIndex, hash, errors.Wrap(err, fname)
	}
	defer existingStoreIndex.Dispose()

	changeVersionProgress := longtailutils.CreateProgress("Updating version          ", 1)
	defer changeVersionProgress.Dispose()

	// Try to change local version
	if useLegacyWrite {
		err = longtaillib.ChangeVersion(
			sourceStore,
			fs,
			hash,
			jobs,
			&changeVersionProgress,
			existingStoreIndex,
			localVersionIndex,
			sourceVersionIndex,
			versionDiff,
			longtailstorelib.NormalizeFileSystemPath(targetPath),
			retainPermissions)
	} else {
		concurrentChunkWriteAPI := longtaillib.CreateConcurrentChunkWriteAPI(fs, sourceVersionIndex,
			versionDiff,
			longtailstorelib.NormalizeFileSystemPath(targetPath))
		defer concurrentChunkWriteAPI.Dispose()
		err = longtaillib.ChangeVersion2(
			sourceStore,
			fs,
			concurrentChunkWriteAPI,
			hash,
			jobs,
			&changeVersionProgress,
			existingStoreIndex,
			localVersionIndex,
			sourceVersionIndex,
			versionDiff,
			longtailstorelib.NormalizeFileSystemPath(targetPath),
			retainPermissions)
	}

	localVersionIndex.Dispose()
	if err == nil {
		return cloneVersionIndex(sourceVersionIndex), hash, nil
	}

	err = downloadFromZip(targetPath, sourceFileZipPath, sourceEndpointResolverURI)
	if err != nil {
		return longtaillib.Longtail_VersionIndex{}, hash, errors.Wrap(err, fname)
	}
	targetFolderScanner := longtailutils.AsyncFolderScanner{}
	targetFolderScanner.Scan(targetPath, pathFilter, fs, jobs)

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
		enableFileMapping,
		&targetFolderScanner)

	localVersionIndex, hash, _, err = targetIndexReader.Get()
	if err != nil {
		err := errors.Wrap(err, "failed scanning target path")
		return longtaillib.Longtail_VersionIndex{}, hash, errors.Wrap(err, fname)
	}
	return localVersionIndex, hash, nil
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
	sourceEndpointResolverURI string,
	targetStore longtaillib.Longtail_BlockStoreAPI,
	targetEndpointResolverURI string,
	sourceRemoteIndexStore longtaillib.Longtail_BlockStoreAPI,
	targetRemoteStore longtaillib.Longtail_BlockStoreAPI,
	targetFilePath string,
	sourceFilePath string,
	sourceFileZipPath string,
	currentVersionIndex longtaillib.Longtail_VersionIndex,
	enableFileMapping bool,
	useLegacyWrite bool) (longtaillib.Longtail_VersionIndex, error) {
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
		"sourceEndpointResolverURI":    sourceEndpointResolverURI,
		"targetEndpointResolverURI":    targetEndpointResolverURI,
		"targetFilePath":               targetFilePath,
		"sourceFilePath":               sourceFilePath,
		"sourceFileZipPath":            sourceFileZipPath,
		"enableFileMapping":            enableFileMapping,
		"useLegacyWrite":               useLegacyWrite,
	})
	log.Info(fname)

	err := validateOneVersion(targetStore, targetFilePath, targetEndpointResolverURI, skipValidate)
	if err == nil {
		return cloneVersionIndex(currentVersionIndex), nil
	}

	if !longtaillib.IsNotExist(err) {
		return cloneVersionIndex(currentVersionIndex), errors.Wrap(err, fname)
	}

	log.Infof("`%s` -> `%s`", sourceFilePath, targetFilePath)

	targetVersionIndex, hash, err := updateCurrentVersionFromLongtail(
		targetPath,
		targetEndpointResolverURI,
		currentVersionIndex,
		jobs,
		hashRegistry,
		fs,
		pathFilter,
		retainPermissions,
		sourceStore,
		sourceEndpointResolverURI,
		enableFileMapping,
		sourceFilePath,
		sourceFileZipPath,
		targetBlockSize,
		maxChunksPerBlock,
		useLegacyWrite)
	if err != nil {
		return targetVersionIndex, errors.Wrap(err, fname)
	}

	newExistingStoreIndex, err := longtailutils.GetExistingStoreIndexSync(
		targetStore,
		targetVersionIndex.GetChunkHashes(),
		minBlockUsagePercent)
	if err != nil {
		return targetVersionIndex, errors.Wrap(err, fname)
	}
	defer newExistingStoreIndex.Dispose()

	versionMissingStoreIndex, err := longtaillib.CreateMissingContent(
		hash,
		newExistingStoreIndex,
		targetVersionIndex,
		targetBlockSize,
		maxChunksPerBlock)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("failed creating missing content store index for `%s`", targetPath))
		return targetVersionIndex, errors.Wrap(err, fname)
	}
	defer versionMissingStoreIndex.Dispose()

	if versionMissingStoreIndex.GetBlockCount() > 0 {
		writeContentProgress := longtailutils.CreateProgress("Writing content blocks    ", 1)

		err = longtaillib.WriteContent(
			fs,
			targetStore,
			jobs,
			&writeContentProgress,
			versionMissingStoreIndex,
			targetVersionIndex,
			longtailstorelib.NormalizeFileSystemPath(targetPath))
		writeContentProgress.Dispose()
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("failed writing content from `%s`", targetPath))
			return targetVersionIndex, errors.Wrap(err, fname)
		}
	}

	stores := []longtaillib.Longtail_BlockStoreAPI{
		targetRemoteStore,
		sourceRemoteIndexStore,
	}
	f, err := longtailutils.FlushStores(stores)
	if err != nil {
		return targetVersionIndex, errors.Wrap(err, fname)
	}

	vbuffer, err := longtaillib.WriteVersionIndexToBuffer(targetVersionIndex)
	if err != nil {
		return targetVersionIndex, errors.Wrap(err, fname)
	}
	defer vbuffer.Dispose()

	err = longtailutils.WriteToURI(targetFilePath, vbuffer.ToBuffer(), longtailutils.WithS3EndpointResolverURI(targetEndpointResolverURI))
	if err != nil {
		return targetVersionIndex, errors.Wrap(err, fname)
	}

	if createVersionLocalStoreIndex {
		versionLocalStoreIndexPath := strings.Replace(targetFilePath, ".lvi", ".lsi", -1) // TODO: This should use a file with path names instead of this rename hack!
		versionLocalStoreIndex, err := longtaillib.MergeStoreIndex(newExistingStoreIndex, versionMissingStoreIndex)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("failed merging store index for `%s`", versionLocalStoreIndexPath))
			return targetVersionIndex, errors.Wrap(err, fname)
		}
		defer versionLocalStoreIndex.Dispose()
		versionLocalStoreIndexBuffer, err := longtaillib.WriteStoreIndexToBuffer(versionLocalStoreIndex)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("failed serializing store index for `%s`", versionLocalStoreIndexPath))
			return targetVersionIndex, errors.Wrap(err, fname)
		}
		defer versionLocalStoreIndexBuffer.Dispose()
		err = longtailutils.WriteToURI(versionLocalStoreIndexPath, versionLocalStoreIndexBuffer.ToBuffer(), longtailutils.WithS3EndpointResolverURI(targetEndpointResolverURI))
		if err != nil {
			return targetVersionIndex, errors.Wrap(err, fname)
		}
	}

	err = f.Wait()
	if err != nil {
		return targetVersionIndex, errors.Wrap(err, fname)
	}

	return targetVersionIndex, nil
}

func cloneStore(
	numWorkerCount int,
	remoteStoreWorkerCount int,
	sourceStoreURI string,
	sourceEndpointResolverURI string,
	targetStoreURI string,
	targetEndpointResolverURI string,
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
	skipValidate bool,
	enableFileMapping bool,
	useLegacyWrite bool) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "cloneStore"
	log := logrus.WithFields(logrus.Fields{
		"fname":                        fname,
		"numWorkerCount":               numWorkerCount,
		"remoteStoreWorkerCount":       remoteStoreWorkerCount,
		"sourceStoreURI":               sourceStoreURI,
		"sourceEndpointResolverURI":    sourceEndpointResolverURI,
		"targetStoreURI":               targetStoreURI,
		"targetEndpointResolverURI":    targetEndpointResolverURI,
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
		"useLegacyWrite":               useLegacyWrite,
	})
	log.Info(fname)

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

	sourceRemoteIndexStore, err := remotestore.CreateBlockStoreForURI(sourceStoreURI, nil, jobs, remoteStoreWorkerCount, 8388608, 1024, remotestore.ReadOnly, enableFileMapping, longtailutils.WithS3EndpointResolverURI(sourceEndpointResolverURI))
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer sourceRemoteIndexStore.Dispose()
	var localIndexStore longtaillib.Longtail_BlockStoreAPI
	var cacheBlockStore longtaillib.Longtail_BlockStoreAPI
	var sourceCompressBlockStore longtaillib.Longtail_BlockStoreAPI

	if len(localCachePath) > 0 {
		localIndexStore = longtaillib.CreateFSBlockStore(jobs, localFS, longtailstorelib.NormalizeFileSystemPath(localCachePath), "", enableFileMapping)

		cacheBlockStore = longtaillib.CreateCacheBlockStore(jobs, localIndexStore, sourceRemoteIndexStore)

		sourceCompressBlockStore = longtaillib.CreateCompressBlockStore(cacheBlockStore, creg)
	} else {
		sourceCompressBlockStore = longtaillib.CreateCompressBlockStore(sourceRemoteIndexStore, creg)
	}

	defer localIndexStore.Dispose()
	defer cacheBlockStore.Dispose()
	defer sourceCompressBlockStore.Dispose()

	var sourceStore longtaillib.Longtail_BlockStoreAPI
	var sourceLRUBlockStore longtaillib.Longtail_BlockStoreAPI
	if useLegacyWrite {
		sourceLRUBlockStore = longtaillib.CreateLRUBlockStoreAPI(sourceCompressBlockStore, 32)
		sourceStore = longtaillib.CreateShareBlockStore(sourceLRUBlockStore)
	} else {
		sourceStore = longtaillib.CreateShareBlockStore(sourceCompressBlockStore)
	}
	defer sourceLRUBlockStore.Dispose()
	defer sourceStore.Dispose()

	targetRemoteStore, err := remotestore.CreateBlockStoreForURI(targetStoreURI, nil, jobs, remoteStoreWorkerCount, targetBlockSize, maxChunksPerBlock, remotestore.ReadWrite, enableFileMapping, longtailutils.WithS3EndpointResolverURI(targetEndpointResolverURI))
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
			sourceEndpointResolverURI,
			targetStore,
			targetEndpointResolverURI,
			sourceRemoteIndexStore,
			targetRemoteStore,
			targetFilePath,
			sourceFilePath,
			sourceFileZipPath,
			currentVersionIndex,
			enableFileMapping,
			useLegacyWrite)
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
	SourceS3EndpointResolverURL  string `name:"source-s3-endpoint-resolver-uri" help"Optional URI for source S3 endpoint resolver"`
	TargetStorageURI             string `name:"target-storage-uri" help:"Target storage URI (local file system, GCS and S3 bucket URI supported)" required:""`
	TargetS3EndpointResolverURL  string `name:"target-s3-endpoint-resolver-uri" help"Optional URI for target S3 endpoint resolver"`
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
	EnableFileMappingOption
	UseLegacyWriteOption
}

func (r *CloneStoreCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := cloneStore(
		ctx.NumWorkerCount,
		ctx.NumRemoteWorkerCount,
		r.SourceStorageURI,
		r.SourceS3EndpointResolverURL,
		r.TargetStorageURI,
		r.TargetS3EndpointResolverURL,
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
		r.SkipValidate,
		r.EnableFileMapping,
		r.UseLegacyWrite)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
