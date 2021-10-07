package longtailutils

import (
	"fmt"
	"sync"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type AsyncFolderScanner struct {
	wg        sync.WaitGroup
	fileInfos longtaillib.Longtail_FileInfos
	elapsed   time.Duration
	err       error
}

func (scanner *AsyncFolderScanner) Scan(
	sourceFolderPath string,
	pathFilter longtaillib.Longtail_PathFilterAPI,
	fs longtaillib.Longtail_StorageAPI) {

	scanner.wg.Add(1)
	go func() {
		startTime := time.Now()
		fileInfos, errno := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			NormalizePath(sourceFolderPath))
		if errno != 0 {
			scanner.err = errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtaillib.GetFilesRecursively(%s) failed", sourceFolderPath)
		}
		scanner.fileInfos = fileInfos
		scanner.elapsed = time.Since(startTime)
		scanner.wg.Done()
	}()
}

func (scanner *AsyncFolderScanner) Get() (longtaillib.Longtail_FileInfos, time.Duration, error) {
	scanner.wg.Wait()
	return scanner.fileInfos, scanner.elapsed, scanner.err
}

func GetFolderIndex(
	sourceFolderPath string,
	sourceIndexPath string,
	targetChunkSize uint32,
	compressionType uint32,
	hashIdentifier uint32,
	pathFilter longtaillib.Longtail_PathFilterAPI,
	fs longtaillib.Longtail_StorageAPI,
	jobs longtaillib.Longtail_JobAPI,
	hashRegistry longtaillib.Longtail_HashRegistryAPI,
	scanner *AsyncFolderScanner) (longtaillib.Longtail_VersionIndex, longtaillib.Longtail_HashAPI, time.Duration, error) {
	log := logrus.WithFields(logrus.Fields{
		"sourceFolderPath": sourceFolderPath,
		"sourceIndexPath":  sourceIndexPath,
		"targetChunkSize":  targetChunkSize,
		"compressionType":  compressionType,
		"hashIdentifier":   hashIdentifier,
	})
	if sourceIndexPath == "" {
		log.Debug("Using scanner result")
		fileInfos, scanTime, err := scanner.Get()
		if err != nil {
			err = errors.Wrap(err, "Failed getting scanner result")
			return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, scanTime, err
		}
		defer fileInfos.Dispose()

		startTime := time.Now()

		compressionTypes := GetCompressionTypesForFiles(fileInfos, compressionType)

		hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
		if errno != 0 {
			err = MakeError(errno, fmt.Sprintf("Unsupported hash identifier: %d", hashIdentifier))
			return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, scanTime + time.Since(startTime), err
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
			NormalizePath(sourceFolderPath),
			fileInfos,
			compressionTypes,
			targetChunkSize)
		if errno != 0 {
			return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, scanTime + time.Since(startTime), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtaillib.CreateVersionIndex(%s)", sourceFolderPath)
		}

		return vindex, hash, scanTime + time.Since(startTime), nil
	}
	startTime := time.Now()

	vbuffer, err := longtailstorelib.ReadFromURI(sourceIndexPath)
	if err != nil {
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, time.Since(startTime), err
	}
	if vbuffer == nil {
		err = fmt.Errorf("Version index does not exist: %s", sourceIndexPath)
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, time.Since(startTime), err
	}
	var errno int
	vindex, errno := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if errno != 0 {
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, time.Since(startTime), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "longtaillib.ReadVersionIndexFromBuffer(%s) failed", sourceIndexPath)
	}

	hash, errno := hashRegistry.GetHashAPI(hashIdentifier)
	if errno != 0 {
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, time.Since(startTime), errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "hashRegistry.GetHashAPI(%d) failed", hashIdentifier)
	}

	return vindex, hash, time.Since(startTime), nil
}

type AsyncVersionIndexReader struct {
	wg           sync.WaitGroup
	versionIndex longtaillib.Longtail_VersionIndex
	hashAPI      longtaillib.Longtail_HashAPI
	elapsedTime  time.Duration
	err          error
}

func (indexReader *AsyncVersionIndexReader) Read(
	sourceFolderPath string,
	sourceIndexPath string,
	targetChunkSize uint32,
	compressionType uint32,
	hashIdentifier uint32,
	pathFilter longtaillib.Longtail_PathFilterAPI,
	fs longtaillib.Longtail_StorageAPI,
	jobs longtaillib.Longtail_JobAPI,
	hashRegistry longtaillib.Longtail_HashRegistryAPI,
	scanner *AsyncFolderScanner) {
	indexReader.wg.Add(1)
	go func() {
		indexReader.versionIndex, indexReader.hashAPI, indexReader.elapsedTime, indexReader.err = GetFolderIndex(
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

func (indexReader *AsyncVersionIndexReader) Get() (longtaillib.Longtail_VersionIndex, longtaillib.Longtail_HashAPI, time.Duration, error) {
	indexReader.wg.Wait()
	return indexReader.versionIndex, indexReader.hashAPI, indexReader.elapsedTime, indexReader.err
}
