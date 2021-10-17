package longtailutils

import (
	"fmt"
	"sync"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
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
	const fname = "AsyncFolderScanner.Scan"
	scanner.wg.Add(1)
	go func() {
		startTime := time.Now()
		fileInfos, err := longtaillib.GetFilesRecursively(
			fs,
			pathFilter,
			NormalizePath(sourceFolderPath))
		if err != nil {
			err := errors.Wrap(err, fmt.Sprintf("Failed getting folder structure for `%s`", sourceFolderPath))
			scanner.err = errors.Wrap(err, fname)
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
	const fname = "GetFolderIndex"
	log := logrus.WithFields(logrus.Fields{
		"fname":            fname,
		"sourceFolderPath": sourceFolderPath,
		"sourceIndexPath":  sourceIndexPath,
		"targetChunkSize":  targetChunkSize,
		"compressionType":  compressionType,
		"hashIdentifier":   hashIdentifier,
	})
	if sourceIndexPath == "" {
		log.Debug(fname)
		fileInfos, scanTime, err := scanner.Get()
		if err != nil {
			return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, scanTime, errors.Wrap(err, fname)
		}
		defer fileInfos.Dispose()

		startTime := time.Now()

		compressionTypes := GetCompressionTypesForFiles(fileInfos, compressionType)

		hash, err := hashRegistry.GetHashAPI(hashIdentifier)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Unsupported hash identifier `%d`", hashIdentifier))
			return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, scanTime + time.Since(startTime), errors.Wrap(err, fname)
		}

		chunker := longtaillib.CreateHPCDCChunkerAPI()
		defer chunker.Dispose()

		createVersionIndexProgress := CreateProgress("Indexing version", 2)
		defer createVersionIndexProgress.Dispose()
		vindex, err := longtaillib.CreateVersionIndex(
			fs,
			hash,
			chunker,
			jobs,
			&createVersionIndexProgress,
			NormalizePath(sourceFolderPath),
			fileInfos,
			compressionTypes,
			targetChunkSize)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Failed creating version index for `%s`", sourceFolderPath))
			return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, scanTime + time.Since(startTime), errors.Wrap(err, fname)
		}

		return vindex, hash, scanTime + time.Since(startTime), nil
	}
	startTime := time.Now()

	vbuffer, err := ReadFromURI(sourceIndexPath)
	if err != nil {
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, time.Since(startTime), errors.Wrap(err, fname)
	}
	vindex, err := longtaillib.ReadVersionIndexFromBuffer(vbuffer)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Cant parse version index from `%s`", sourceIndexPath))
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, time.Since(startTime), errors.Wrap(err, fname)
	}

	hash, err := hashRegistry.GetHashAPI(hashIdentifier)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Unsupported hash identifier `%d`", hashIdentifier))
		return longtaillib.Longtail_VersionIndex{}, longtaillib.Longtail_HashAPI{}, time.Since(startTime), errors.Wrap(err, fname)
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
