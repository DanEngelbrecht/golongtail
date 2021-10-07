package main

import (
	"sync"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
)

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
	sourceIndexPath string,
	targetChunkSize uint32,
	compressionType uint32,
	hashIdentifier uint32,
	pathFilter longtaillib.Longtail_PathFilterAPI,
	fs longtaillib.Longtail_StorageAPI,
	jobs longtaillib.Longtail_JobAPI,
	hashRegistry longtaillib.Longtail_HashRegistryAPI,
	scanner *asyncFolderScanner) (longtaillib.Longtail_VersionIndex, longtaillib.Longtail_HashAPI, time.Duration, error) {
	if sourceIndexPath == "" {
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

		createVersionIndexProgress := longtailutils.CreateProgress("Indexing version")
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

	vbuffer, err := longtailstorelib.ReadFromURI(sourceIndexPath)
	if err != nil {
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

type asyncVersionIndexReader struct {
	wg           sync.WaitGroup
	versionIndex longtaillib.Longtail_VersionIndex
	hashAPI      longtaillib.Longtail_HashAPI
	elapsedTime  time.Duration
	err          error
}

func (indexReader *asyncVersionIndexReader) read(
	sourceFolderPath string,
	sourceIndexPath string,
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
