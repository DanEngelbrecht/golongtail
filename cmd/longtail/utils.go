package main

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
)

func normalizePath(path string) string {
	doubleForwardRemoved := strings.Replace(path, "//", "/", -1)
	doubleBackwardRemoved := strings.Replace(doubleForwardRemoved, "\\\\", "/", -1)
	backwardRemoved := strings.Replace(doubleBackwardRemoved, "\\", "/", -1)
	return backwardRemoved
}

func createBlockStoreForURI(
	uri string,
	optionalStoreIndexPath string,
	jobAPI longtaillib.Longtail_JobAPI,
	numWorkerCount int,
	targetBlockSize uint32,
	maxChunksPerBlock uint32,
	accessType longtailstorelib.AccessType) (longtaillib.Longtail_BlockStoreAPI, error) {
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
			return longtaillib.CreateFSBlockStore(jobAPI, longtaillib.CreateFSStorageAPI(), blobStoreURL.Path[1:]), nil
		}
	}
	return longtaillib.CreateFSBlockStore(jobAPI, longtaillib.CreateFSStorageAPI(), uri), nil
}

func getCompressionTypesForFiles(fileInfos longtaillib.Longtail_FileInfos, compressionType uint32) []uint32 {
	pathCount := fileInfos.GetFileCount()
	compressionTypes := make([]uint32, pathCount)
	for i := uint32(0); i < pathCount; i++ {
		compressionTypes[i] = compressionType
	}
	return compressionTypes
}

var (
	compressionTypeMap = map[string]uint32{
		"none":            noCompressionType,
		"brotli":          longtaillib.GetBrotliGenericDefaultCompressionType(),
		"brotli_min":      longtaillib.GetBrotliGenericMinCompressionType(),
		"brotli_max":      longtaillib.GetBrotliGenericMaxCompressionType(),
		"brotli_text":     longtaillib.GetBrotliTextDefaultCompressionType(),
		"brotli_text_min": longtaillib.GetBrotliTextMinCompressionType(),
		"brotli_text_max": longtaillib.GetBrotliTextMaxCompressionType(),
		"lz4":             longtaillib.GetLZ4DefaultCompressionType(),
		"zstd":            longtaillib.GetZStdDefaultCompressionType(),
		"zstd_min":        longtaillib.GetZStdMinCompressionType(),
		"zstd_max":        longtaillib.GetZStdMaxCompressionType(),
	}

	hashIdentifierMap = map[string]uint32{
		"meow":   longtaillib.GetMeowHashIdentifier(),
		"blake2": longtaillib.GetBlake2HashIdentifier(),
		"blake3": longtaillib.GetBlake3HashIdentifier(),
	}

	reverseHashIdentifierMap = map[uint32]string{
		longtaillib.GetMeowHashIdentifier():   "meow",
		longtaillib.GetBlake2HashIdentifier(): "blake2",
		longtaillib.GetBlake3HashIdentifier(): "blake3",
	}
)

const noCompressionType = uint32(0)

func getCompressionType(compressionAlgorithm string) (uint32, error) {
	if compressionType, exists := compressionTypeMap[compressionAlgorithm]; exists {
		return compressionType, nil
	}
	return 0, fmt.Errorf("unsupported compression algorithm: `%s`", compressionAlgorithm)
}

func getHashIdentifier(hashAlgorithm string) (uint32, error) {
	if identifier, exists := hashIdentifierMap[hashAlgorithm]; exists {
		return identifier, nil
	}
	return 0, fmt.Errorf("not a supported hash api: `%s`", hashAlgorithm)
}

func hashIdentifierToString(hashIdentifier uint32) string {
	if identifier, exists := reverseHashIdentifierMap[hashIdentifier]; exists {
		return identifier
	}
	return fmt.Sprintf("%d", hashIdentifier)
}
