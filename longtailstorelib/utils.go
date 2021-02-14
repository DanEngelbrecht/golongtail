package longtailstorelib

import (
	"context"
	"crypto/sha1"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/pkg/errors"
)

func readBlobWithRetry(
	ctx context.Context,
	client BlobClient,
	key string) ([]byte, int, error) {
	retryCount := 0
	objHandle, err := client.NewObject(key)
	if err != nil {
		return nil, retryCount, err
	}
	exists, err := objHandle.Exists()
	if err != nil {
		return nil, retryCount, err
	}
	if !exists {
		return nil, retryCount, longtaillib.ErrENOENT
	}
	blobData, err := objHandle.Read()
	if err != nil {
		log.Printf("Retrying getBlob %s using %s\n", key, client.String())
		retryCount++
		blobData, err = objHandle.Read()
	} else if blobData == nil {
		return nil, retryCount, longtaillib.ErrENOENT
	}
	if err != nil {
		log.Printf("Retrying 500 ms delayed getBlob %s using %s\n", key, client.String())
		time.Sleep(500 * time.Millisecond)
		retryCount++
		blobData, err = objHandle.Read()
	} else if blobData == nil {
		return nil, retryCount, longtaillib.ErrENOENT
	}
	if err != nil {
		log.Printf("Retrying 2 s delayed getBlob %s using %s\n", key, client.String())
		time.Sleep(2 * time.Second)
		retryCount++
		blobData, err = objHandle.Read()
	} else if blobData == nil {
		return nil, retryCount, longtaillib.ErrENOENT
	}

	if err != nil {
		return nil, retryCount, err
	}

	return blobData, retryCount, nil
}

func writeBlobWithRetry(
	ctx context.Context,
	client BlobClient,
	key string,
	blob []byte) (int, error) {

	retryCount := 0
	objHandle, err := client.NewObject(key)
	if err != nil {
		return retryCount, err
	}
	if exists, err := objHandle.Exists(); err == nil && !exists {
		_, err := objHandle.Write(blob)
		if err != nil {
			log.Printf("Retrying putBlob %s in store %s\n", key, client.String())
			retryCount++
			_, err = objHandle.Write(blob)
		}
		if err != nil {
			log.Printf("Retrying 500 ms delayed putBlob %s in store %s\n", key, client.String())
			time.Sleep(500 * time.Millisecond)
			retryCount++
			_, err = objHandle.Write(blob)
		}
		if err != nil {
			log.Printf("Retrying 2 s delayed putBlob %s in store %s\n", key, client.String())
			time.Sleep(2 * time.Second)
			retryCount++
			_, err = objHandle.Write(blob)
		}

		if err != nil {
			return retryCount, err
		}
	}
	return retryCount, nil
}

func readStoreIndex(
	ctx context.Context,
	client BlobClient) (longtaillib.Longtail_StoreIndex, []string, error) {

	//	var storeIndex longtaillib.Longtail_StoreIndex
	//	storeIndexBuffer, _, err := readBlobWithRetry(ctx, client, "store.lsi")
	//	if err != nil {
	//		if !errors.Is(err, longtaillib.ErrENOENT) {
	//			return longtaillib.Longtail_StoreIndex{}, nil, err
	//		}
	//		errno := 0
	//		storeIndex, errno = longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
	//		if errno != 0 {
	//			return longtaillib.Longtail_StoreIndex{}, nil, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "readStoreStoreIndex: longtaillib.CreateStoreIndexFromBlocks() failed")
	//		}
	//	} else {
	//		errno := 0
	//		storeIndex, errno = longtaillib.ReadStoreIndexFromBuffer(storeIndexBuffer)
	//		if errno != 0 {
	//			return longtaillib.Longtail_StoreIndex{}, nil, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "readStoreStoreIndex: longtaillib.CreateStoreIndexFromBlocks() failed")
	//		}
	//	}

	storeIndex, errno := longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
	if errno != 0 {
		return longtaillib.Longtail_StoreIndex{}, nil, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "readStoreStoreIndex: longtaillib.CreateStoreIndexFromBlocks() failed")
	}

	blobs, err := client.GetObjects("index/")
	if err != nil {
		storeIndex.Dispose()
		return longtaillib.Longtail_StoreIndex{}, nil, err
	}

	var indexNames []string
	for _, blob := range blobs {
		if blob.Size == 0 {
			continue
		}
		if strings.HasSuffix(blob.Name, ".plsi") {
			indexData, _, err := readBlobWithRetry(ctx, client, blob.Name)
			if err != nil {
				continue
			}
			partialStoreIndex, errno := longtaillib.ReadStoreIndexFromBuffer(indexData)
			if errno != 0 {
				continue
			}
			storeIndexNew, errno := longtaillib.MergeStoreIndex(storeIndex, partialStoreIndex)
			partialStoreIndex.Dispose()
			if errno != 0 {
				continue
			}
			storeIndex.Dispose()
			storeIndex = storeIndexNew
			indexNames = append(indexNames, blob.Name)
		}
	}
	return storeIndex, indexNames, nil
}

func updateStoreIndex(
	storeIndex longtaillib.Longtail_StoreIndex,
	addedBlockIndexes []longtaillib.Longtail_BlockIndex) (longtaillib.Longtail_StoreIndex, error) {
	addedStoreIndex, errno := longtaillib.CreateStoreIndexFromBlocks(addedBlockIndexes)
	if errno != 0 {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "contentIndexWorker: longtaillib.CreateStoreIndexFromBlocks() failed")
	}

	updatedStoreIndex, errno := longtaillib.MergeStoreIndex(addedStoreIndex, storeIndex)
	addedStoreIndex.Dispose()
	if errno != 0 {
		updatedStoreIndex.Dispose()
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM), "contentIndexWorker: longtaillib.MergeStoreIndex() failed")
	}
	return updatedStoreIndex, nil
}

func writeStoreIndex(
	ctx context.Context,
	blobClient BlobClient,
	addedStoreIndex longtaillib.Longtail_StoreIndex) error {

	//	addedStoreIndexBlob, errno := longtaillib.WriteStoreIndexToBuffer(addedStoreIndex)
	//	if errno != 0 {
	//		return longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
	//	}
	//	addedStoreIndexSha := sha1.Sum(addedStoreIndexBlob)
	//	addedStoreIndexName := fmt.Sprintf("index/%x.plsi", addedStoreIndexSha)
	//	objHandle, err := blobClient.NewObject(addedStoreIndexName)
	//	ok, err := objHandle.Write(addedStoreIndexBlob)
	//	if err != nil {
	//		return err
	//	}
	//	if !ok {
	//		// Return error here
	//		return nil
	//	}

	storeIndex, partialIndexNames, err := readStoreIndex(ctx, blobClient)
	defer storeIndex.Dispose()
	if err != nil {
		return err
	}

	newStoreIndex, errno := longtaillib.MergeStoreIndex(storeIndex, addedStoreIndex)
	if errno != 0 {
		return longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
	}
	defer newStoreIndex.Dispose()
	newStoreIndexBlob, errno := longtaillib.WriteStoreIndexToBuffer(newStoreIndex)
	if errno != 0 {
		return longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
	}

	newStoreIndexSha := sha1.Sum(newStoreIndexBlob)
	newStoreIndexName := fmt.Sprintf("index/%x.plsi", newStoreIndexSha)
	objHandle, err := blobClient.NewObject(newStoreIndexName)
	ok, err := objHandle.Write(newStoreIndexBlob)
	if err != nil {
		return err
	}
	if !ok {
		// Return error here
		return nil
	}

	//	objHandle, err = blobClient.NewObject("store.lsi")
	//	ok, err = objHandle.Write(storeIndexBlob)
	//	if err != nil {
	//		return err
	//	}
	//	if !ok {
	//		// Return error here
	//		return nil
	//	}

	// Loop hole here? Need to think hard if it is possible to mess things up...
	for _, indexName := range partialIndexNames {
		objHandle, err := blobClient.NewObject(indexName)
		if err == nil {
			objHandle.Delete()
		}
	}
	//objHandle, err = blobClient.NewObject(addedStoreIndexName)
	//if err == nil {
	//	objHandle.Delete()
	//}
	return nil
}

func getStoreIndexFromBlocks(
	ctx context.Context,
	s *remoteStore,
	blobClient BlobClient,
	blockKeys []string) (longtaillib.Longtail_StoreIndex, error) {

	storeIndex, errno := longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
	if errno != 0 {
		return longtaillib.Longtail_StoreIndex{}, longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
	}

	batchCount := runtime.NumCPU()
	batchStart := 0

	if batchCount > len(blockKeys) {
		batchCount = len(blockKeys)
	}
	clients := make([]BlobClient, batchCount)
	for c := 0; c < batchCount; c++ {
		client, err := s.blobStore.NewClient(ctx)
		if err != nil {
			storeIndex.Dispose()
			return longtaillib.Longtail_StoreIndex{}, err
		}
		clients[c] = client
	}

	var wg sync.WaitGroup

	for batchStart < len(blockKeys) {
		batchLength := batchCount
		if batchStart+batchLength > len(blockKeys) {
			batchLength = len(blockKeys) - batchStart
		}
		batchBlockIndexes := make([]longtaillib.Longtail_BlockIndex, batchLength)
		wg.Add(batchLength)
		for batchPos := 0; batchPos < batchLength; batchPos++ {
			i := batchStart + batchPos
			blockKey := blockKeys[i]
			go func(client BlobClient, batchPos int, blockKey string) {
				storedBlockData, _, err := readBlobWithRetry(
					ctx,
					client,
					blockKey)

				if err != nil {
					wg.Done()
					return
				}

				blockIndex, errno := longtaillib.ReadBlockIndexFromBuffer(storedBlockData)
				if errno != 0 {
					wg.Done()
					return
				}

				blockPath := GetBlockPath("chunks", blockIndex.GetBlockHash())
				if blockPath == blockKey {
					batchBlockIndexes[batchPos] = blockIndex
				} else {
					log.Printf("Block %s name does not match content hash, expected name %s\n", blockKey, blockPath)
				}

				wg.Done()
			}(clients[batchPos], batchPos, blockKey)
		}
		wg.Wait()
		writeIndex := 0
		for i, blockIndex := range batchBlockIndexes {
			if !blockIndex.IsValid() {
				continue
			}
			if i > writeIndex {
				batchBlockIndexes[writeIndex] = blockIndex
			}
			writeIndex++
		}
		batchBlockIndexes = batchBlockIndexes[:writeIndex]
		batchStoreIndex, errno := longtaillib.CreateStoreIndexFromBlocks(batchBlockIndexes)
		for _, blockIndex := range batchBlockIndexes {
			blockIndex.Dispose()
		}
		if errno != 0 {
			storeIndex.Dispose()
			return longtaillib.Longtail_StoreIndex{}, longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
		}
		newStoreIndex, errno := longtaillib.MergeStoreIndex(storeIndex, batchStoreIndex)
		batchStoreIndex.Dispose()
		storeIndex.Dispose()
		storeIndex = newStoreIndex
		batchStart += batchLength
		log.Printf("Scanned %d/%d blocks in %s\n", batchStart, len(blockKeys), blobClient.String())
	}

	for c := 0; c < batchCount; c++ {
		clients[c].Close()
	}

	return storeIndex, nil
}
