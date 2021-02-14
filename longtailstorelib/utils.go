package longtailstorelib

import (
	"context"
	"crypto/sha1"
	"fmt"
	"log"
	"runtime"
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
	client BlobClient) (longtaillib.Longtail_StoreIndex, error) {

	var storeIndex longtaillib.Longtail_StoreIndex
	storeIndexBuffer, _, err := readBlobWithRetry(ctx, client, "store.lsi")
	if err != nil {
		if !errors.Is(err, longtaillib.ErrENOENT) {
			return longtaillib.Longtail_StoreIndex{}, err
		}
		errno := 0
		storeIndex, errno = longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
		if errno != 0 {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "readStoreStoreIndex: longtaillib.CreateStoreIndexFromBlocks() failed")
		}
	} else {
		errno := 0
		storeIndex, errno = longtaillib.ReadStoreIndexFromBuffer(storeIndexBuffer)
		if errno != 0 {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "readStoreStoreIndex: longtaillib.CreateStoreIndexFromBlocks() failed")
		}
	}
	return storeIndex, nil
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

func writePartialStoreIndex(
	ctx context.Context,
	client BlobClient,
	storeIndex longtaillib.Longtail_StoreIndex) (string, error) {
	storeIndexBlob, errno := longtaillib.WriteStoreIndexToBuffer(storeIndex)
	if errno != 0 {
		return "", longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
	}
	storeIndexSha := sha1.Sum(storeIndexBlob)
	storeIndexName := fmt.Sprintf("index/%x.plsi", storeIndexSha)
	objHandle, err := client.NewObject(storeIndexName)
	_, err = objHandle.Write(storeIndexBlob)
	if err != nil {
		return "", err
	}
	return storeIndexName, nil
}

func readStoreIndexBlob(
	ctx context.Context,
	client BlobClient,
	key string) (longtaillib.Longtail_StoreIndex, error) {
	storeIndexBuffer, _, err := readBlobWithRetry(ctx, client, key)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, err
	}
	storeIndex, errno := longtaillib.ReadStoreIndexFromBuffer(storeIndexBuffer)
	if errno != 0 {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(longtaillib.ErrnoToError(errno, longtaillib.ErrEIO), "readStoreStoreIndex: longtaillib.CreateStoreIndexFromBlocks() failed")
	}
	return storeIndex, nil
}

func consolidateStoreIndex(
	ctx context.Context,
	client BlobClient,
	addedStoreIndex longtaillib.Longtail_StoreIndex) (longtaillib.Longtail_StoreIndex, error) {

	_, err := writePartialStoreIndex(ctx, client, addedStoreIndex)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, err
	}

	partialIndexBlobs, err := client.GetObjects("index/")
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, err
	}

	consolidatedStoreIndex, err := readStoreIndexBlob(ctx, client, "store.lsi")
	if err != nil {
		if !errors.Is(err, longtaillib.ErrENOENT) {
			return longtaillib.Longtail_StoreIndex{}, err
		}
		errno := 0
		consolidatedStoreIndex, errno = longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
		if errno != 0 {
			return longtaillib.Longtail_StoreIndex{}, longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
		}
	}

	for _, blob := range partialIndexBlobs {
		partialStoreIndex, err := readStoreIndexBlob(ctx, client, blob.Name)
		if err != nil {
			consolidatedStoreIndex.Dispose()
			// Somebody has changed stuff behind our back
			return longtaillib.Longtail_StoreIndex{}, longtaillib.ErrEBUSY
		}
		mergedStoreIndex, errno := longtaillib.MergeStoreIndex(consolidatedStoreIndex, partialStoreIndex)
		if errno != 0 {
			consolidatedStoreIndex.Dispose()
			return longtaillib.Longtail_StoreIndex{}, longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
		}
		consolidatedStoreIndex.Dispose()
		consolidatedStoreIndex = mergedStoreIndex
	}

	consolidatedStoreIndexName, err := writePartialStoreIndex(ctx, client, consolidatedStoreIndex)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, err
	}
	for _, blob := range partialIndexBlobs {
		if blob.Name == consolidatedStoreIndexName {
			continue
		}
		objHandle, err := client.NewObject(blob.Name)
		if err != nil {
			consolidatedStoreIndex.Dispose()
			return longtaillib.Longtail_StoreIndex{}, err
		}
		err = objHandle.Delete()
		if err != nil {
			// Someone else has also picked it up, which is fine
			continue
		}
	}
	return consolidatedStoreIndex, nil
}

func addToStoreIndex(
	ctx context.Context,
	client BlobClient,
	storeIndex longtaillib.Longtail_StoreIndex) error {
	consolidatedStoreIndex, err := consolidateStoreIndex(ctx, client, storeIndex)
	if err != nil {
		return err
	}
	defer consolidatedStoreIndex.Dispose()

	storeIndexBlob, errno := longtaillib.WriteStoreIndexToBuffer(consolidatedStoreIndex)
	if errno != 0 {
		return longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
	}
	objHandle, err := client.NewObject("store.lsi")
	if err != nil {
		return err
	}

	_, err = objHandle.Write(storeIndexBlob)
	if err != nil {
		return err
	}

	return nil
}

func writeStoreIndex(
	ctx context.Context,
	blobClient BlobClient,
	addedStoreIndex longtaillib.Longtail_StoreIndex) error {
	for {
		err := addToStoreIndex(ctx, blobClient, addedStoreIndex)
		if !errors.Is(err, longtaillib.ErrEBUSY) {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
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
