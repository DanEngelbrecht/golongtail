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
	forceWrite bool,
	blob []byte) (int, error) {

	retryCount := 0
	objHandle, err := client.NewObject(key)
	if err != nil {
		return retryCount, err
	}
	write := forceWrite
	if !forceWrite {
		exists, err := objHandle.Exists()
		if err != nil {
			return retryCount, err
		}
		write = !exists
	}
	if !write {
		return 0, nil
	}

	_, err = objHandle.Write(blob)
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
	storeIndexName := fmt.Sprintf("index/%x.lsi", storeIndexSha)
	_, err := writeBlobWithRetry(ctx, client, storeIndexName, false, storeIndexBlob)
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

func copyStoreIndex(storeIndex longtaillib.Longtail_StoreIndex) (longtaillib.Longtail_StoreIndex, error) {
	storeIndexBlob, errno := longtaillib.WriteStoreIndexToBuffer(storeIndex)
	if errno != 0 {
		return longtaillib.Longtail_StoreIndex{}, longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
	}
	copyIndex, errno := longtaillib.ReadStoreIndexFromBuffer(storeIndexBlob)
	if errno != 0 {
		return longtaillib.Longtail_StoreIndex{}, longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
	}
	return copyIndex, nil
}

func getNewPartialIndexNames(
	ctx context.Context,
	client BlobClient,
	skipName string) ([]string, bool, error) {
	exists := false
	partialIndexBlobs, err := client.GetObjects("index/")
	if err != nil {
		return []string{}, exists, err
	}
	newIndexNames := []string{}
	for _, blob := range partialIndexBlobs {
		if blob.Name == skipName {
			exists = true
			continue
		}
		newIndexNames = append(newIndexNames, blob.Name)
	}
	return newIndexNames, exists, nil
}

func writeStoreIndex(
	ctx context.Context,
	client BlobClient,
	addedStoreIndex longtaillib.Longtail_StoreIndex) error {

	consolidatedStoreIndex, err := copyStoreIndex(addedStoreIndex)
	if err != nil {
		log.Printf("writeStoreIndex: longtaillib.MergeStoreIndex failed with %v\n", err)
		return err
	}

	consolidatedStoreIndexName, err := writePartialStoreIndex(ctx, client, consolidatedStoreIndex)
	if err != nil {
		log.Printf("writeStoreIndex: writePartialStoreIndex failed with %v\n", err)
		return err
	}

	for {
		newIndexNames, _, err := getNewPartialIndexNames(ctx, client, consolidatedStoreIndexName)
		if err != nil {
			consolidatedStoreIndex.Dispose()
			log.Printf("writeStoreIndex: getNewPartialIndexNames failed with %v\n", err)
			return err
		}

		consolidatedNames := []string{}
		for _, name := range newIndexNames {
			partialStoreIndex, err := readStoreIndexBlob(ctx, client, name)
			if err != nil {
				continue
			}
			mergedStoreIndex, errno := longtaillib.MergeStoreIndex(consolidatedStoreIndex, partialStoreIndex)
			partialStoreIndex.Dispose()
			if errno != 0 {
				consolidatedStoreIndex.Dispose()
				log.Printf("writeStoreIndex: longtaillib.MergeStoreIndex failed with %v\n", longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM))
				return longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
			}
			consolidatedStoreIndex.Dispose()
			consolidatedStoreIndex = mergedStoreIndex
			consolidatedNames = append(consolidatedNames, name)
		}

		if len(consolidatedNames) == 0 {
			storeIndex, err := readStoreIndex(ctx, client)
			if err != nil {
				consolidatedStoreIndex.Dispose()
				log.Printf("writeStoreIndex: readStoreIndex failed with %v\n", err)
				return err
			}

			newStoreIndex, errno := longtaillib.MergeStoreIndex(storeIndex, consolidatedStoreIndex)
			storeIndex.Dispose()
			if errno != 0 {
				consolidatedStoreIndex.Dispose()
				log.Printf("writeStoreIndex: longtaillib.MergeStoreIndex failed with %v\n", longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM))
				return longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
			}

			storeIndexBlob, errno := longtaillib.WriteStoreIndexToBuffer(newStoreIndex)
			newStoreIndex.Dispose()
			if errno != 0 {
				consolidatedStoreIndex.Dispose()
				log.Printf("writeStoreIndex: WriteStoreIndexToBuffer failed with %v\n", longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM))
				return longtaillib.ErrnoToError(errno, longtaillib.ErrENOMEM)
			}

			_, err = writeBlobWithRetry(ctx, client, "store.lsi", true, storeIndexBlob)

			consolidatedStoreIndex.Dispose()

			return nil
		}

		tmpName, err := writePartialStoreIndex(ctx, client, consolidatedStoreIndex)
		if err != nil {
			consolidatedStoreIndex.Dispose()
			log.Printf("writeStoreIndex: writePartialStoreIndex failed with %v\n", err)
			return err
		}
		consolidatedNames = append(consolidatedNames, consolidatedStoreIndexName)
		consolidatedStoreIndexName = tmpName

		for _, name := range consolidatedNames {
			objHandle, err := client.NewObject(name)
			if err != nil {
				consolidatedStoreIndex.Dispose()
				log.Printf("writeStoreIndex: client.NewObject with %v\n", err)
				return err
			}
			_ = objHandle.Delete()
		}
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
