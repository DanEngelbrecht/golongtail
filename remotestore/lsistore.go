package remotestore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
)

// Get current index
// -------------------------
// List all .lsi files in store uri => A
// List all .lsi files in local cache => B
// For any .lsi files in B that is not A, remove from B
// For any .lsi files A that is not in B, download to B
// Merge all .lsi files in B

// Add blocks to store index(1)
// -------------------------
// Make store index of added blocks => A
// Write A to memory buffer B
// Calculate SHA256 of B and name <SHA256>.lsi => C
// Save C to local cache D
// Save C to store uri

// Add blocks to store index(2)
// -------------------------
// Make store index of added blocks => A
// List all .lsi files in store uri => D
// Find smallest .lsi under certain treshold E
// Download E
// Merge E and A => F
// Write F to memory buffer B
// Calculate SHA256 of F and name <SHA256>.lsi => G
// Save G to local cache D
// Save G to store uri
// Remove E from store uri

func mergeLSIs(localBuffer []byte, remoteBuffer []byte) (longtaillib.NativeBuffer, error) {
	const fname = "mergeLSIs"
	remoteLSI, err := longtaillib.ReadStoreIndexFromBuffer(remoteBuffer)
	if err != nil {
		return longtaillib.NativeBuffer{}, errors.Wrap(err, fname)
	}
	defer remoteLSI.Dispose()
	localLSI, err := longtaillib.ReadStoreIndexFromBuffer(localBuffer)
	if err != nil {
		return longtaillib.NativeBuffer{}, errors.Wrap(err, fname)
	}
	defer localLSI.Dispose()
	mergedLSI, err := longtaillib.MergeStoreIndex(remoteLSI, localLSI)
	if err != nil {
		return longtaillib.NativeBuffer{}, errors.Wrap(err, fname)
	}
	defer mergedLSI.Dispose()
	mergedBuffer, err := longtaillib.WriteStoreIndexToBuffer(mergedLSI)
	if err != nil {
		return longtaillib.NativeBuffer{}, errors.Wrap(err, fname)
	}
	return mergedBuffer, nil
}

func PutStoreLSI(ctx context.Context, remoteStore longtailstorelib.BlobStore, LSI longtaillib.Longtail_StoreIndex, maxStoreIndexSize int64) error {
	const fname = "PutStoreLSI"
	buffer, err := longtaillib.WriteStoreIndexToBuffer(LSI)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	defer buffer.Dispose()

	remoteClient, err := remoteStore.NewClient(ctx)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	remoteLSIs, err := remoteClient.GetObjects("store")
	if err != nil {
		return errors.Wrap(err, fname)
	}

	mergedNames := []string{}
	if len(remoteLSIs) > 0 {
		sort.Slice(remoteLSIs, func(i, j int) bool { return remoteLSIs[i].Size < remoteLSIs[j].Size })
		for i := 0; i < len(remoteLSIs); i++ {
			mergedSize := remoteLSIs[i].Size + buffer.Size()
			if mergedSize > maxStoreIndexSize {
				break
			}
			remoteBuffer, _, err := longtailutils.ReadBlobWithRetry(
				ctx,
				remoteClient,
				remoteLSIs[i].Name)
			if err != nil {
				return errors.Wrap(err, fname)
			}

			mergedLSI, err := mergeLSIs(buffer.ToBuffer(), remoteBuffer)
			if err != nil {
				return errors.Wrap(err, fname)
			}
			buffer.Dispose()
			buffer = mergedLSI

			mergedNames = append(mergedNames, remoteLSIs[i].Name)
		}
	}

	saveBuffer := buffer.ToBuffer()

	sha256 := sha256.Sum256(saveBuffer)
	newName := fmt.Sprintf("store_%x.lsi", sha256)
	for _, remoteLSI := range remoteLSIs {
		if remoteLSI.Name == newName {
			return nil
		}
	}

	_, err = longtailutils.WriteBlobWithRetry(ctx, remoteClient, newName, saveBuffer)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	for _, mergedName := range mergedNames {
		err = longtailutils.DeleteBlob(ctx, remoteClient, mergedName)
		if err != nil && !longtaillib.IsNotExist(err) {
			return errors.Wrap(err, fname)
		}
	}
	return nil
}

// If caller get an IsNotExist(err) it should likely call GetStoreLSI again as the list of store LSI has changed (one that we found was removed)
func GetStoreLSI(ctx context.Context, remoteStore longtailstorelib.BlobStore, localStore *longtailstorelib.BlobStore) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "GetStoreLSI"
	//	log.Debug(fname)
	remoteClient, err := remoteStore.NewClient(ctx)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	var localClient longtailstorelib.BlobClient
	if localStore != nil {
		localClient, err = (*localStore).NewClient(ctx)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
	}

	remoteLSIs, err := remoteClient.GetObjects("store")
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	sort.Slice(remoteLSIs, func(i, j int) bool { return remoteLSIs[i].Name < remoteLSIs[j].Name })

	var localLSIs []longtailstorelib.BlobProperties
	if localStore != nil {
		localLSIs, err = localClient.GetObjects("store")
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		sort.Slice(localLSIs, func(i, j int) bool { return localLSIs[i].Name < localLSIs[j].Name })
	}

	remoteCount := len(remoteLSIs)
	localCount := len(localLSIs)
	localIndex := 0
	remoteIndex := 0
	newLSIs := []string{}
	LSIs := []longtaillib.Longtail_StoreIndex{}
	defer func(LSIs []longtaillib.Longtail_StoreIndex) {
		for _, LSI := range LSIs {
			LSI.Dispose()
		}
	}(LSIs)

	for remoteIndex < remoteCount && localIndex < localCount {
		if remoteLSIs[remoteIndex].Name == localLSIs[localIndex].Name {
			buffer, _, err := longtailutils.ReadBlobWithRetry(
				ctx,
				localClient,
				localLSIs[localIndex].Name)
			if err != nil {
				return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			}
			LSI, err := longtaillib.ReadStoreIndexFromBuffer(buffer)
			if err != nil {
				return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			}
			LSIs = append(LSIs, LSI)
			remoteIndex++
			localIndex++
			continue
		}
		if remoteLSIs[remoteIndex].Name < localLSIs[localIndex].Name {
			newLSIs = append(newLSIs, remoteLSIs[remoteIndex].Name)
			remoteIndex++
			continue
		}
		if remoteLSIs[remoteIndex].Name > localLSIs[localIndex].Name {
			err = longtailutils.DeleteBlob(ctx, localClient, localLSIs[localIndex].Name)
			if err != nil {
				return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			}
			localIndex++
			continue
		}
	}
	for remoteIndex < remoteCount {
		newLSIs = append(newLSIs, remoteLSIs[remoteIndex].Name)
		remoteIndex++
	}
	for localIndex < localCount {
		err = longtailutils.DeleteBlob(ctx, localClient, localLSIs[localIndex].Name)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		localIndex++
	}

	type Result struct {
		LSI   longtaillib.Longtail_StoreIndex
		Error error
	}
	storeIndexChan := make(chan Result, len(newLSIs))

	for _, newLSIName := range newLSIs {
		go func(ctx context.Context, remoteStore longtailstorelib.BlobStore, localStore *longtailstorelib.BlobStore, lsiName string, resultChan chan Result) {
			remoteClient, err := remoteStore.NewClient(ctx)
			if err != nil {
				resultChan <- Result{Error: err}
				return
			}
			buffer, _, err := longtailutils.ReadBlobWithRetry(
				ctx,
				remoteClient,
				lsiName)
			if err != nil {
				resultChan <- Result{Error: err}
				return
			}

			LSI, err := longtaillib.ReadStoreIndexFromBuffer(buffer)
			if err != nil {
				resultChan <- Result{Error: err}
				return
			}

			if localStore != nil {
				localClient, err := (*localStore).NewClient(ctx)
				if err != nil {
					resultChan <- Result{Error: err}
					return
				}
				_, err = longtailutils.WriteBlobWithRetry(
					ctx,
					localClient,
					lsiName,
					buffer)
				if err != nil {
					resultChan <- Result{Error: err}
					return
				}
			}
			resultChan <- Result{LSI: LSI}
		}(ctx, remoteStore, localStore, newLSIName, storeIndexChan)
	}
	//	wg.Wait()

	for _ = range newLSIs {
		LSIResult := <-storeIndexChan
		if LSIResult.Error != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		LSIs = append(LSIs, LSIResult.LSI)
	}
	if len(LSIs) > 0 {
		result := LSIs[0]
		for i := 1; i < len(LSIs); i++ {
			newLSI, err := longtaillib.MergeStoreIndex(result, LSIs[i])
			if err != nil {
				return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			}
			result.Dispose()
			LSIs[i].Dispose()
			result = newLSI
		}
		LSIs[0] = longtaillib.Longtail_StoreIndex{}
		return result, nil
	}
	return longtaillib.Longtail_StoreIndex{}, nil
}
