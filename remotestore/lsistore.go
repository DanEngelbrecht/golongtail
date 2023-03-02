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
	"github.com/sirupsen/logrus"
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

func PutStoreLSI(ctx context.Context, remoteStore longtailstorelib.BlobStore, localStore *longtailstorelib.BlobStore, LSI longtaillib.Longtail_StoreIndex, maxStoreIndexSize int64) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "PutStoreLSI"
	log := logrus.WithFields(logrus.Fields{
		"fname":             fname,
		"ctx":               ctx,
		"remoteStore":       remoteStore,
		"LSI":               LSI,
		"maxStoreIndexSize": maxStoreIndexSize,
	})
	log.Debug(fname)

	LSIs, err := GetStoreLSIs(ctx, remoteStore, localStore)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	defer func() {
		for _, LSI := range LSIs {
			LSI.LSI.Dispose()
		}
	}()

	ConsolidatedLSI := longtaillib.Longtail_StoreIndex{}
	DisposeConsolidatedLSI := true
	defer func() {
		if DisposeConsolidatedLSI {
			ConsolidatedLSI.Dispose()
		}
	}()

	unmergedLSIs := []int{}
	mergedLSIs := []int{}
	if len(LSIs) > 0 {
		sort.Slice(LSIs, func(i, j int) bool { return LSIs[i].LSI.GetSize() < LSIs[j].LSI.GetSize() })
		for i := 0; i < len(LSIs); i++ {
			mergedSize := LSIs[i].LSI.GetSize() + LSI.GetSize()
			if mergedSize > maxStoreIndexSize {
				unmergedLSIs = append(unmergedLSIs, i)
				continue
			}

			if ConsolidatedLSI.IsValid() {
				MergedLSI, err := longtaillib.MergeStoreIndex(ConsolidatedLSI, LSIs[i].LSI)
				if err != nil {
					return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
				}
				ConsolidatedLSI.Dispose()
				ConsolidatedLSI = MergedLSI
			} else {
				ConsolidatedLSI, err = longtaillib.MergeStoreIndex(LSI, LSIs[i].LSI)
				if err != nil {
					return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
				}
			}
			LSIs[i].LSI.Dispose()
			mergedLSIs = append(mergedLSIs, i)
		}
	}

	var buffer longtaillib.NativeBuffer
	if ConsolidatedLSI.IsValid() {
		buffer, err = longtaillib.WriteStoreIndexToBuffer(ConsolidatedLSI)
	} else {
		buffer, err = longtaillib.WriteStoreIndexToBuffer(LSI)
	}
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	defer buffer.Dispose()

	saveBuffer := buffer.ToBuffer()

	exists := false
	sha256 := sha256.Sum256(saveBuffer)
	newName := fmt.Sprintf("store_%x.lsi", sha256)
	for _, remoteLSI := range LSIs {
		if remoteLSI.Name == newName {
			exists = true
			break
		}
	}

	remoteClient, err := remoteStore.NewClient(ctx)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	if !exists {
		_, err = longtailutils.WriteBlobWithRetry(ctx, remoteClient, newName, saveBuffer)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
	}

	for Index := range mergedLSIs {
		err = longtailutils.DeleteBlob(ctx, remoteClient, LSIs[Index].Name)
		if err != nil && !longtaillib.IsNotExist(err) {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
	}

	if len(unmergedLSIs) == 0 {
		if ConsolidatedLSI.IsValid() {
			DisposeConsolidatedLSI = false
			return ConsolidatedLSI, nil
		}
		result, err := LSI.Copy()
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		return result, nil
	}

	result := longtaillib.Longtail_StoreIndex{}
	if ConsolidatedLSI.IsValid() {
		result = ConsolidatedLSI
	} else {
		result = LSI
	}

	disposeResult := false
	defer func() {
		if disposeResult {
			result.Dispose()
		}
	}()

	for _, i := range unmergedLSIs {
		mergedLSI, err := longtaillib.MergeStoreIndex(result, LSIs[i].LSI)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		if disposeResult {
			result.Dispose()
		}
		result = mergedLSI
		LSIs[i].LSI.Dispose()
		disposeResult = true
	}

	disposeResult = false
	return result, nil
}

type LSIEntry struct {
	Name string
	LSI  longtaillib.Longtail_StoreIndex
}

func GetStoreLSIs(ctx context.Context, remoteStore longtailstorelib.BlobStore, localStore *longtailstorelib.BlobStore) ([]LSIEntry, error) {
	const fname = "GetStoreLSIs"
	log := logrus.WithFields(logrus.Fields{
		"fname":       fname,
		"ctx":         ctx,
		"remoteStore": remoteStore,
		"localStore":  localStore,
	})
	log.Debug(fname)

	remoteClient, err := remoteStore.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	var localClient longtailstorelib.BlobClient
	if localStore != nil {
		localClient, err = (*localStore).NewClient(ctx)
		if err != nil {
			return nil, errors.Wrap(err, fname)
		}
	}

	remoteLSIs, err := remoteClient.GetObjects("store")
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	sort.Slice(remoteLSIs, func(i, j int) bool { return remoteLSIs[i].Name < remoteLSIs[j].Name })

	var localLSIs []longtailstorelib.BlobProperties
	if localStore != nil {
		localLSIs, err = localClient.GetObjects("store")
		if err != nil {
			return nil, errors.Wrap(err, fname)
		}
		sort.Slice(localLSIs, func(i, j int) bool { return localLSIs[i].Name < localLSIs[j].Name })
	}

	remoteCount := len(remoteLSIs)
	localCount := len(localLSIs)
	localIndex := 0
	remoteIndex := 0
	newLSIs := []string{}
	success := false
	LSIs := []LSIEntry{}
	defer func() {
		if !success {
			for _, Entry := range LSIs {
				Entry.LSI.Dispose()
			}
		}
	}()

	for remoteIndex < remoteCount && localIndex < localCount {
		if remoteLSIs[remoteIndex].Name == localLSIs[localIndex].Name {
			buffer, _, err := longtailutils.ReadBlobWithRetry(
				ctx,
				localClient,
				localLSIs[localIndex].Name)
			if err != nil {
				return nil, errors.Wrap(err, fname)
			}
			LSI, err := longtaillib.ReadStoreIndexFromBuffer(buffer)
			if err != nil {
				return nil, errors.Wrap(err, fname)
			}
			LSIs = append(LSIs, LSIEntry{Name: localLSIs[localIndex].Name, LSI: LSI})
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
				return nil, errors.Wrap(err, fname)
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
			return nil, errors.Wrap(err, fname)
		}
		localIndex++
	}

	type Result struct {
		Entry LSIEntry
		Error error
	}
	storeIndexChan := make(chan Result, len(newLSIs))

	for _, newLSIName := range newLSIs {
		// TODO: We probably need to limit the number of goroutines we spawn ere as each one
		// wil do a network request
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
			resultChan <- Result{Entry: LSIEntry{Name: lsiName, LSI: LSI}}
		}(ctx, remoteStore, localStore, newLSIName, storeIndexChan)
	}

	for _ = range newLSIs {
		LSIResult := <-storeIndexChan
		if LSIResult.Error != nil {
			log.WithFields(logrus.Fields{
				"error": LSIResult.Error,
			}).Error("failed reading remote lsi")
			if err == nil {
				err = LSIResult.Error
			}
		}
		LSIs = append(LSIs, LSIResult.Entry)
	}
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	success = true
	return LSIs, nil
}

// If caller get an IsNotExist(err) it should likely call GetStoreLSI again as the list of store LSI has changed (one that we found was removed)
func GetStoreLSI(ctx context.Context, remoteStore longtailstorelib.BlobStore, localStore *longtailstorelib.BlobStore) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "GetStoreLSI"
	log := logrus.WithFields(logrus.Fields{
		"fname":       fname,
		"ctx":         ctx,
		"remoteStore": remoteStore,
		"localStore":  localStore,
	})
	log.Debug(fname)

	LSIs, err := GetStoreLSIs(ctx, remoteStore, localStore)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	defer func() {
		for _, LSI := range LSIs {
			LSI.LSI.Dispose()
		}
	}()

	if len(LSIs) > 0 {
		result := LSIs[0].LSI
		for i := 1; i < len(LSIs); i++ {
			newLSI, err := longtaillib.MergeStoreIndex(result, LSIs[i].LSI)
			if err != nil {
				return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			}
			result.Dispose()
			LSIs[i].LSI.Dispose()
			result = newLSI
		}
		LSIs[0].LSI = longtaillib.Longtail_StoreIndex{}
		return result, nil
	}
	return longtaillib.Longtail_StoreIndex{}, nil
}
