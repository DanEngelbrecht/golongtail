package remotestore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"time"

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

// Add blocks to store index
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

func OverwriteStoreLSI(ctx context.Context, remoteStore longtailstorelib.BlobStore, LSI longtaillib.Longtail_StoreIndex) error {
	const fname = "OverwriteStoreLSI"
	log := logrus.WithFields(logrus.Fields{
		"fname":       fname,
		"ctx":         ctx,
		"remoteStore": remoteStore,
		"LSI":         LSI,
	})
	log.Debug(fname)

	remoteClient, err := remoteStore.NewClient(ctx)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	defer remoteClient.Close()
	remoteLSIs, err := remoteClient.GetObjects("store", ".lsi")
	if err != nil && !longtaillib.IsNotExist(err) {
		return errors.Wrap(err, fname)
	}
	log.Debugf("found %d store indexes in remote store", len(remoteLSIs))
	buffer, err := longtaillib.WriteStoreIndexToBuffer(LSI)
	if err != nil && !longtaillib.IsNotExist(err) {
		return errors.Wrap(err, fname)
	}
	defer buffer.Dispose()

	saveBuffer := buffer.ToBuffer()

	exists := false
	sha256 := sha256.Sum256(saveBuffer)
	newName := fmt.Sprintf("store_%x.lsi", sha256)
	for _, remoteLSI := range remoteLSIs {
		if remoteLSI.Name == newName {
			exists = true
			break
		}
	}

	if !exists {
		_, err = longtailutils.WriteBlobWithRetry(ctx, remoteClient, newName, saveBuffer)
		if err != nil {
			return errors.Wrap(err, fname)
		}
		log.Debugf("stored new store index `%s`", newName)
	}

	for _, remoteLSI := range remoteLSIs {
		if remoteLSI.Name == newName {
			continue
		}
		_, err = longtailutils.DeleteBlobWithRetry(ctx, remoteClient, remoteLSI.Name)
		if err != nil && !longtaillib.IsNotExist(err) {
			return errors.Wrap(err, fname)
		}
		log.Debugf("deleted pruned store index `%s`", remoteLSI.Name)
	}

	return nil
}

func mergeLSIs(LSIs []longtaillib.Longtail_StoreIndex) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "mergeLSIs"
	log := logrus.WithFields(logrus.Fields{
		"fname": fname,
		"LSIs":  LSIs,
	})
	log.Debug(fname)

	if len(LSIs) < 2 {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(errors.New("need at least two indexes to merge"), fname)
	}

	result := LSIs[0]
	disposeResult := false
	defer func() {
		if disposeResult {
			result.Dispose()
		}
	}()

	for i := 1; i < len(LSIs); i++ {
		mergedLSI, err := longtaillib.MergeStoreIndex(result, LSIs[i])
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		if disposeResult {
			result.Dispose()
		}
		result = mergedLSI
		disposeResult = true
	}

	disposeResult = false
	return result, nil
}

func PutStoreLSI(ctx context.Context, remoteStore longtailstorelib.BlobStore, localStore *longtailstorelib.BlobStore, newLSI longtaillib.Longtail_StoreIndex, maxStoreIndexSize int64) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "PutStoreLSI"
	log := logrus.WithFields(logrus.Fields{
		"fname":             fname,
		"ctx":               ctx,
		"remoteStore":       remoteStore,
		"LSI":               newLSI,
		"maxStoreIndexSize": maxStoreIndexSize,
	})
	log.Debug(fname)

	LSIs, err := GetStoreLSIs(ctx, remoteStore, localStore)

	// We retry as long as we get a "does not exist" error as that indicates that GetStoreLSIs detected a change in the remote store while reading it
	for longtaillib.IsNotExist(err) {
		time.Sleep(2 * time.Millisecond)
		LSIs, err = GetStoreLSIs(ctx, remoteStore, localStore)
	}

	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	defer func() {
		for _, LSI := range LSIs {
			LSI.LSI.Dispose()
		}
	}()

	ResultLSI := newLSI
	DisposeResultLSI := false
	defer func() {
		if DisposeResultLSI {
			ResultLSI.Dispose()
		}
	}()

	unmergedLSIs := []int{}
	mergedLSIs := []int{}
	if len(LSIs) > 0 {
		sort.Slice(LSIs, func(i, j int) bool { return LSIs[i].LSI.GetSize() < LSIs[j].LSI.GetSize() })
		for i := 0; i < len(LSIs); i++ {
			lsiSize := LSIs[i].LSI.GetSize()
			if lsiSize > maxStoreIndexSize {
				for i < len(LSIs) {
					unmergedLSIs = append(unmergedLSIs, i)
					i++
				}
				break
			}

			MergedLSI, err := longtaillib.MergeStoreIndex(ResultLSI, LSIs[i].LSI)
			if err != nil {
				return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			}

			if MergedLSI.GetSize() > maxStoreIndexSize {
				MergedLSI.Dispose()
				unmergedLSIs = append(unmergedLSIs, i)
				continue
			}

			if DisposeResultLSI {
				ResultLSI.Dispose()
			}
			ResultLSI = MergedLSI
			DisposeResultLSI = true
			log.Debugf("merged store index `%s`", LSIs[i].Name)
			mergedLSIs = append(mergedLSIs, i)

			// We don't need the source LSI anymore
			LSIs[i].LSI.Dispose()
		}
	}

	log.Debugf("merged in %d store indexes, leaving %d unmerged", len(mergedLSIs), len(unmergedLSIs))

	buffer, err := longtaillib.WriteStoreIndexToBuffer(ResultLSI)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	defer buffer.Dispose()

	saveBuffer := buffer.ToBuffer()

	sha256 := sha256.Sum256(saveBuffer)
	newName := fmt.Sprintf("store_%x.lsi", sha256)

	remoteClient, err := remoteStore.NewClient(ctx)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	defer remoteClient.Close()

	_, err = longtailutils.WriteBlobWithRetry(ctx, remoteClient, newName, saveBuffer)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	log.Debugf("stored new store index `%s`", newName)

	{
		for _, Index := range mergedLSIs {
			if LSIs[Index].Name == newName {
				continue
			}
			_, err = longtailutils.DeleteBlobWithRetry(ctx, remoteClient, LSIs[Index].Name)
			if err == nil || longtaillib.IsNotExist(err) {
				log.Debugf("deleted merged store index `%s`", LSIs[Index].Name)
				continue
			}
			log.WithError(err).Warnf("failed to delete `%s` from store `%s`", LSIs[Index].Name, remoteClient.String())
		}
	}

	if len(unmergedLSIs) == 0 {
		if DisposeResultLSI {
			DisposeResultLSI = false
			return ResultLSI, nil
		}
		CopyLSI, err := ResultLSI.Copy()
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		return CopyLSI, nil
	}

	toMerge := []longtaillib.Longtail_StoreIndex{ResultLSI}
	for _, i := range unmergedLSIs {
		toMerge = append(toMerge, LSIs[i].LSI)
	}
	fullLSI, err := mergeLSIs(toMerge)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	return fullLSI, nil
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
	defer remoteClient.Close()
	remoteLSIs, err := remoteClient.GetObjects("store", ".lsi")
	if err != nil && !longtaillib.IsNotExist(err) {
		return nil, errors.Wrap(err, fname)
	}
	log.Debugf("found %d store indexes in remote store", len(remoteLSIs))
	sort.Slice(remoteLSIs, func(i, j int) bool { return remoteLSIs[i].Name < remoteLSIs[j].Name })

	var localClient longtailstorelib.BlobClient
	if localStore != nil {
		localClient, err = (*localStore).NewClient(ctx)
		if err != nil {
			return nil, errors.Wrap(err, fname)
		}
		defer localClient.Close()
	}

	var localLSIs []longtailstorelib.BlobProperties
	if localStore != nil {
		localLSIs, err = localClient.GetObjects("store", ".lsi")
		if err != nil && !longtaillib.IsNotExist(err) {
			return nil, errors.Wrap(err, fname)
		}
		log.Debugf("found %d store indexes in local store", len(localLSIs))
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

	// Syncronize local cache with remote state and read in all store indexes
	for (remoteIndex < remoteCount) && (localIndex < localCount) {
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

		// Exists in remote store, but not locally
		if remoteLSIs[remoteIndex].Name < localLSIs[localIndex].Name {
			newLSIs = append(newLSIs, remoteLSIs[remoteIndex].Name)
			log.Debugf("found new store index `%s` in remote store", remoteLSIs[remoteIndex].Name)
			remoteIndex++
			continue
		}

		// Exists in cache but not in remote
		if remoteLSIs[remoteIndex].Name > localLSIs[localIndex].Name {
			_, err = longtailutils.DeleteBlobWithRetry(ctx, localClient, localLSIs[localIndex].Name)
			if err != nil {
				log.WithError(err).Warnf("failed to delete `%s` from store `%s`", localLSIs[localIndex].Name, localClient.String())
			}
			log.Debugf("removed obsolete store index `%s` from local store", localLSIs[localIndex].Name)
			localIndex++
			continue
		}
	}
	for remoteIndex < remoteCount {
		newLSIs = append(newLSIs, remoteLSIs[remoteIndex].Name)
		remoteIndex++
	}
	for localIndex < localCount {
		_, err = longtailutils.DeleteBlobWithRetry(ctx, localClient, localLSIs[localIndex].Name)
		if err != nil {
			log.WithError(err).Warnf("failed to delete `%s` from store `%s`", localLSIs[localIndex].Name, localClient.String())
		}
		localIndex++
	}

	type Result struct {
		Entry LSIEntry
		Error error
	}
	storeIndexChan := make(chan Result, len(newLSIs))

	// Download all LSIs not in local cache from remote and store them locally
	for _, newLSIName := range newLSIs {
		// TODO: We probably need to limit the number of goroutines we spawn ere as each one will do a network request
		go func(ctx context.Context, remoteStore longtailstorelib.BlobStore, localStore *longtailstorelib.BlobStore, lsiName string, resultChan chan Result) {
			remoteClient, err := remoteStore.NewClient(ctx)
			if err != nil {
				resultChan <- Result{Error: err}
				return
			}
			defer remoteClient.Close()
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

			// Store locally
			if localStore != nil {
				localClient, err := (*localStore).NewClient(ctx)
				if err != nil {
					LSI.Dispose()
					resultChan <- Result{Error: err}
					return
				}
				defer localClient.Close()
				_, err = longtailutils.WriteBlobWithRetry(
					ctx,
					localClient,
					lsiName,
					buffer)
				if err != nil {
					LSI.Dispose()
					resultChan <- Result{Error: err}
					return
				}
			}
			log.Debugf("added store index `%s` to local store", lsiName)
			resultChan <- Result{Entry: LSIEntry{Name: lsiName, LSI: LSI}}
		}(ctx, remoteStore, localStore, newLSIName, storeIndexChan)
	}

	for _ = range newLSIs {
		LSIResult := <-storeIndexChan
		if LSIResult.Error != nil {
			if longtaillib.IsNotExist(LSIResult.Error) {
				if err == nil {
					log.WithFields(logrus.Fields{
						"error": LSIResult.Error,
						"lsi":   LSIResult.Entry.Name,
					}).Debug("remote lsi was removed before reading")
					err = LSIResult.Error
				}
				continue
			}

			if err == nil || longtaillib.IsNotExist(err) {
				log.WithFields(logrus.Fields{
					"error": LSIResult.Error,
					"lsi":   LSIResult.Entry.Name,
				}).Error("failed reading remote lsi")
				err = LSIResult.Error
			}
			continue
		}
		LSIs = append(LSIs, LSIResult.Entry)
	}
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}

	success = true
	log.Debugf("found %d store indexes", len(LSIs))
	return LSIs, nil
}

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

	// We retry as long as we get a "does not exist" error as that indicates that GetStoreLSIs detected a change in the remote store while reading it
	for longtaillib.IsNotExist(err) {
		time.Sleep(2 * time.Millisecond)
		LSIs, err = GetStoreLSIs(ctx, remoteStore, localStore)
	}
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	if len(LSIs) == 0 {
		return longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
	}
	if len(LSIs) == 1 {
		return LSIs[0].LSI, nil
	}

	defer func() {
		for _, LSI := range LSIs {
			LSI.LSI.Dispose()
		}
	}()

	toMerge := []longtaillib.Longtail_StoreIndex{}
	for i := 0; i < len(LSIs); i++ {
		toMerge = append(toMerge, LSIs[i].LSI)
	}
	fullLSI, err := mergeLSIs(toMerge)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	return fullLSI, nil
}
