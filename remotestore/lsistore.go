package remotestore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
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

func makeLSIBuffer(lsi longtaillib.Longtail_StoreIndex) (longtaillib.NativeBuffer, string, error) {
	const fname = "makeLSIBuffer"
	log := logrus.WithFields(logrus.Fields{
		"LSI": lsi,
	})
	log.Debug(fname)

	buffer, err := longtaillib.WriteStoreIndexToBuffer(lsi)
	if err != nil {
		return longtaillib.NativeBuffer{}, "", errors.Wrap(err, fname)
	}

	sha256 := sha256.Sum256(buffer.ToBuffer())
	newName := fmt.Sprintf("store_%x.lsi", sha256)
	return buffer, newName, nil
}

/*
	1)	Get all LSIs: ExistingLSIs
	2)	Sort LSIs by smallest size to biggest size: MergeCandidates
	3)	Filter all LSIs that are larger or equal in size to newLSI: ConsumeCandidates
			Do a merge to each ConsumeCandidate LSI and see if it consumes all of newLSI into it
			If we find one that consumes newLSI, merge all ExistingLSIs and return that
	4)  Set MergeBase to newLSI
	5)	Attempt merge each MergeCandidate int MergeBase
		- If size of merged result <= maxStoreIndexSize, set MergeBase to merged result, add MergeCandidate to mergedLSIs
		- Else add MergeCandidate to unmergedLSIs
	6)	Check that all LSIs in mergedLSIs still exists, if not, goto 1) (or return NotFound?)
	7)	Write MergeBase to remoteStore (and localStore?)
	8)	Delete all remote LSIs in mergedLSIs
	9)	Merge all unmergedLSIs into MergeBase
	10)	Return MergeBase

	This still leaves a small hole in between 6 and 7 where LSIs are picked up by someone else causing redundant LSIs in the store.
*/

func attemptPutLSI(ctx context.Context, remoteStore longtailstorelib.BlobStore, localStore *longtailstorelib.BlobStore, newLSI longtaillib.Longtail_StoreIndex, newLSIBlocks map[uint64]bool, maxStoreIndexSize int64, workerCount int) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "attemptPutLSI"
	log := logrus.WithFields(logrus.Fields{
		"fname":             fname,
		"ctx":               ctx,
		"remoteStore":       remoteStore,
		"LSI":               newLSI,
		"maxStoreIndexSize": maxStoreIndexSize,
	})
	log.Debug(fname)

	LSIs, err := GetStoreLSIs(ctx, remoteStore, localStore, workerCount)

	// We retry as long as we get a "does not exist" error as that indicates that GetStoreLSIs detected a change in the remote store while reading it
	for longtaillib.IsNotExist(err) {
		time.Sleep(2 * time.Millisecond)
		LSIs, err = GetStoreLSIs(ctx, remoteStore, localStore, workerCount)
	}

	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	remoteClient, err := remoteStore.NewClient(ctx)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	defer remoteClient.Close()

	if len(LSIs) == 0 {
		nativeBuffer, name, err := makeLSIBuffer(newLSI)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		defer nativeBuffer.Dispose()

		_, err = longtailutils.WriteBlobWithRetry(ctx, remoteClient, name, nativeBuffer.ToBuffer())
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		log.Debugf("stored new store index `%s`", name)

		CopyLSI, err := newLSI.Copy()
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		return CopyLSI, nil
	}

	defer func() {
		for _, LSI := range LSIs {
			LSI.LSI.Dispose()
		}
	}()

	newBlockCount := newLSI.GetBlockCount()
	newChunkCount := newLSI.GetChunkCount()

	sort.Slice(LSIs, func(i, j int) bool { return LSIs[i].LSI.GetSize() < LSIs[j].LSI.GetSize() })

	// Attempt merge to see if all of newLSI is consumed by an existing LSI
	for i := 0; i < len(LSIs); i++ {
		candidateBlockCount := LSIs[i].LSI.GetBlockCount()
		candidateChunkCount := LSIs[i].LSI.GetChunkCount()
		if candidateBlockCount >= newBlockCount && candidateChunkCount >= newChunkCount {
			hitCount := 0
			for _, blockHash := range LSIs[i].LSI.GetBlockHashes() {
				_, exists := newLSIBlocks[blockHash]
				if exists {
					hitCount++
				}
			}
			if hitCount == len(newLSIBlocks) {
				if len(LSIs) == 1 {
					CopyLSI, err := LSIs[i].LSI.Copy()
					if err != nil {
						return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
					}
					return CopyLSI, nil
				}
				toMerge := []longtaillib.Longtail_StoreIndex{}
				for _, entry := range LSIs {
					toMerge = append(toMerge, entry.LSI)
				}
				fullLSI, err := mergeLSIs(toMerge)
				if err != nil {
					return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
				}
				return fullLSI, nil
			}
		}
	}

	unmergedLSIs := []int{}
	mergedLSIs := []int{}

	WriteLSI := newLSI
	DisposeWriteLSI := false
	defer func() {
		if DisposeWriteLSI {
			WriteLSI.Dispose()
		}
	}()

	for i := 0; i < len(LSIs); i++ {
		lsiSize := LSIs[i].LSI.GetSize()

		MergedLSI, err := longtaillib.MergeStoreIndex(WriteLSI, LSIs[i].LSI)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}

		mergedLSISize := MergedLSI.GetSize()
		if mergedLSISize > maxStoreIndexSize && mergedLSISize > lsiSize {
			MergedLSI.Dispose()
			unmergedLSIs = append(unmergedLSIs, i)
			continue
		}

		if DisposeWriteLSI {
			WriteLSI.Dispose()
		}
		WriteLSI = MergedLSI
		DisposeWriteLSI = true
		log.Debugf("merged store index `%s`", LSIs[i].Name)
		mergedLSIs = append(mergedLSIs, i)

		// We don't need the source LSI anymore
		LSIs[i].LSI.Dispose()
	}

	nativeBuffer, newName, err := makeLSIBuffer(WriteLSI)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	defer nativeBuffer.Dispose()

	// Did any of the merged LSIs get deleted while we built our LSI?
	// If so bail as we want to avoid merging in the same LSIs to multiple places.
	// There is room for the same LSI to be picked up by two operations between us checking
	// for deleted LSIs and us writing our LSI, which will lead to redundant LSI data.
	// This is unwanted but not critical as any reduncancies will be removed when fetching LSIs.
	for _, Index := range mergedLSIs {
		exists, err := longtailutils.BlobExists(ctx, remoteClient, LSIs[Index].Name)
		if err == nil && !exists {
			err = errors.Wrapf(os.ErrNotExist, "%s merged object already deleted", LSIs[Index].Name)
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
	}

	_, err = longtailutils.WriteBlobWithRetry(ctx, remoteClient, newName, nativeBuffer.ToBuffer())
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	log.Debugf("stored new store index `%s`", newName)

	// There is still a potential for holes here where things gets deleted before we have finished our
	// delete cleanup. That will result in redundancy in the list of LSIs which is unwanted but not critical problem.
	// We do our best to avoid it, but we can't make this 100% airtight.
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

	if len(unmergedLSIs) == 0 {
		if DisposeWriteLSI {
			DisposeWriteLSI = false
			return WriteLSI, nil
		}
		CopyLSI, err := WriteLSI.Copy()
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		return CopyLSI, nil
	}

	// Merge in all unmergedLSIs

	toMerge := []longtaillib.Longtail_StoreIndex{WriteLSI}
	for _, i := range unmergedLSIs {
		toMerge = append(toMerge, LSIs[i].LSI)
	}
	fullLSI, err := mergeLSIs(toMerge)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	return fullLSI, nil
}

func PutStoreLSI(ctx context.Context, remoteStore longtailstorelib.BlobStore, localStore *longtailstorelib.BlobStore, newLSI longtaillib.Longtail_StoreIndex, maxStoreIndexSize int64, workerCount int) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "PutStoreLSI"
	log := logrus.WithFields(logrus.Fields{
		"fname":             fname,
		"ctx":               ctx,
		"remoteStore":       remoteStore,
		"LSI":               newLSI,
		"maxStoreIndexSize": maxStoreIndexSize,
	})
	log.Debug(fname)

	blockMap := map[uint64]bool{}
	for _, blockHash := range newLSI.GetBlockHashes() {
		blockMap[blockHash] = true
	}

	result, err := attemptPutLSI(ctx, remoteStore, localStore, newLSI, blockMap, maxStoreIndexSize, workerCount)
	for longtaillib.IsNotExist(err) {
		time.Sleep(2 * time.Millisecond)
		result, err = attemptPutLSI(ctx, remoteStore, localStore, newLSI, blockMap, maxStoreIndexSize, workerCount)
	}

	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	return result, err
}

type LSIEntry struct {
	Name string
	LSI  longtaillib.Longtail_StoreIndex
}

func GetStoreLSIs(ctx context.Context, remoteStore longtailstorelib.BlobStore, localStore *longtailstorelib.BlobStore, workerCount int) ([]LSIEntry, error) {
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
	lsiCount := len(newLSIs)
	if lsiCount == 0 {
		success = true
		log.Debugf("found %d store indexes", len(LSIs))
		return LSIs, nil
	}

	storeIndexChan := make(chan Result, lsiCount)

	if lsiCount < workerCount {
		workerCount = lsiCount
	}

	// Download all LSIs not in local cache from remote and store them locally
	fetchLSIChannel := make(chan string, lsiCount)
	for _, newLSIName := range newLSIs {
		fetchLSIChannel <- newLSIName
	}
	for i := 0; i < workerCount; i++ {
		go func() {
			remoteClient, err := remoteStore.NewClient(ctx)
			if err == nil {
				defer remoteClient.Close()
			}

			var localClient longtailstorelib.BlobClient
			if localStore != nil {
				localClient, err = (*localStore).NewClient(ctx)
				if err == nil {
					defer localClient.Close()
				}
			}

			for lsiName := range fetchLSIChannel {
				if err != nil {
					storeIndexChan <- Result{Error: err}
					continue
				}

				buffer, _, err := longtailutils.ReadBlobWithRetry(
					ctx,
					remoteClient,
					lsiName)
				if err != nil {
					storeIndexChan <- Result{Error: err}
					continue
				}

				LSI, err := longtaillib.ReadStoreIndexFromBuffer(buffer)
				if err != nil {
					storeIndexChan <- Result{Error: err}
					continue
				}

				// Store locally
				if localClient != nil {
					_, err = longtailutils.WriteBlobWithRetry(
						ctx,
						localClient,
						lsiName,
						buffer)
					if err != nil {
						LSI.Dispose()
						storeIndexChan <- Result{Error: err}
						continue
					}
				}
				log.Debugf("added store index `%s` to local store", lsiName)
				storeIndexChan <- Result{Entry: LSIEntry{Name: lsiName, LSI: LSI}}
			}
		}()
	}
	close(fetchLSIChannel)

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

func GetStoreLSI(ctx context.Context, remoteStore longtailstorelib.BlobStore, localStore *longtailstorelib.BlobStore, workerCount int) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "GetStoreLSI"
	log := logrus.WithFields(logrus.Fields{
		"fname":       fname,
		"ctx":         ctx,
		"remoteStore": remoteStore,
		"localStore":  localStore,
	})
	log.Debug(fname)

	LSIs, err := GetStoreLSIs(ctx, remoteStore, localStore, workerCount)

	// We retry as long as we get a "does not exist" error as that indicates that GetStoreLSIs detected a change in the remote store while reading it
	for longtaillib.IsNotExist(err) {
		time.Sleep(2 * time.Millisecond)
		LSIs, err = GetStoreLSIs(ctx, remoteStore, localStore, workerCount)
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
