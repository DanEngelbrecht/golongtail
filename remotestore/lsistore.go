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

func PutStoreLSI(ctx context.Context, remoteStore longtailstorelib.BlobClient, LSI longtaillib.Longtail_StoreIndex) error {
	const fname = "PutStoreLSI"
	buffer, err := longtaillib.WriteStoreIndexToBuffer(LSI)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	remoteLSIs, err := remoteStore.GetObjects("store")
	if err != nil {
		return errors.Wrap(err, fname)
	}
	sha256 := sha256.Sum256(buffer.ToBuffer())
	newName := fmt.Sprintf("store_%x.lsi", sha256)
	for _, remoteLSI := range remoteLSIs {
		if remoteLSI.Name == newName {
			return nil
		}
	}
	// TODO: Advanced version will merge LSI with smallest existing LSI = A, merge A with LSI = B, upload B and remove A
	_, err = longtailutils.WriteBlobWithRetry(ctx, remoteStore, newName, buffer.ToBuffer())
	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

// If caller get an IsNotExist(err) it should likely call GetStoreLSI again as the list of store LSI has changed (one that we found was removed)
func GetStoreLSI(ctx context.Context, remoteStore longtailstorelib.BlobClient, localStore longtailstorelib.BlobClient) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "GetStoreLSI"
	//	log.Debug(fname)

	remoteLSIs, err := remoteStore.GetObjects("store")
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	sort.Slice(remoteLSIs, func(i, j int) bool { return remoteLSIs[i].Name < remoteLSIs[j].Name })

	localLSIs, err := localStore.GetObjects("store")
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	sort.Slice(localLSIs, func(i, j int) bool { return localLSIs[i].Name < localLSIs[j].Name })

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
				localStore,
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
			obj, err := localStore.NewObject(localLSIs[localIndex].Name)
			if err != nil {
				return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			}
			err = obj.Delete()
			if err != nil {
				if !longtaillib.IsNotExist(err) {
					return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
				}
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
		obj, err := localStore.NewObject(localLSIs[localIndex].Name)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		err = obj.Delete()
		if err != nil {
			if !longtaillib.IsNotExist(err) {
				return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			}
		}
		localIndex++
	}
	for _, newLSIName := range newLSIs {
		buffer, _, err := longtailutils.ReadBlobWithRetry(
			ctx,
			remoteStore,
			newLSIName)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}

		if localStore != nil {
			_, err = longtailutils.WriteBlobWithRetry(
				ctx,
				localStore,
				newLSIName,
				buffer)
			if err != nil {
				return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			}
		}
		LSI, err := longtaillib.ReadStoreIndexFromBuffer(buffer)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		LSIs = append(LSIs, LSI)
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
