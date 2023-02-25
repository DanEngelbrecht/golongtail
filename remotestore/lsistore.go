package remotestore

import (
	"crypto/sha256"
	"fmt"
	"sort"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/pkg/errors"
)

type LSIStore interface {
	LSIStore_ListLSIs() ([]string, []int64, error)
	LSIStore_Read(Name string) ([]byte, error)
	LSIStore_Write(Name string, buffer []byte) error
	LSIStore_Delete(Name string) error
}

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

func PutStoreLSI(remoteStore *LSIStore, LSI longtaillib.Longtail_StoreIndex) error {
	const fname = "PutStoreLSI"
	buffer, err := longtaillib.WriteStoreIndexToBuffer(LSI)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	remoteLSINames, _, err := (*remoteStore).LSIStore_ListLSIs()
	if err != nil {
		return errors.Wrap(err, fname)
	}
	sha256 := sha256.Sum256(buffer.ToBuffer())
	newName := fmt.Sprintf("store_%x.lsi", sha256)
	for _, name := range remoteLSINames {
		if name == newName {
			return nil
		}
	}
	// TODO: Advanced version will merge LSI with smallest existing LSI = A, merge A with LSI = B, upload B and remove A
	err = (*remoteStore).LSIStore_Write(newName, buffer.ToBuffer())
	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

// If caller get an IsNotExist(err) it should likely call GetStoreLSI again as the list of store LSI has changed (one that we found was removed)
func GetStoreLSI(remoteStore *LSIStore, localStore *LSIStore) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "GetStoreLSI"
	//	log.Debug(fname)
	remoteLSINames, _, err := (*remoteStore).LSIStore_ListLSIs()
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	var localLSINames []string
	if localStore != nil {
		localLSINames, _, err = (*localStore).LSIStore_ListLSIs()
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
	}
	sort.Strings(remoteLSINames)
	sort.Strings(localLSINames)
	remoteCount := len(remoteLSINames)
	localCount := len(localLSINames)
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
		if remoteLSINames[remoteIndex] == localLSINames[localIndex] {
			buffer, err := (*localStore).LSIStore_Read(localLSINames[localIndex])
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
		if remoteLSINames[remoteIndex] < localLSINames[localIndex] {
			newLSIs = append(newLSIs, remoteLSINames[remoteIndex])
			remoteIndex++
			continue
		}
		if remoteLSINames[remoteIndex] > localLSINames[localIndex] {
			err = (*localStore).LSIStore_Delete((localLSINames[localIndex]))
			if err != nil {
				return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			}
			localIndex++
			continue
		}
	}
	for remoteIndex < remoteCount {
		newLSIs = append(newLSIs, remoteLSINames[remoteIndex])
		remoteIndex++
	}
	for localIndex < localCount {
		(*localStore).LSIStore_Delete(localLSINames[localIndex])
		localIndex++
	}
	for _, newLSIName := range newLSIs {
		buffer, err := (*remoteStore).LSIStore_Read(newLSIName)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		if localStore != nil {
			err = (*localStore).LSIStore_Write(newLSIName, buffer)
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

type nilLSIStore struct {
}

func (store *nilLSIStore) LSIStore_ListLSIs() ([]string, []int64, error)   { return nil, nil, nil }
func (store *nilLSIStore) LSIStore_Read(Name string) ([]byte, error)       { return nil, nil }
func (store *nilLSIStore) LSIStore_Write(Name string, buffer []byte) error { return nil }
func (store *nilLSIStore) LSIStore_Delete(Name string) error               { return nil }

func NewNilLSIStore() (LSIStore, error) {
	s := &nilLSIStore{}
	return s, nil
}
