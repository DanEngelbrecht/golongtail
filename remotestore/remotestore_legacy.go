package remotestore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func tryAddRemoteStoreIndexWithLockingLegacy(
	ctx context.Context,
	addStoreIndex longtaillib.Longtail_StoreIndex,
	blobClient longtailstorelib.BlobClient) (bool, longtaillib.Longtail_StoreIndex, error) {
	const fname = "tryAddRemoteStoreIndexWithLockingLegacy"
	log := logrus.WithFields(logrus.Fields{
		"fname":      fname,
		"blobClient": blobClient,
	})
	log.Debug(fname)

	key := "store.lsi"
	objHandle, err := blobClient.NewObject(key)
	if err != nil {
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	exists, err := objHandle.LockWriteVersion()
	if err != nil {
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	if exists {
		blob, err := objHandle.Read()
		if err != nil {
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		if longtaillib.IsNotExist(err) {
			return false, longtaillib.Longtail_StoreIndex{}, nil
		}

		remoteStoreIndex, err := longtaillib.ReadStoreIndexFromBuffer(blob)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Cant parse store index from `%s/%s`", blobClient.String(), key))
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		defer remoteStoreIndex.Dispose()
		log.WithFields(logrus.Fields{"path": objHandle.String(), "bytes": len(blob)}).Info("read store index")

		newStoreIndex, err := longtaillib.MergeStoreIndex(remoteStoreIndex, addStoreIndex)
		if err != nil {
			err = errors.Wrap(err, fmt.Sprintf("Failed merging store index for `%s/%s`", blobClient.String(), key))
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}

		storeBlob, err := longtaillib.WriteStoreIndexToBuffer(newStoreIndex)
		if err != nil {
			newStoreIndex.Dispose()
			err = errors.Wrap(err, fmt.Sprintf("Failed serializing store index for `%s/%s`", blobClient.String(), key))
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		defer storeBlob.Dispose()

		ok, err := objHandle.Write(storeBlob.ToBuffer())
		if err != nil {
			newStoreIndex.Dispose()
			return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
		}
		if !ok {
			newStoreIndex.Dispose()
			return false, longtaillib.Longtail_StoreIndex{}, nil
		}
		log.WithFields(logrus.Fields{"path": objHandle.String(), "bytes": storeBlob.Size()}).Info("wrote store index")
		return ok, newStoreIndex, nil
	}
	storeBlob, err := longtaillib.WriteStoreIndexToBuffer(addStoreIndex)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Failed serializing store index for `%s/%s`", blobClient.String(), key))
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	defer storeBlob.Dispose()

	ok, err := objHandle.Write(storeBlob.ToBuffer())
	if err != nil {
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	if ok {
		log.WithFields(logrus.Fields{"path": objHandle.String(), "bytes": storeBlob.Size()}).Info("wrote store index")
	}
	return ok, longtaillib.Longtail_StoreIndex{}, nil
}

func tryWriteRemoteStoreIndexLegacy(
	ctx context.Context,
	storeIndex longtaillib.Longtail_StoreIndex,
	existingIndexItems []string,
	blobClient longtailstorelib.BlobClient) (bool, error) {
	const fname = "tryWriteRemoteStoreIndexLegacy"
	log := logrus.WithFields(logrus.Fields{
		"fname":      fname,
		"blobClient": blobClient,
	})
	log.Debug(fname)

	storeBlob, err := longtaillib.WriteStoreIndexToBuffer(storeIndex)
	if err != nil {
		err := errors.Wrap(err, "Failed serializing store index")
		return false, errors.Wrap(err, fname)
	}
	defer storeBlob.Dispose()

	sha256 := sha256.Sum256(storeBlob.ToBuffer())
	key := fmt.Sprintf("store_%x.lsi", sha256)

	for _, item := range existingIndexItems {
		if item == key {
			return true, nil
		}
	}

	objHandle, err := blobClient.NewObject(key)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	exists, err := objHandle.Exists()
	if err != nil {
		return false, errors.Wrap(err, fname)
	}
	if exists {
		return false, nil
	}

	ok, err := objHandle.Write(storeBlob.ToBuffer())
	if !ok || err != nil {
		return ok, errors.Wrap(err, fname)
	}

	log.WithFields(logrus.Fields{"path": objHandle.String(), "bytes": storeBlob.Size()}).Info("wrote store index")

	for _, item := range existingIndexItems {
		objHandle, err := blobClient.NewObject(item)
		if err != nil {
			continue
		}
		err = objHandle.Delete()
		if err != nil {
			continue
		}
	}

	return true, nil
}

func tryAddRemoteStoreIndexLegacy(
	ctx context.Context,
	addStoreIndex longtaillib.Longtail_StoreIndex,
	blobClient longtailstorelib.BlobClient) (bool, longtaillib.Longtail_StoreIndex, error) {
	const fname = "tryAddRemoteStoreIndexLegacy"
	log := logrus.WithFields(logrus.Fields{
		"fname":      fname,
		"blobClient": blobClient.String(),
	})
	log.Debug(fname)

	if blobClient.SupportsLocking() {
		return tryAddRemoteStoreIndexWithLockingLegacy(ctx, addStoreIndex, blobClient)
	}

	storeIndex, items, err := readStoreStoreIndexWithItemsLegacy(ctx, blobClient)
	if err != nil {
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	if !storeIndex.IsValid() {
		ok, err := tryWriteRemoteStoreIndexLegacy(ctx, addStoreIndex, items, blobClient)
		return ok, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	mergedStoreIndex, err := longtaillib.MergeStoreIndex(storeIndex, addStoreIndex)
	storeIndex.Dispose()
	if err != nil {
		err = errors.Wrap(err, "Failed merging store index")
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}

	ok, err := tryWriteRemoteStoreIndexLegacy(ctx, mergedStoreIndex, items, blobClient)
	if err != nil {
		return false, longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	return ok, mergedStoreIndex, nil
}

func tryOverwriteRemoteStoreIndexWithLockingLegacy(
	ctx context.Context,
	storeIndex longtaillib.Longtail_StoreIndex,
	client longtailstorelib.BlobClient) (bool, error) {
	const fname = "tryOverwriteRemoteStoreIndexWithLockingLegacy"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"client": client,
	})
	log.Debug(fname)

	storeBlob, err := longtaillib.WriteStoreIndexToBuffer(storeIndex)
	if err != nil {
		err := errors.Wrap(err, "Failed serializing store index")
		return false, errors.Wrap(err, fname)
	}
	defer storeBlob.Dispose()

	key := "store.lsi"
	objHandle, err := client.NewObject(key)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	_, err = objHandle.LockWriteVersion()
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	ok, err := objHandle.Write(storeBlob.ToBuffer())
	if err != nil {
		return false, errors.Wrap(err, fname)
	}
	return ok, nil
}

func tryOverwriteRemoteStoreIndexWithoutLockingLegacy(
	ctx context.Context,
	storeIndex longtaillib.Longtail_StoreIndex,
	client longtailstorelib.BlobClient) (bool, error) {
	const fname = "tryOverwriteRemoteStoreIndexWithoutLockingLegacy"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"client": client.String(),
	})
	log.Debug(fname)

	items, err := getStoreStoreIndexesLegacy(ctx, client)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	storeBlob, err := longtaillib.WriteStoreIndexToBuffer(storeIndex)
	if err != nil {
		err = errors.Wrap(err, "Failed serializing store index")
		return false, errors.Wrap(err, fname)
	}
	defer storeBlob.Dispose()

	sha256 := sha256.Sum256(storeBlob.ToBuffer())
	key := fmt.Sprintf("store_%x.lsi", sha256)

	objHandle, err := client.NewObject(key)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	exists, err := objHandle.Exists()
	if err != nil {
		return false, errors.Wrap(err, fname)
	}
	if !exists {
		ok, err := objHandle.Write(storeBlob.ToBuffer())
		if !ok || err != nil {
			return ok, errors.Wrap(err, fname)
		}
	}

	for _, item := range items {
		if item == key {
			continue
		}
		objHandle, err := client.NewObject(item)
		if err != nil {
			continue
		}
		err = objHandle.Delete()
		if err != nil {
			continue
		}
	}

	return true, nil
}

func tryOverwriteRemoteStoreIndexLegacy(
	ctx context.Context,
	storeIndex longtaillib.Longtail_StoreIndex,
	blobClient longtailstorelib.BlobClient) (bool, error) {
	const fname = "tryOverwriteRemoteStoreIndexLegacy"
	log := logrus.WithFields(logrus.Fields{
		"fname":      fname,
		"blobClient": blobClient,
	})
	log.Debug(fname)

	if blobClient.SupportsLocking() {
		return tryOverwriteRemoteStoreIndexWithLockingLegacy(ctx, storeIndex, blobClient)
	}
	return tryOverwriteRemoteStoreIndexWithoutLockingLegacy(ctx, storeIndex, blobClient)
}

func tryOverwriteStoreIndexWithRetryLegacy(
	ctx context.Context,
	storeIndex longtaillib.Longtail_StoreIndex,
	blobClient longtailstorelib.BlobClient) error {
	const fname = "tryOverwriteStoreIndexWithRetryLegacy"
	log := logrus.WithFields(logrus.Fields{
		"fname":      fname,
		"blobClient": blobClient,
	})
	log.Debug(fname)

	errorRetries := 0
	for {
		ok, err := tryOverwriteRemoteStoreIndexLegacy(
			ctx,
			storeIndex,
			blobClient)
		if ok {
			return nil
		}
		if err != nil {
			errorRetries++
			if errorRetries == 3 {
				log.Errorf("Failed updating remote store after %d tryAddRemoteStoreIndex: %s", 3, err)
				return errors.Wrapf(err, fname)
			} else {
				log.Warnf("Error from tryOverwriteStoreIndexWithRetryLegacy %s", err)
			}
		}
		log.Debug("Retrying updating remote store index")
	}
}

func readStoreStoreIndexFromPathLegacy(
	ctx context.Context,
	key string,
	client longtailstorelib.BlobClient) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "readStoreStoreIndexFromPathLegacy"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"key":    key,
		"client": client.String(),
	})
	log.Debug(fname)

	blobData, _, err := longtailutils.ReadBlobWithRetry(ctx, client, key)
	if err != nil {
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
	}
	if len(blobData) == 0 {
		err = errors.Wrap(os.ErrNotExist, fmt.Sprintf("%s/%s contains no data", client.String(), key))
		return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
	}
	storeIndex, err := longtaillib.ReadStoreIndexFromBuffer(blobData)
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Cant parse store index from `%s/%s`", client.String(), key))
		return longtaillib.Longtail_StoreIndex{}, errors.Wrapf(err, fname)
	}
	return storeIndex, nil
}

func getStoreStoreIndexesLegacy(
	ctx context.Context,
	client longtailstorelib.BlobClient) ([]string, error) {
	const fname = "getStoreStoreIndexesLegacy"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"client": client.String(),
	})
	log.Debug(fname)

	var items []string
	retryCount := 0
	retryDelay := []time.Duration{0, 100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, 1 * time.Second, 2 * time.Second}
	blobs, err := client.GetObjects("store")
	for err != nil {
		if longtaillib.IsNotExist(err) {
			return items, nil
		}
		if retryCount == len(retryDelay) {
			err = errors.Wrapf(err, "Failed list store indexes in store %s", client.String())
			log.Error(err)
			return nil, errors.Wrap(err, fname)
		}
		err = errors.Wrapf(err, "Retrying list store indexes in store %s with %s delay", client.String(), retryDelay[retryCount])
		log.Info(err)

		time.Sleep(retryDelay[retryCount])
		retryCount++
		blobs, err = client.GetObjects("store")
	}

	for _, blob := range blobs {
		if blob.Size == 0 {
			continue
		}
		if strings.HasSuffix(blob.Name, ".lsi") {
			items = append(items, blob.Name)
		}
	}
	return items, nil
}

func mergeStoreIndexItemsLegacy(
	ctx context.Context,
	client longtailstorelib.BlobClient,
	items []string) (longtaillib.Longtail_StoreIndex, []string, error) {
	const fname = "mergeStoreIndexItemsLegacy"
	log := logrus.WithFields(logrus.Fields{
		"fname":      fname,
		"client":     client.String(),
		"len(items)": len(items),
	})
	log.Debug(fname)

	var usedItems []string
	storeIndex := longtaillib.Longtail_StoreIndex{}
	for _, item := range items {
		tmpStoreIndex, err := readStoreStoreIndexFromPathLegacy(ctx, item, client)

		if err != nil || (!tmpStoreIndex.IsValid()) {
			storeIndex.Dispose()
			if longtaillib.IsNotExist(err) {
				// The file we expected is no longer there, tell caller that we need to try again
				return longtaillib.Longtail_StoreIndex{}, nil, nil
			}
			return longtaillib.Longtail_StoreIndex{}, nil, errors.Wrap(err, fname)
		}
		if !storeIndex.IsValid() {
			storeIndex = tmpStoreIndex
			usedItems = append(usedItems, item)
			continue
		}
		mergedStoreIndex, err := longtaillib.MergeStoreIndex(storeIndex, tmpStoreIndex)
		tmpStoreIndex.Dispose()
		storeIndex.Dispose()
		if err != nil {
			err := errors.Wrap(err, "contentIndexWorker: longtaillib.MergeStoreIndex() failed")
			return longtaillib.Longtail_StoreIndex{}, nil, errors.Wrap(err, fname)
		}
		storeIndex = mergedStoreIndex
		usedItems = append(usedItems, item)
	}
	return storeIndex, usedItems, nil
}

func readStoreStoreIndexWithItemsLegacy(
	ctx context.Context,
	client longtailstorelib.BlobClient) (longtaillib.Longtail_StoreIndex, []string, error) {
	const fname = "readStoreStoreIndexWithItemsLegacy"
	log := logrus.WithFields(logrus.Fields{
		"fname":  fname,
		"client": client.String(),
	})
	log.Debug(fname)

	for {
		items, err := getStoreStoreIndexesLegacy(ctx, client)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, nil, err
		}

		if len(items) == 0 {
			storeIndex, err := longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
			if err != nil {
				err := errors.Wrap(err, "Failed to create empty store index")
				return longtaillib.Longtail_StoreIndex{}, nil, errors.Wrapf(err, fname)
			}
			log.Info("created empty store index")
			return storeIndex, nil, nil
		} else {
			log.WithFields(logrus.Fields{"items": items}).Info("read store index items")
		}

		storeIndex, usedItems, err := mergeStoreIndexItemsLegacy(ctx, client, items)
		if err != nil {
			return longtaillib.Longtail_StoreIndex{}, nil, errors.Wrapf(err, fname)
		}
		if len(usedItems) == 0 {
			// The underlying index files changed as we were scanning them, abort and try again
			continue
		}
		if storeIndex.IsValid() {
			return storeIndex, usedItems, nil
		}
		log.Infof("Retrying reading remote store index")
	}
}

func addToRemoteStoreIndexLegacy(
	ctx context.Context,
	blobClient longtailstorelib.BlobClient,
	addStoreIndex longtaillib.Longtail_StoreIndex) (longtaillib.Longtail_StoreIndex, error) {
	const fname = "addToRemoteStoreIndexLegacy"
	log := logrus.WithFields(logrus.Fields{
		"fname":      fname,
		"blobClient": blobClient,
	})
	log.Debug(fname)

	errorRetries := 0
	for {
		ok, newStoreIndex, err := tryAddRemoteStoreIndexLegacy(
			ctx,
			addStoreIndex,
			blobClient)
		if ok {
			return newStoreIndex, nil
		}
		if err != nil {
			errorRetries++
			if errorRetries == 3 {
				log.Errorf("Failed updating remote store after %d tryAddRemoteStoreIndex: %s", 3, err)
				return longtaillib.Longtail_StoreIndex{}, errors.Wrap(err, fname)
			} else {
				log.Warnf("Error from tryAddRemoteStoreIndex %s", err)
			}
		}
		log.Debug("Retrying updating remote store index")
	}
	return longtaillib.Longtail_StoreIndex{}, nil
}
