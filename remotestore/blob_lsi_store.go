package remotestore

import (
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/pkg/errors"
)

type blobLSIStore struct {
	client longtailstorelib.BlobClient
}

func (store *blobLSIStore) LSIStore_ListLSIs() ([]string, []int64, error) {
	// TODO: Add retry
	const fname = "blobLSIStore.LSIStore_ListLSIs"
	indexes, err := store.client.GetObjects("store")
	if err != nil {
		return nil, nil, errors.Wrap(err, fname)
	}
	names := make([]string, len(indexes))
	sizes := make([]int64, len(indexes))
	for n, i := range indexes {
		names[n] = i.Name
		sizes[n] = i.Size
	}
	return names, sizes, nil
}

func (store *blobLSIStore) LSIStore_Read(Name string) ([]byte, error) {
	// TODO: Add retry
	const fname = "blobLSIStore.LSIStore_Read"
	object, err := store.client.NewObject(Name)
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	return object.Read()
}
func (store *blobLSIStore) LSIStore_Write(Name string, buffer []byte) error {
	// TODO: Add retry
	const fname = "blobLSIStore.LSIStore_Write"
	object, err := store.client.NewObject(Name)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	_, err = object.Write(buffer)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

func (store *blobLSIStore) LSIStore_Delete(Name string) error {
	// TODO: Add retry?
	const fname = "blobLSIStore.LSIStore_Delete"
	object, err := store.client.NewObject(Name)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	err = object.Delete()
	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

func NewBlobLSIStore(client longtailstorelib.BlobClient) (LSIStore, error) {
	s := &blobLSIStore{client: client}
	return s, nil
}
