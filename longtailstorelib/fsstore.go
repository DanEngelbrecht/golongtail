package longtailstorelib

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
)

type fsBlobStore struct {
	enableLocking bool
	prefix        string
}

type fsBlobClient struct {
	store *fsBlobStore
}

type fsBlobObject struct {
	client         *fsBlobClient
	path           string
	metageneration int64
}

// NewFSBlobStore ...
func NewFSBlobStore(prefix string, enableLocking bool) (BlobStore, error) {
	s := &fsBlobStore{prefix: prefix, enableLocking: enableLocking}
	return s, nil
}

func (blobStore *fsBlobStore) NewClient(ctx context.Context) (BlobClient, error) {
	return &fsBlobClient{store: blobStore}, nil
}

func (blobStore *fsBlobStore) String() string {
	return "fsstore" + blobStore.prefix
}

func (blobClient *fsBlobClient) NewObject(filepath string) (BlobObject, error) {
	fsPath := path.Join(blobClient.store.prefix, filepath)
	return &fsBlobObject{client: blobClient, path: fsPath, metageneration: -1}, nil
}

func (blobClient *fsBlobClient) GetObjects(pathPrefix string) ([]BlobProperties, error) {
	const fname = "fsBlobObject.GetObjects"
	searchPath := blobClient.store.prefix

	objects := make([]BlobProperties, 0)
	err := filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		leafPath := path[len(searchPath)+1:]
		if leafPath[:len(pathPrefix)] == pathPrefix {
			props := BlobProperties{Size: info.Size(), Name: leafPath}
			objects = append(objects, props)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}

	return objects, nil
}

func (blobClient *fsBlobClient) SupportsLocking() bool {
	return blobClient.store.enableLocking
}

func (blobClient *fsBlobClient) Close() {
}

func (blobClient *fsBlobClient) String() string {
	return blobClient.store.String() + ":client"
}

func (blobObject *fsBlobObject) Exists() (bool, error) {
	const fname = "fsBlobObject.Exists"
	_, err := os.Stat(blobObject.path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("Failed to check for existance `%s`", blobObject.path))
		return false, errors.Wrap(err, fname)
	}
	return true, nil
}

func (blobObject *fsBlobObject) Read() ([]byte, error) {
	const fname = "fsBlobObject.Read"
	data, err := ioutil.ReadFile(blobObject.path)
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	return data, nil
}

func (blobObject *fsBlobObject) getMetaGeneration() (int64, error) {
	const fname = "fsBlobObject.getMetaGeneration"
	metapath := blobObject.path + ".gen"
	data, err := ioutil.ReadFile(metapath)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, errors.Wrap(err, fname)
	}
	meta_generation := int64(binary.LittleEndian.Uint64(data))
	return meta_generation, nil
}

func (blobObject *fsBlobObject) setMetaGeneration(meta_generation int64) error {
	const fname = "fsBlobObject.setMetaGeneration"
	metapath := blobObject.path + ".gen"

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(meta_generation))
	err := ioutil.WriteFile(metapath, data, 0644)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

func (blobObject *fsBlobObject) deleteGeneration() error {
	const fname = "fsBlobObject.deleteGeneration"
	metapath := blobObject.path + ".gen"
	err := os.Remove(metapath)
	if err != nil {
		return errors.Wrap(err, fname)
	}
	return nil
}

func (blobObject *fsBlobObject) lockFile() (*Lock, error) {
	const fname = "fsBlobObject.lockFile"

	lockPath := blobObject.path + "._lck"

	err := os.MkdirAll(filepath.Dir(blobObject.path), os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}

	filelock := NewFileLock(lockPath)
	err = filelock.Lock()
	if err != nil {
		return nil, errors.Wrap(err, fname)
	}
	return filelock, nil
}

func (blobObject *fsBlobObject) LockWriteVersion() (bool, error) {
	const fname = "fsBlobObject.LockWriteVersion"

	if !blobObject.client.store.enableLocking {
		err := fmt.Errorf("Locking is not supported for %s", blobObject.client.store.String())
		return false, errors.Wrap(err, fname)
	}

	filelock, err := blobObject.lockFile()
	if err != nil {
		return false, errors.Wrap(err, fname)
	}
	defer filelock.Unlock()

	exists, err := blobObject.Exists()
	if err != nil {
		return false, err
	}

	if exists {
		blobObject.metageneration, err = blobObject.getMetaGeneration()
		if err != nil {
			return exists, errors.Wrap(err, fname)
		}
	} else {
		blobObject.metageneration = 0
	}

	return exists, err
}

func (blobObject *fsBlobObject) Write(data []byte) (bool, error) {
	const fname = "fsBlobObject.Write"

	if blobObject.client.store.enableLocking {
		filelock, err := blobObject.lockFile()
		if err != nil {
			return false, errors.Wrap(err, fname)
		}
		defer filelock.Unlock()
	}

	err := os.MkdirAll(filepath.Dir(blobObject.path), os.ModePerm)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}

	if blobObject.client.store.enableLocking {
		if blobObject.metageneration != -1 {
			currentMetaGeneration, err := blobObject.getMetaGeneration()
			if err != nil {
				return false, errors.Wrap(err, fname)
			}
			if currentMetaGeneration != blobObject.metageneration {
				return false, nil
			}
		}
	}

	err = ioutil.WriteFile(blobObject.path, data, 0644)
	if err != nil {
		return false, errors.Wrap(err, fname)
	}
	if blobObject.client.store.enableLocking {
		if blobObject.metageneration != -1 {
			err = blobObject.setMetaGeneration(blobObject.metageneration + 1)
			if err != nil {
				return false, errors.Wrap(err, fname)
			}
		}
	}
	return true, nil
}

func (blobObject *fsBlobObject) Delete() error {
	const fname = "fsBlobObject.Delete"

	if blobObject.client.store.enableLocking {
		filelock, err := blobObject.lockFile()
		if err != nil {
			return errors.Wrap(err, fname)
		}
		defer filelock.Unlock()

		if blobObject.metageneration != -1 {
			currentMetaGeneration, err := blobObject.getMetaGeneration()
			if err != nil {
				return errors.Wrap(err, fname)
			}
			if currentMetaGeneration != blobObject.metageneration {
				err = fmt.Errorf(fmt.Sprintf("Failed to delete `%s`, meta generation mismatch", blobObject.path))
				return errors.Wrap(err, fname)
			}
		}
	}
	err := os.Remove(blobObject.path)
	if err != nil {
		return errors.Wrap(err, fname)
	}

	if blobObject.client.store.enableLocking {
		err = blobObject.deleteGeneration()
		if err != nil {
			return errors.Wrap(err, fname)
		}
	}
	return nil
}

// ErrTimeout indicates that the lock attempt timed out.
var ErrTimeout error = timeoutError("lock timeout exceeded")

type timeoutError string

func (t timeoutError) Error() string {
	return string(t)
}
func (timeoutError) Timeout() bool {
	return true
}

// ErrLocked indicates TryLock failed because the lock was already locked.
var ErrLocked error = trylockError("fslock is already locked")

type trylockError string

func (t trylockError) Error() string {
	return string(t)
}

func (trylockError) Temporary() bool {
	return true
}
