package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DanEngelbrecht/golongtail/golongtail"
	"github.com/pkg/errors"
)

// FSBloblStore is the base object for all chunk and index stores with FS backing
type FSBloblStore struct {
	root string
}

func (s FSBloblStore) String() string {
	return s.root
}

// NewFSBlobStore initializes a base object used for chunk or index stores backed by FS.
func NewFSBlobStore(rootPath string) (*FSBloblStore, error) {
	s := &FSBloblStore{root: rootPath}

	return s, nil
}

// Close the FS base store. NOP opertation but needed to implement the store interface.
func (s FSBloblStore) Close() error { return nil }

// HasBlob ...
func (s FSBloblStore) HasBlob(ctx context.Context, key string) bool {
	blobPath := path.Join(s.root, key)
	if _, err := os.Stat(blobPath); os.IsNotExist(err) {
		return false
	}
	return true
}

// PutBlob ...
func (s FSBloblStore) PutBlob(ctx context.Context, key string, contentType string, blob []byte) error {
	blobPath := path.Join(s.root, key)
	blobParent, _ := path.Split(blobPath)
	err := os.MkdirAll(blobParent, os.ModePerm)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(blobPath, blob, 0644)
	if err != nil {
		return err
	}
	return nil
}

// GetBlob ...
func (s FSBloblStore) GetBlob(ctx context.Context, key string) ([]byte, error) {
	blobPath := path.Join(s.root, key)
	return ioutil.ReadFile(blobPath)
}

func fsProgressProxy(progressFunc golongtail.ProgressFunc,
	progressContext interface{},
	blockCount uint32,
	blocksCopied *uint32) {

	current := *blocksCopied
	for current < blockCount {
		newCount := *blocksCopied
		if newCount != current {
			progressFunc(progressContext, int(blockCount), int(newCount))
			current = newCount
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// PutContent ...
func (s FSBloblStore) PutContent(
	ctx context.Context,
	progressFunc golongtail.ProgressFunc,
	progressContext interface{},
	contentIndex golongtail.Longtail_ContentIndex,
	fs golongtail.Longtail_StorageAPI,
	contentPath string) error {
	paths, err := golongtail.GetPathsForContentBlocks(contentIndex)
	if err != nil {
		return err
	}

	// TODO: Not the best implementation, it should probably create about one worker per code and have separate connection
	// to GCS for each worker as You cant write to two objects to the same connection apparently
	blockCount := paths.GetPathCount()
	workerCount := uint32(runtime.NumCPU() * 4) // Twice as many as cores - lots of waiting time
	if workerCount > blockCount {
		workerCount = blockCount
	}

	var blocksCopied uint32

	var pg sync.WaitGroup
	if progressFunc != nil {
		pg.Add(int(1))
		go func() {
			fsProgressProxy(progressFunc, progressContext, blockCount, &blocksCopied)
			pg.Done()
		}()
	}

	var wg sync.WaitGroup
	wg.Add(int(workerCount))
	for i := uint32(0); i < workerCount; i++ {
		start := (blockCount * i) / workerCount
		end := ((blockCount * (i + 1)) / workerCount)
		go func(start uint32, end uint32, blocksCopied *uint32) {

			for p := start; p < end; p++ {
				blockBath := golongtail.GetPath(paths, p)

				if s.HasBlob(context.Background(), "chunks/"+blockBath) {
					atomic.AddUint32(blocksCopied, 1)
					continue
				}

				block, err := golongtail.ReadFromStorage(fs, contentPath, blockBath)
				if err != nil {
					log.Printf("Failed to read block: `%s`, %v", blockBath, err)
					break
				}

				err = s.PutBlob(context.Background(), "chunks/"+blockBath, "application/octet-stream", block[:])
				if err != nil {
					log.Printf("Failed to write block: `%s`, %v", blockBath, err)
					break
				}
				atomic.AddUint32(blocksCopied, 1)
			}

			wg.Done()
		}(start, end, &blocksCopied)
	}

	wg.Wait()
	missingCount := blockCount - blocksCopied

	if progressFunc != nil {
		atomic.AddUint32(&blocksCopied, missingCount)
		pg.Wait()
	}
	if missingCount > 0 {
		return fmt.Errorf("Failed to copy %d blocks to `%s`", missingCount, s)
	}

	hash, err := createHashAPIFromIdentifier(contentIndex.GetHashAPI())
	if err != nil {
		return errors.Wrap(err, s.String())
	}
	defer hash.Dispose()

	var storeBlob []byte

	remoteContentPath := path.Join(s.root, "store.lci")
	if _, err := os.Stat(remoteContentPath); os.IsExist(err) {
		blob, err := ioutil.ReadFile(remoteContentPath)
		if err != nil {
			return errors.Wrap(err, s.String())
		}
		remoteContentIndex, err := golongtail.ReadContentIndexFromBuffer(blob)
		if err != nil {
			return errors.Wrap(err, s.String())
		}
		defer remoteContentIndex.Dispose()

		mergedContentIndex, err := golongtail.MergeContentIndex(remoteContentIndex, contentIndex)
		if err != nil {
			return errors.Wrap(err, s.String())
		}
		defer mergedContentIndex.Dispose()

		storeBlob, err = golongtail.WriteContentIndexToBuffer(mergedContentIndex)
		if err != nil {
			return errors.Wrap(err, s.String())
		}
	} else {
		storeBlob, err = golongtail.WriteContentIndexToBuffer(contentIndex)
		if err != nil {
			return errors.Wrap(err, s.String())
		}
	}

	indexParent, _ := path.Split(remoteContentPath)
	err = os.MkdirAll(indexParent, os.ModePerm)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(remoteContentPath, storeBlob, 0644)
	if err != nil {
		return err
	}
	return nil
}

// GetContent ...
func (s FSBloblStore) GetContent(
	ctx context.Context,
	progressFunc golongtail.ProgressFunc,
	progressContext interface{},
	contentIndex golongtail.Longtail_ContentIndex,
	fs golongtail.Longtail_StorageAPI,
	contentPath string) error {
	paths, err := golongtail.GetPathsForContentBlocks(contentIndex)
	if err != nil {
		return err
	}

	// TODO: Not the best implementation, it should probably create about one worker per code and have separate connection
	// to GCS for each worker as You cant write to two objects to the same connection apparently
	blockCount := paths.GetPathCount()
	workerCount := uint32(runtime.NumCPU() * 4) // Twice as many as cores - lots of waiting time
	if workerCount > blockCount {
		workerCount = blockCount
	}

	var blocksCopied uint32

	var pg sync.WaitGroup
	if progressFunc != nil {
		pg.Add(int(1))
		go func() {
			fsProgressProxy(progressFunc, progressContext, blockCount, &blocksCopied)
			pg.Done()
		}()
	}

	var wg sync.WaitGroup
	wg.Add(int(workerCount))
	for i := uint32(0); i < workerCount; i++ {
		start := (blockCount * i) / workerCount
		end := ((blockCount * (i + 1)) / workerCount)
		go func(start uint32, end uint32, blocksCopied *uint32) {

			for p := start; p < end; p++ {
				blockPath := golongtail.GetPath(paths, p)

				localBlockPath := path.Join(contentPath, blockPath)
				if _, err := os.Stat(localBlockPath); os.IsNotExist(err) {
					block, err := s.GetBlob(context.Background(), "chunks/"+blockPath)
					if err != nil {
						log.Printf("Failed to read block: `%s`, %v", blockPath, err)
						break
					}
					err = golongtail.WriteToStorage(fs, contentPath, blockPath, block)
					if err != nil {
						log.Printf("Failed to write block: `%s`, %v", blockPath, err)
						break
					}

				}
				atomic.AddUint32(blocksCopied, 1)
			}

			wg.Done()
		}(start, end, &blocksCopied)
	}

	wg.Wait()
	missingCount := blockCount - blocksCopied

	if progressFunc != nil {
		atomic.AddUint32(&blocksCopied, missingCount)
		pg.Wait()
	}

	if missingCount > 0 {
		return fmt.Errorf("Failed to copy %d blocks from `%s`", missingCount, s)
	}
	return nil
}
