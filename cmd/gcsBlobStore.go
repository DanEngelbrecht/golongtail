package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/DanEngelbrecht/golongtail/golongtail"
	"github.com/pkg/errors"
)

// GCSBlobStore is the base object for all chunk and index stores with GCS backing
type GCSBlobStore struct {
	url      *url.URL
	Location string
	client   *storage.Client
	bucket   *storage.BucketHandle
}

func (s GCSBlobStore) String() string {
	return s.Location
}

// NewGCSBlobStore initializes a base object used for chunk or index stores backed by GCS.
func NewGCSBlobStore(u *url.URL) (*GCSBlobStore, error) {
	var err error
	s := &GCSBlobStore{url: u, Location: u.String()}
	if u.Scheme != "gs" {
		return s, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	ctx := context.Background()
	s.client, err = storage.NewClient(ctx)
	if err != nil {
		return s, errors.Wrap(err, u.String())
	}

	bucketName := u.Host
	s.bucket = s.client.Bucket(bucketName)

	return s, nil
}

// Close the GCS base store. NOP opertation but needed to implement the store interface.
func (s GCSBlobStore) Close() error { return nil }

// HasObjectBlob ...
func (s GCSBlobStore) HasBlob(ctx context.Context, key string) bool {
	objHandle := s.bucket.Object(key)
	_, err := objHandle.Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return false
	}
	return true
}

// PutObjectBlob ...
func (s GCSBlobStore) PutBlob(ctx context.Context, key string, contentType string, blob []byte) error {
	objHandle := s.bucket.Object(key)
	objWriter := objHandle.NewWriter(ctx)

	_, err := objWriter.Write(blob)
	if err != nil {
		objWriter.Close()
		return errors.Wrap(err, s.String()+"/"+key)
	}

	err = objWriter.Close()
	if err != nil {
		return errors.Wrap(err, s.String()+"/"+key)
	}

	_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: contentType})
	if err != nil {
		return errors.Wrap(err, s.String()+"/"+key)
	}

	return nil
}

// GetObjectBlob ...
func (s GCSBlobStore) GetBlob(ctx context.Context, key string) ([]byte, error) {
	objHandle := s.bucket.Object(key)
	obj, err := objHandle.NewReader(ctx)
	if err != nil {
		return nil, errors.Wrap(err, s.String()+"/"+key)
	}
	defer obj.Close()

	b, err := ioutil.ReadAll(obj)

	if err != nil {
		return nil, err
	}

	return b, nil
}

func gcsProgressProxy(progressFunc golongtail.ProgressFunc,
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
func (s GCSBlobStore) PutContent(
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

	incompleteCount := blockCount
	var blocksCopied uint32
	var missingCount uint32

	var pg sync.WaitGroup
	if progressFunc != nil {
		pg.Add(int(1))
		go func() {
			gcsProgressProxy(progressFunc, progressContext, blockCount, &blocksCopied)
			pg.Done()
		}()
	}

	for retryCount := 0; retryCount < 5; retryCount++ {

		var wg sync.WaitGroup
		wg.Add(int(workerCount))

		for i := uint32(0); i < workerCount; i++ {
			start := (blockCount * i) / workerCount
			end := ((blockCount * (i + 1)) / workerCount)
			go func(start uint32, end uint32, blocksCopied *uint32) {

				workerStore, err := NewGCSBlobStore(s.url)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to connect to: `%s`, %v", s.url, err)
					wg.Done()
					return
				}
				defer workerStore.Close()

				for p := start; p < end; p++ {
					path := golongtail.GetPath(paths, p)

					if workerStore.HasBlob(context.Background(), "chunks/"+path) {
						atomic.AddUint32(blocksCopied, 1)
						continue
					}

					block, err := golongtail.ReadFromStorage(fs, contentPath, path)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to read block: `%s`, %v", path, err)
						break
					}

					err = workerStore.PutBlob(context.Background(), "chunks/"+path, "application/octet-stream", block[:])
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to write block: `%s`, %v", path, err)
						continue
					}
					atomic.AddUint32(blocksCopied, 1)
				}
				wg.Done()
			}(start, end, &blocksCopied)
		}

		wg.Wait()
		missingCount = blockCount - blocksCopied

		if missingCount == 0 {
			break
		}
		incompleteCount = missingCount
		fmt.Fprintf(os.Stderr, "Retrying to copy %d blocks to `%s`", incompleteCount, s.url)
	}
	if progressFunc != nil {
		atomic.AddUint32(&blocksCopied, missingCount)
		pg.Wait()
	}

	if missingCount > 0 {
		return fmt.Errorf("Failed to copy %d blocks from `%s`", missingCount, s)
	}

	hash, err := createHashAPIFromIdentifier(contentIndex.GetHashAPI())
	if err != nil {
		return errors.Wrap(err, s.String())
	}
	defer hash.Dispose()

	storeBlob, err := golongtail.WriteContentIndexToBuffer(contentIndex)
	if err != nil {
		return errors.Wrap(err, s.String())
	}
	objHandle := s.bucket.Object("store.lci")
	for {
		writeCondition := storage.Conditions{DoesNotExist: true}
		objAttrs, _ := objHandle.Attrs(ctx)
		if objAttrs != nil {
			writeCondition = storage.Conditions{GenerationMatch: objAttrs.Generation}
			reader, err := objHandle.If(writeCondition).NewReader(ctx)
			if err != nil {
				return errors.Wrap(err, s.String())
			}
			if reader == nil {
				continue
			}
			blob, err := ioutil.ReadAll(reader)
			reader.Close()
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
		}
		writer := objHandle.If(writeCondition).NewWriter(ctx)
		if writer == nil {
			continue
		}
		_, err = writer.Write(storeBlob)
		if err != nil {
			writer.CloseWithError(err)
			return errors.Wrap(err, s.String())
		}
		writer.Close()
		_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: "application/octet-stream"})
		if err != nil {
			return errors.Wrap(err, s.String())
		}
		break
	}
	return nil
}

// GetContent ...
func (s GCSBlobStore) GetContent(
	ctx context.Context,
	progressFunc golongtail.ProgressFunc,
	progressContext interface{},
	contentIndex golongtail.Longtail_ContentIndex,
	fs golongtail.Longtail_StorageAPI,
	contentPath string) error {
	missingPaths, err := golongtail.GetPathsForContentBlocks(contentIndex)
	if err != nil {
		return err
	}

	// TODO: Not the best implementation, it should probably create about one worker per code and have separate connection
	// to GCS for each worker as You cant write to two objects to the same connection apparently
	blockCount := missingPaths.GetPathCount()
	workerCount := uint32(runtime.NumCPU() * 4) // Twice as many as cores - lots of waiting time
	if workerCount > blockCount {
		workerCount = blockCount
	}

	incompleteCount := blockCount
	var missingCount uint32
	var blocksCopied uint32

	var pg sync.WaitGroup
	if progressFunc != nil {
		pg.Add(int(1))
		go func() {
			gcsProgressProxy(progressFunc, progressContext, blockCount, &blocksCopied)
			pg.Done()
		}()
	}

	for retryCount := 0; retryCount < 5; retryCount++ {

		var wg sync.WaitGroup
		wg.Add(int(workerCount))

		for i := uint32(0); i < workerCount; i++ {
			start := (blockCount * i) / workerCount
			end := ((blockCount * (i + 1)) / workerCount)
			go func(start uint32, end uint32, blocksCopied *uint32) {
				workerStore, err := NewGCSBlobStore(s.url)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to connect to: `%s`, %v", s.url, err)
					wg.Done()
					return
				}
				defer workerStore.Close()

				for p := start; p < end; p++ {
					path := golongtail.GetPath(missingPaths, p)

					blockData, err := s.GetBlob(context.Background(), "chunks/"+path)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to read block: `%s`, %v", path, err)
						continue
					}

					err = golongtail.WriteToStorage(fs, contentPath, path, blockData)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Failed to store block: `%s`, %v", path, err)
						break
					}
					atomic.AddUint32(blocksCopied, 1)
				}
				wg.Done()
			}(start, end, &blocksCopied)
		}

		wg.Wait()
		missingCount := blockCount - blocksCopied

		if missingCount == 0 {
			break
		}
		incompleteCount = missingCount
		fmt.Fprintf(os.Stderr, "Retrying to copy %d blocks from `%s`", incompleteCount, s.url)
	}

	if progressFunc != nil {
		atomic.AddUint32(&blocksCopied, missingCount)
		pg.Wait()
	}

	if missingCount > 0 {
		return fmt.Errorf("Failed to copy %d blocks to `%s`", missingCount, s)
	}

	return nil
}
