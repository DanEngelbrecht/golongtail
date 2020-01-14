package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"runtime"
	"net/url"
	"sync"

	"github.com/DanEngelbrecht/golongtail/longtail"
	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
)

// GCSBlobStore is the base object for all chunk and index stores with GCS backing
type GCSBlobStore struct {
	url *url.URL
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
		return errors.Wrap(err, s.String())
	}

	err = objWriter.Close()
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: contentType})
	if err != nil {
		return errors.Wrap(err, s.String())
	}

	return nil
}

// GetObjectBlob ...
func (s GCSBlobStore) GetBlob(ctx context.Context, key string) ([]byte, error) {
	objHandle := s.bucket.Object(key)
	obj, err := objHandle.NewReader(ctx)
	if err != nil {
		return nil, errors.Wrap(err, s.String())
	}
	defer obj.Close()

	b, err := ioutil.ReadAll(obj)

	if err != nil {
		return nil, err
	}

	return b, nil
}

// PutContent ...
func (s GCSBlobStore) PutContent(ctx context.Context, contentIndex longtail.Longtail_ContentIndex, fs longtail.Longtail_StorageAPI, contentPath string) error {
	paths, err := longtail.GetPathsForContentBlocks(contentIndex)
	if err != nil {
		return err
	}

	// TODO: Not the best implementation, it should probably create about one worker per code and have separate connection
	// to GCS for each worker as You cant write to two objects to the same connection apparently
	blockCount := paths.GetPathCount()
	log.Printf("Storing %d blocks to `%s`\n", int(blockCount), s)
	workerCount := uint32(runtime.NumCPU() * 4) // Twice as many as cores - lots of waiting time
	if workerCount > blockCount {
		workerCount = blockCount
	}
	// TODO: Refactor using channels and add proper progress
	var wg sync.WaitGroup
	wg.Add(int(workerCount))
	for i := uint32(0); i < workerCount; i++ {
		start := (blockCount * i) / workerCount
		end := ((blockCount * (i + 1)) / workerCount)
		go func(start uint32, end uint32) {

			workerStore, err := NewGCSBlobStore(s.url)
			if err != nil {
				log.Printf("Failed to connect to: `%s`, %v", s.url, err)
				wg.Done()
				return
			}
			defer workerStore.Close()

			for p := start; p < end; p++ {
				path := longtail.GetPath(paths, p)

				if workerStore.HasBlob(context.Background(), "chunks/"+path) {
					continue
				}

				block, err := longtail.ReadFromStorage(fs, contentPath, path)
				if err != nil {
					log.Printf("Failed to read block: `%s`, %v", path, err)
					continue
				}

				err = workerStore.PutBlob(context.Background(), "chunks/"+path, "application/octet-stream", block[:])
				if err != nil {
					log.Printf("Failed to write block: `%s`, %v", path, err)
					continue
				}
			}
			wg.Done()
		}(start, end)
	}
	wg.Wait()

	hash, err := createHashAPIFromIdentifier(contentIndex.GetHashAPI())
	if err != nil {
		return errors.Wrap(err, s.String())
	}
	defer hash.Dispose()

	storeBlob, err := longtail.WriteContentIndexToBuffer(contentIndex)
	if err != nil {
		return errors.Wrap(err, s.String())
	}
	objHandle := s.bucket.Object("index/store.lci")
	for {
		writeCondition := storage.Conditions{DoesNotExist: true}
		objAttrs, err := objHandle.Attrs(ctx)
		if err == nil {
			writeCondition = storage.Conditions{GenerationMatch: objAttrs.Generation}
			reader, err := objHandle.If(writeCondition).NewReader(ctx)
			if err != nil {
				return errors.Wrap(err, s.String())
			}
			if reader == nil {
				continue
			}
			defer reader.Close()
			blob, err := ioutil.ReadAll(reader)
			if err != nil {
				return errors.Wrap(err, s.String())
			}

			remoteContentIndex, err := longtail.ReadContentIndexFromBuffer(blob)
			if err != nil {
				return errors.Wrap(err, s.String())
			}
			defer remoteContentIndex.Dispose()
			mergedContentIndex, err := longtail.MergeContentIndex(remoteContentIndex, contentIndex)
			if err != nil {
				return errors.Wrap(err, s.String())
			}
			defer mergedContentIndex.Dispose()

			storeBlob, err = longtail.WriteContentIndexToBuffer(mergedContentIndex)
			if err != nil {
				return errors.Wrap(err, s.String())
			}
		} else if err != storage.ErrObjectNotExist {
			return errors.Wrap(err, s.String())
		}
		writer := objHandle.If(writeCondition).NewWriter(ctx)
		if writer == nil {
			continue
		}
		_, err = writer.Write(storeBlob)
		if err != nil {
			return errors.Wrap(err, s.String())
		}
		break
	}
	return nil
}

// GetContent ...
func (s GCSBlobStore) GetContent(ctx context.Context, contentIndex longtail.Longtail_ContentIndex, fs longtail.Longtail_StorageAPI, contentPath string) (error) {
	missingPaths, err := longtail.GetPathsForContentBlocks(contentIndex)
	if err != nil {
		return err
	}

	// TODO: Not the best implementation, it should probably create about one worker per code and have separate connection
	// to GCS for each worker as You cant write to two objects to the same connection apparently
	blockCount := missingPaths.GetPathCount()
	log.Printf("Fetching %d blocks from `%s`\n", int(blockCount), s)
	workerCount := uint32(runtime.NumCPU() * 4) // Twice as many as cores - lots of waiting time
	if workerCount > blockCount {
		workerCount = blockCount
	}
	// TODO: Refactor using channels and add proper progress
	var wg sync.WaitGroup
	wg.Add(int(workerCount))
	for i := uint32(0); i < workerCount; i++ {
		start := (blockCount * i) / workerCount
		end := ((blockCount * (i + 1)) / workerCount)
		go func(start uint32, end uint32) {
			workerStore, err := NewGCSBlobStore(s.url)
			if err != nil {
				log.Printf("Failed to connect to: `%s`, %v", s.url, err)
				wg.Done()
				return
			}
			defer workerStore.Close()

			for p := start; p < end; p++ {
				path := longtail.GetPath(missingPaths, p)

				blockData, err := s.GetBlob(context.Background(), "chunks/"+path)
				if err != nil {
					log.Printf("Failed to read block: `%s`, %v", path, err)
					continue
				}

				err = longtail.WriteToStorage(fs, contentPath, path, blockData)
				if err != nil {
					log.Printf("Failed to store block: `%s`, %v", path, err)
					continue
				}
				//					log.Printf("Copied block: `%s` from `%s` to `%s`", path, "chunks", contentPath)
			}
			wg.Done()
		}(start, end)
	}
	wg.Wait()

	return nil
}
