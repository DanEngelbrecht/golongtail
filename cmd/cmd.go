package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path"
	"runtime"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"

	"github.com/DanEngelbrecht/golongtail/longtail"
)

type loggerData struct {
}

func logger(context interface{}, level int, message string) {
	switch level {
	case 0:
		log.Printf("DEBUG: %s", message)
	case 1:
		log.Printf("INFO: %s", message)
	case 2:
		log.Printf("WARNING: %s", message)
	case 3:
		log.Fatal(message)
	}
}

type progressData struct {
	inited     bool
	oldPercent int
	task       string
}

func progress(context interface{}, total int, current int) {
	p := context.(*progressData)
	if current < total {
		if !p.inited {
			log.Printf("%s: ", p.task)
			p.inited = true
		}
		percentDone := (100 * current) / total
		if (percentDone - p.oldPercent) >= 5 {
			log.Printf("%d%% ", percentDone)
			p.oldPercent = percentDone
		}
		return
	}
	if p.inited {
		if p.oldPercent != 100 {
			log.Printf("100%%")
		}
		log.Printf(" Done\n")
	}
}

// GCSStoreBase is the base object for all chunk and index stores with GCS backing
type GCSStoreBase struct {
	Location string
	client   *storage.Client
	bucket   *storage.BucketHandle
}

func (s GCSStoreBase) String() string {
	return s.Location
}

// NewGCSStoreBase initializes a base object used for chunk or index stores backed by GCS.
func NewGCSStoreBase(u *url.URL) (GCSStoreBase, error) {
	var err error
	s := GCSStoreBase{Location: u.String()}
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
func (s GCSStoreBase) Close() error { return nil }

func (s GCSStoreBase) hasObjectBlob(ctx context.Context, key string) bool {
	objHandle := s.bucket.Object(key)
	_, err := objHandle.Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return false
	}
	return true
}

func (s GCSStoreBase) putObjectBlob(ctx context.Context, key string, contentType string, blob []byte) error {
	objHandle := s.bucket.Object(key)
	objWriter := objHandle.NewWriter(ctx)

	_, err := objWriter.Write(blob)
	if err != nil {
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

func (s GCSStoreBase) getObjectBlob(ctx context.Context, key string) ([]byte, error) {
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

func main() {
	l := longtail.SetLogger(logger, &loggerData{})
	defer longtail.ClearLogger(l)
	longtail.SetLogLevel(0)

	fs := longtail.CreateFSStorageAPI()
	defer fs.Dispose()
	hash := longtail.CreateMeowHashAPI()
	defer hash.Dispose()
	jobs := longtail.CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobs.Dispose()
	creg := longtail.CreateDefaultCompressionRegistry()
	defer creg.Dispose()

	gcsBucket := "gs://test_block_storage"
	loc, err := url.Parse(gcsBucket)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Connecting to `%s`\n", gcsBucket)
	indexStore, err := NewGCSStoreBase(loc)
	if err != nil {
		log.Fatal(err)
	}
	defer indexStore.Close()

	//	version := "75a99408249875e875f8fba52b75ea0f5f12a00e"
	version := "916600e1ecb9da13f75835cd1b2d2e6a67f1a92d"

	versionPath := "C:/Temp/longtail/local/WinEditor/git" + version + "_Win64_Editor"
	contentPath := "C:/Temp/longtail/local/chunks"

	fmt.Printf("Indexing files and folders in `%s`\n", versionPath)
	fileInfos, err := longtail.GetFilesRecursively(fs, versionPath)
	if err != nil {
		log.Fatal(err)
	}
	defer fileInfos.Dispose()

	pathCount := fileInfos.GetFileCount()
	fmt.Printf("Found %d assets\n", pathCount)

	compressionType := longtail.GetLizardDefaultCompressionType()
	compressionTypes := make([]uint32, pathCount)
	for i := uint32(0); i < pathCount; i++ {
		compressionTypes[i] = compressionType
	}

	fmt.Printf("Indexing `%s`\n", versionPath)
	vindex, err := longtail.CreateVersionIndex(
		fs,
		hash,
		jobs,
		progress,
		&progressData{task: "Indexing version"},
		versionPath,
		fileInfos.GetPaths(),
		fileInfos.GetFileSizes(),
		compressionTypes,
		32758)
	if err != nil {
		log.Fatal(err)
	}
	defer vindex.Dispose()

	versionTempPath := path.Join(os.TempDir(), "version.lvi")
	err = longtail.WriteVersionIndex(fs, vindex, versionTempPath)
	if err != nil {
		log.Fatal(err)
	}

	targetBlockSize := uint32(32758 * 12)
	maxChunksPerBlock := uint32(1024)

	var remoteContentIndex longtail.Longtail_ContentIndex
	remoteContentIndexBlob, err := indexStore.getObjectBlob(context.Background(), "store.lci")
	if err == nil {
		storeTempPath := path.Join(os.TempDir(), "store.lci")
		// TODO: Would be nicer if the longtail api provided a way to read/write indexes directly from a memory chunk...
		err = ioutil.WriteFile(storeTempPath, remoteContentIndexBlob, 0644)
		if err != nil {
			log.Fatal(err)
		}
		remoteContentIndex, err = longtail.ReadContentIndex(fs, storeTempPath)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		remoteContentIndex, err = longtail.CreateContentIndex(
			hash,
			0,
			nil,
			nil,
			nil,
			0,
			0)
		if err != nil {
			log.Fatal(err)
		}
	}

	missingContentIndex, err := longtail.CreateMissingContent(
		hash,
		remoteContentIndex,
		vindex,
		targetBlockSize,
		maxChunksPerBlock)
	if err != nil {
		log.Fatal(err)
	}
	defer missingContentIndex.Dispose()

	err = longtail.WriteContent(
		fs,
		fs,
		creg,
		jobs,
		progress,
		&progressData{task: "Writing content blocks"},
		missingContentIndex,
		vindex,
		versionPath,
		contentPath)

	missingPaths, err := longtail.GetPathsForContentBlocks(missingContentIndex)
	if err != nil {
		log.Fatal(err)
	}

	// TODO: Not the best implementation, it should probably create about one worker per code and have separate connection
	// to GCS for each worker as You cant write to two objects to the same connection apparently
	blockCount := missingPaths.GetPathCount()
	var wg sync.WaitGroup
	wg.Add(int(blockCount))
	var mux sync.Mutex
	for i := uint32(0); i < blockCount; i++ {
		go func(i uint32) {
			path := longtail.GetPath(missingPaths, i)

			if indexStore.hasObjectBlob(context.Background(), "chunks/"+path) {
				wg.Done()
				return
			}

			block, err := longtail.ReadFromStorage(fs, contentPath, path)
			if err != nil {
				log.Printf("Failed to read block: `%s`, %v", path, err)
				wg.Done()
				return
			}

			mux.Lock()
			err = indexStore.putObjectBlob(context.Background(), "chunks/"+path, "application/octet-stream", block)
			mux.Unlock()
			if err != nil {
				log.Printf("Failed to write block: `%s`, %v", path, err)
				wg.Done()
				return
			}
			log.Printf("Copied block: `%s` from `%s` to `%s`", path, contentPath, "chunks")
			wg.Done()
		}(i)
	}
	wg.Wait()

	if true {
		// TODO: Would be nicer if the longtail api provided a way to read/write indexes directly from a memory chunk...
		vindexFileData, err := ioutil.ReadFile(versionTempPath)
		if err != nil {
			log.Fatal(err)
		}

		err = indexStore.putObjectBlob(context.Background(), "index/"+version+".lvi", "application/octet-stream", vindexFileData)
		if err != nil {
			log.Fatal(err)
		}
	}

	newContentIndex, err := longtail.MergeContentIndex(missingContentIndex, remoteContentIndex)
	if err != nil {
		log.Fatal(err)
	}
	defer newContentIndex.Dispose()

	// TODO: Would be nicer if the longtail api provided a way to read/write indexes directly from a memory chunk...
	storeTempPath := path.Join(os.TempDir(), "store.lci")
	err = longtail.WriteContentIndex(fs, newContentIndex, storeTempPath)
	if err != nil {
		log.Fatal(err)
	}

	newContentIndexData, err := ioutil.ReadFile(storeTempPath)
	if err != nil {
		log.Fatal(err)
	}

	err = indexStore.putObjectBlob(context.Background(), "store.lci", "application/octet-stream", newContentIndexData)
	if err != nil {
		log.Fatal(err)
	}

	/*
		blob := "0000/000022a7d087269b03956429b1c2278de308e06791a62e39a6b17dbda78ea32a.cacnk"
		fmt.Printf("Fetching blob `%s`\n", blob)
		data, err := indexStore.getObjectBlob(context.Background(), blob)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Block `%s` is %d bytes\n", blob, len(data))

		testBlob := "xxxx/remove_me.cacnk"
		err = indexStore.putObjectBlob(context.Background(), testBlob, "application/octet-stream", data)
		if err != nil {
			log.Fatal(err)
		}
	*/
}
