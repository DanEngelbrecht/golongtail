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
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"

	"github.com/DanEngelbrecht/golongtail/longtail"
	"gopkg.in/alecthomas/kingpin.v2"
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
			fmt.Printf("%s: ", p.task)
			p.inited = true
		}
		percentDone := (100 * current) / total
		if (percentDone - p.oldPercent) >= 5 {
			fmt.Printf("%d%% ", percentDone)
			p.oldPercent = percentDone
		}
		return
	}
	if p.inited {
		if p.oldPercent != 100 {
			fmt.Printf("100%%")
		}
		fmt.Printf(" Done\n")
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

// TODO: Would be nicer if the longtail api provided a way to read/write indexes directly from a memory chunk...
func (s GCSStoreBase) storeTempObjectBlob(ctx context.Context, key string, fs longtail.Longtail_StorageAPI) (tempPath string, err error) {
	remoteBlob, err := s.getObjectBlob(context.Background(), key)
	if err != nil {
		return "", err
	}
	tmpName := strings.Replace(key, "/", "_", -1)
	tmpName = strings.Replace(tmpName, "\\", "_", -1)
	tmpName = strings.Replace(tmpName, ":", "_", -1)
	storeTempPath := path.Join(os.TempDir(), "longtail_"+tmpName+".tmp")
	err = ioutil.WriteFile(storeTempPath, remoteBlob, 0644)
	if err != nil {
		return "", err
	}
	return storeTempPath, nil
}

func trace(s string) (string, time.Time) {
	return s, time.Now()
}

func un(s string, startTime time.Time) {
	elapsed := time.Since(startTime)
	log.Printf("trace end: %s, elapsed %f secs\n", s, elapsed.Seconds())
}

func upSyncVersion(
	gcsBucket string,
	sourceFolderPath string,
	targetFilePath string,
	localCachePath string,
	targetChunkSize uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32) error {
	defer un(trace("upSyncVersion " + targetFilePath))
	fs := longtail.CreateFSStorageAPI()
	defer fs.Dispose()
	hash := longtail.CreateMeowHashAPI()
	defer hash.Dispose()
	jobs := longtail.CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobs.Dispose()
	creg := longtail.CreateDefaultCompressionRegistry()
	defer creg.Dispose()

	loc, err := url.Parse(gcsBucket)
	if err != nil {
		return err
	}

	fmt.Printf("Connecting to `%s`\n", gcsBucket)
	indexStore, err := NewGCSStoreBase(loc)
	if err != nil {
		return err
	}
	defer indexStore.Close()

	fmt.Printf("Indexing files and folders in `%s`\n", sourceFolderPath)
	fileInfos, err := longtail.GetFilesRecursively(fs, sourceFolderPath)
	if err != nil {
		return err
	}
	defer fileInfos.Dispose()

	pathCount := fileInfos.GetFileCount()
	fmt.Printf("Found %d assets\n", int(pathCount))

	compressionType := longtail.GetLizardDefaultCompressionType()
	compressionTypes := make([]uint32, pathCount)
	for i := uint32(0); i < pathCount; i++ {
		compressionTypes[i] = compressionType
	}

	fmt.Printf("Indexing `%s`\n", sourceFolderPath)
	vindex, err := longtail.CreateVersionIndex(
		fs,
		hash,
		jobs,
		progress,
		&progressData{task: "Indexing version"},
		sourceFolderPath,
		fileInfos.GetPaths(),
		fileInfos.GetFileSizes(),
		compressionTypes,
		targetChunkSize)
	if err != nil {
		return err
	}
	defer vindex.Dispose()

	versionTempPath := path.Join(os.TempDir(), "version.lvi")
	err = longtail.WriteVersionIndex(fs, vindex, versionTempPath)
	if err != nil {
		return err
	}

	var remoteContentIndex longtail.Longtail_ContentIndex
	remoteContentIndexTmpPath, err := indexStore.storeTempObjectBlob(context.Background(), "store.lci", fs)
	if err == nil {
		defer os.Remove(remoteContentIndexTmpPath)
		remoteContentIndex, err = longtail.ReadContentIndex(fs, remoteContentIndexTmpPath)
		if err != nil {
			return err
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
			return err
		}
	}

	missingContentIndex, err := longtail.CreateMissingContent(
		hash,
		remoteContentIndex,
		vindex,
		targetBlockSize,
		maxChunksPerBlock)
	if err != nil {
		return err
	}
	defer missingContentIndex.Dispose()

	if missingContentIndex.GetBlockCount() > 0 {
		err = longtail.WriteContent(
			fs,
			fs,
			creg,
			jobs,
			progress,
			&progressData{task: "Writing content blocks"},
			missingContentIndex,
			vindex,
			sourceFolderPath,
			localCachePath)

		missingPaths, err := longtail.GetPathsForContentBlocks(missingContentIndex)
		if err != nil {
			return err
		}

		// TODO: Not the best implementation, it should probably create about one worker per code and have separate connection
		// to GCS for each worker as You cant write to two objects to the same connection apparently
		blockCount := missingPaths.GetPathCount()
		fmt.Printf("Storing %d blocks to `%s`\n", int(blockCount), gcsBucket)
		workerCount := uint32(runtime.NumCPU())
		if workerCount > blockCount {
			workerCount = blockCount
		}
		// TODO: Refactor using channels and add proper progress
		var wg sync.WaitGroup
		wg.Add(int(workerCount))
		for i := uint32(0); i < workerCount; i++ {
			start := (blockCount * i) / workerCount
			end := ((blockCount * (i + 1)) / workerCount)
			//			fmt.Printf("Launching worker %d, to copy range %d to %d\n", i, start, end-1)
			go func(start uint32, end uint32) {
				workerIndexStore, err := NewGCSStoreBase(loc)
				if err != nil {
					log.Printf("Failed to connect to: `%s`, %v", gcsBucket, err)
					wg.Done()
					return
				}
				defer workerIndexStore.Close()

				for p := start; p < end; p++ {
					path := longtail.GetPath(missingPaths, p)

					if indexStore.hasObjectBlob(context.Background(), "chunks/"+path) {
						continue
					}

					block, err := longtail.ReadFromStorage(fs, localCachePath, path)
					if err != nil {
						log.Printf("Failed to read block: `%s`, %v", path, err)
						continue
					}

					err = indexStore.putObjectBlob(context.Background(), "chunks/"+path, "application/octet-stream", block[:])
					if err != nil {
						log.Printf("Failed to write block: `%s`, %v", path, err)
						continue
					}
					//					log.Printf("Copied block: `%s` from `%s` to `%s`", path, localCachePath, "chunks")
				}
				wg.Done()
			}(start, end)
		}
		wg.Wait()

		newContentIndex, err := longtail.MergeContentIndex(missingContentIndex, remoteContentIndex)
		if err != nil {
			return err
		}
		defer newContentIndex.Dispose()

		// TODO: Would be nicer if the longtail api provided a way to read/write indexes directly from a memory chunk...
		storeTempPath := path.Join(os.TempDir(), "store.lci")
		err = longtail.WriteContentIndex(fs, newContentIndex, storeTempPath)
		if err != nil {
			return err
		}

		newContentIndexData, err := ioutil.ReadFile(storeTempPath)
		if err != nil {
			return err
		}

		err = indexStore.putObjectBlob(context.Background(), "store.lci", "application/octet-stream", newContentIndexData)
		if err != nil {
			return err
		}
	}

	vindexFileData, err := ioutil.ReadFile(versionTempPath)
	if err != nil {
		return err
	}

	err = indexStore.putObjectBlob(context.Background(), targetFilePath, "application/octet-stream", vindexFileData)
	if err != nil {
		return err
	}

	return nil
}

func downSyncVersion(
	gcsBucket string,
	sourceFilePath string,
	targetFolderPath string,
	localCachePath string,
	targetChunkSize uint32,
	targetBlockSize uint32,
	maxChunksPerBlock uint32) error {
	defer un(trace("downSyncVersion " + sourceFilePath))
	fs := longtail.CreateFSStorageAPI()
	defer fs.Dispose()
	hash := longtail.CreateMeowHashAPI()
	defer hash.Dispose()
	jobs := longtail.CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobs.Dispose()
	creg := longtail.CreateDefaultCompressionRegistry()
	defer creg.Dispose()

	loc, err := url.Parse(gcsBucket)
	if err != nil {
		return err
	}

	fmt.Printf("Connecting to `%s`\n", gcsBucket)
	indexStore, err := NewGCSStoreBase(loc)
	if err != nil {
		return err
	}
	defer indexStore.Close()

	var remoteVersionIndex longtail.Longtail_VersionIndex

	remoteVersionTmpPath, err := indexStore.storeTempObjectBlob(context.Background(), sourceFilePath, fs)
	if err != nil {
		return err
	}
	defer os.Remove(remoteVersionTmpPath)
	remoteVersionIndex, err = longtail.ReadVersionIndex(fs, remoteVersionTmpPath)
	if err != nil {
		return err
	}

	fmt.Printf("Indexing files and folders in `%s`\n", targetFolderPath)
	fileInfos, err := longtail.GetFilesRecursively(fs, targetFolderPath)
	if err != nil {
		return err
	}
	defer fileInfos.Dispose()

	pathCount := fileInfos.GetFileCount()
	fmt.Printf("Found %d assets\n", int(pathCount))

	compressionType := longtail.GetLizardDefaultCompressionType()
	compressionTypes := make([]uint32, pathCount)
	for i := uint32(0); i < pathCount; i++ {
		compressionTypes[i] = compressionType
	}

	localVersionIndex, err := longtail.CreateVersionIndex(
		fs,
		hash,
		jobs,
		progress,
		&progressData{task: "Indexing version"},
		targetFolderPath,
		fileInfos.GetPaths(),
		fileInfos.GetFileSizes(),
		compressionTypes,
		targetChunkSize)
	if err != nil {
		return err
	}
	defer localVersionIndex.Dispose()

	localContentIndex, err := longtail.ReadContent(
		fs,
		hash,
		jobs,
		progress,
		&progressData{task: "Scanning local blocks"},
		localCachePath)
	if err != nil {
		return err
	}
	defer localContentIndex.Dispose()

	missingContentIndex, err := longtail.CreateMissingContent(
		hash,
		localContentIndex,
		remoteVersionIndex,
		targetBlockSize,
		maxChunksPerBlock)
	if err != nil {
		return err
	}
	defer missingContentIndex.Dispose()

	if missingContentIndex.GetBlockCount() > 0 {
		var remoteContentIndex longtail.Longtail_ContentIndex
		remoteContentIndexTmpPath, err := indexStore.storeTempObjectBlob(context.Background(), "store.lci", fs)
		if err == nil {
			defer os.Remove(remoteContentIndexTmpPath)
			remoteContentIndex, err = longtail.ReadContentIndex(fs, remoteContentIndexTmpPath)
			if err != nil {
				return err
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
				return err
			}
		}

		neededContentIndex, err := longtail.RetargetContent(remoteContentIndex, missingContentIndex)
		if err != nil {
			return err
		}
		defer neededContentIndex.Dispose()

		missingPaths, err := longtail.GetPathsForContentBlocks(neededContentIndex)
		if err != nil {
			return err
		}

		// TODO: Not the best implementation, it should probably create about one worker per code and have separate connection
		// to GCS for each worker as You cant write to two objects to the same connection apparently
		blockCount := missingPaths.GetPathCount()
		fmt.Printf("Fetching %d blocks from `%s`\n", int(blockCount), gcsBucket)
		workerCount := uint32(runtime.NumCPU())
		if workerCount > blockCount {
			workerCount = blockCount
		}
		// TODO: Refactor using channels and add proper progress
		var wg sync.WaitGroup
		wg.Add(int(workerCount))
		for i := uint32(0); i < workerCount; i++ {
			start := (blockCount * i) / workerCount
			end := ((blockCount * (i + 1)) / workerCount)
			//			fmt.Printf("Launching worker %d, to copy range %d to %d\n", i, start, end-1)
			go func(start uint32, end uint32) {
				workerIndexStore, err := NewGCSStoreBase(loc)
				if err != nil {
					log.Printf("Failed to connect to: `%s`, %v", gcsBucket, err)
					wg.Done()
					return
				}
				defer workerIndexStore.Close()

				for p := start; p < end; p++ {
					path := longtail.GetPath(missingPaths, p)

					blockData, err := indexStore.getObjectBlob(context.Background(), "chunks/"+path)
					if err != nil {
						log.Printf("Failed to read block: `%s`, %v", path, err)
						continue
					}

					err = longtail.WriteToStorage(fs, localCachePath, path, blockData)
					if err != nil {
						log.Printf("Failed to store block: `%s`, %v", path, err)
						continue
					}
					//					log.Printf("Copied block: `%s` from `%s` to `%s`", path, "chunks", localCachePath)
				}
				wg.Done()
			}(start, end)
		}
		wg.Wait()

		mergedContentIndex, err := longtail.MergeContentIndex(localContentIndex, neededContentIndex)
		if err != nil {
			return err
		}
		localContentIndex.Dispose()
		localContentIndex = mergedContentIndex
	}

	versionDiff, err := longtail.CreateVersionDiff(localVersionIndex, remoteVersionIndex)
	if err != nil {
		return err
	}
	defer versionDiff.Dispose()

	err = longtail.ChangeVersion(
		fs,
		fs,
		hash,
		jobs,
		progress,
		&progressData{task: "Updating version"},
		creg,
		localContentIndex,
		localVersionIndex,
		remoteVersionIndex,
		versionDiff,
		localCachePath,
		targetFolderPath)
	if err != nil {
		return err
	}
	return nil
}

func parseLevel(lvl string) (int, error) {
	switch strings.ToLower(lvl) {
	case "debug":
		return 0, nil
	case "info":
		return 1, nil
	case "warn":
		return 2, nil
	case "error":
		return 3, nil
	case "off":
		return 4, nil
	}

	return -1, fmt.Errorf("not a valid log Level: %q", lvl)
}

var (
	logLevel          = kingpin.Flag("log-level", "Log level").Default("warn").Enum("debug", "info", "warn", "error")
	targetChunkSize   = kingpin.Flag("target-chunk-size", "Target chunk size").Default("32768").Uint32()
	targetBlockSize   = kingpin.Flag("target-block-size", "Target block size").Default("524288").Uint32()
	maxChunksPerBlock = kingpin.Flag("max-chunks-per-block", "Max chunks per block").Default("1024").Uint32()
	storageURI        = kingpin.Flag("storage-uri", "Storage URI (only GCS bucket URI supported)").String()

	commandUpSync     = kingpin.Command("upsync", "Upload a folder")
	upSyncContentPath = commandUpSync.Flag("content-path", "Location to store blocks prepared for upload").Default(path.Join(os.TempDir(), "longtail_block_store")).String()
	sourceFolderPath  = commandUpSync.Flag("source-path", "Source folder path").String()
	targetFilePath    = commandUpSync.Flag("target-path", "Target file path relative to --storage-uri").String()

	commandDownSync     = kingpin.Command("downsync", "Download a folder")
	downSyncContentPath = commandDownSync.Flag("content-path", "Location for downloaded/cached blocks").Default(path.Join(os.TempDir(), "longtail_block_store")).String()
	targetFolderPath    = commandDownSync.Flag("target-path", "Target folder path").String()
	sourceFilePath      = commandDownSync.Flag("source-path", "Source file path relative to --storage-uri").String()
)

func main() {
	kingpin.HelpFlag.Short('h')
	kingpin.CommandLine.DefaultEnvars()
	kingpin.Parse()

	logLevel, err := ParseLevel(*logLevel)
	if err != nil {
		log.Fatal(err)
	}

	l := longtail.SetLogger(logger, &loggerData{})
	defer longtail.ClearLogger(l)
	longtail.SetLogLevel(logLevel)

	switch kingpin.Parse() {
	case commandUpSync.FullCommand():
		err := upSyncVersion(*storageURI, *sourceFolderPath, *targetFilePath, *upSyncContentPath, *targetChunkSize, *targetBlockSize, *maxChunksPerBlock)
		if err != nil {
			log.Fatal(err)
		}
	case commandDownSync.FullCommand():
		err := downSyncVersion(*storageURI, *sourceFilePath, *targetFolderPath, *downSyncContentPath, *targetChunkSize, *targetBlockSize, *maxChunksPerBlock)
		if err != nil {
			log.Fatal(err)
		}
	}

	/*
		// var versions = [...]string{"75a99408249875e875f8fba52b75ea0f5f12a00e", "916600e1ecb9da13f75835cd1b2d2e6a67f1a92d", "b1d3adb4adce93d0f0aa27665a52be0ab0ee8b59"}
		// var versions = [...]string{"75a99408249875e875f8fba52b75ea0f5f12a00e", "916600e1ecb9da13f75835cd1b2d2e6a67f1a92d"} //, "b1d3adb4adce93d0f0aa27665a52be0ab0ee8b59"}
		// var versions = [...]string{"b1d3adb4adce93d0f0aa27665a52be0ab0ee8b59"}

		var versions = [...]string{
			"2f7f84a05fc290c717c8b5c0e59f8121481151e6",
			"916600e1ecb9da13f75835cd1b2d2e6a67f1a92d",
			"fdeb1390885c2f426700ca653433730d1ca78dab",
			"81cccf054b23a0b5a941612ef0a2a836b6e02fd6",
			"558af6b2a10d9ab5a267b219af4f795a17cc032f",
			"c2ae7edeab85d5b8b21c8c3a29c9361c9f957f0c"}

		if true {
			for _, version := range versions {
				sourceFolderPath := "C:/Temp/longtail/local/WinEditor/git" + version + "_Win64_Editor"
				targetFilePath := "index/" + version + ".lvi"
				err := upSyncVersion(*storageURI, *sourceFolderPath, *targetFilePath, *upSyncContentPath, targetChunkSize, targetBlockSize, maxChunksPerBlock)
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		if true {
			for _, version := range versions {
				targetFolderPath := "C:/Temp/longtail/local/WinEditor/current"
				sourceFilePath := "index/" + version + ".lvi"
				err := downSyncVersion(*storageURI, *sourceFilePath, *targetFolderPath, *downSyncContentPath, targetChunkSize, targetBlockSize, maxChunksPerBlock)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	*/
}
