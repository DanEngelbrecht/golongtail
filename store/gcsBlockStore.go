package store

import (
	//	"context"
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/DanEngelbrecht/golongtail/lib"
	"github.com/pkg/errors"
	//	"github.com/pkg/errors"
)

type gcsBlockStore struct {
	url      *url.URL
	Location string
	client   *storage.Client
	bucket   *storage.BucketHandle

	contentIndexMux *sync.Mutex
	unsavedBlocks   *map[uint64]lib.Longtail_BlockIndex

	// Fake it:
	backingStorage    *lib.Longtail_StorageAPI
	backingBlockStore *lib.Longtail_BlockStoreAPI
}

// String() ...
func (s gcsBlockStore) String() string {
	return s.Location
}

// NewGCSBlockStore ...
func NewGCSBlockStore(u *url.URL) (lib.Longtail_BlockStoreAPI, error) {
	//	var err error
	if u.Scheme != "gs" {
		return lib.Longtail_BlockStoreAPI{}, fmt.Errorf("invalid scheme '%s', expected 'gs'", u.Scheme)
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return lib.Longtail_BlockStoreAPI{}, errors.Wrap(err, u.String())
	}

	bucketName := u.Host
	bucket := client.Bucket(bucketName)

	backingStorage := lib.CreateFSStorageAPI()
	backingBlockStore := lib.CreateFSBlockStore(backingStorage, "fake_remote_store")

	s := gcsBlockStore{url: u, Location: u.String(), client: client, bucket: bucket, backingStorage: &backingStorage, backingBlockStore: &backingBlockStore}
	s.contentIndexMux = &sync.Mutex{}
	s.unsavedBlocks = &map[uint64]lib.Longtail_BlockIndex{}
	blockStoreAPI := lib.CreateBlockStoreAPI(s)
	return blockStoreAPI, nil
}

func getBlockPath(basePath string, blockHash uint64) string {
	sID := fmt.Sprintf("%x", blockHash)
	dir := filepath.Join(basePath, sID[0:4])
	name := filepath.Join(dir, sID) + ".lsb"
	name = strings.Replace(name, "\\", "/", -1)
	return name
}

// PutStoredBlock ...
func (s gcsBlockStore) PutStoredBlock(storedBlock lib.Longtail_StoredBlock) int {
	blockIndex := storedBlock.GetBlockIndex()
	blockHash := blockIndex.GetBlockHash()
	key := getBlockPath("chunks", blockHash)
	ctx := context.Background()
	objHandle := s.bucket.Object(key)
	_, err := objHandle.Attrs(ctx)
	if err != storage.ErrObjectNotExist {
		return 0
	}

	objWriter := objHandle.NewWriter(ctx)

	blockIndexBytes, err := lib.WriteBlockIndexToBuffer(storedBlock.GetBlockIndex())
	if err != nil {
		objWriter.Close()
		//return errors.Wrap(err, s.String()+"/"+key)
		return lib.ENOMEM
	}

	blockData := storedBlock.GetBlockData()
	blob := append(blockIndexBytes, blockData...)

	_, err = objWriter.Write(blob)
	if err != nil {
		objWriter.Close()
		//		return errors.Wrap(err, s.String()+"/"+key)
		return lib.EIO
	}

	err = objWriter.Close()
	if err != nil {
		//		return errors.Wrap(err, s.String()+"/"+key)
		return lib.EIO
	}

	_, err = objHandle.Update(ctx, storage.ObjectAttrsToUpdate{ContentType: "application/octet-stream"})
	if err != nil {
		return lib.EIO
	}

	copyBlockIndex, err := lib.ReadBlockIndexFromBuffer(blockIndexBytes)
	if err != nil {
		return lib.ENOMEM
	}
	s.contentIndexMux.Lock()
	defer s.contentIndexMux.Unlock()
	(*s.unsavedBlocks)[blockHash] = copyBlockIndex
	return 0
}

// GetStoredBlock ...
func (s gcsBlockStore) GetStoredBlock(blockHash uint64) (lib.Longtail_StoredBlock, int) {
	key := getBlockPath("chunks", blockHash)
	ctx := context.Background()
	objHandle := s.bucket.Object(key)
	obj, err := objHandle.NewReader(ctx)
	if err != nil {
		//return nil, errors.Wrap(err, s.String()+"/"+key)
		return lib.Longtail_StoredBlock{}, lib.ENOENT
	}
	defer obj.Close()

	b, err := ioutil.ReadAll(obj)

	if err != nil {
		//		return nil, err
		return lib.Longtail_StoredBlock{}, lib.EIO
	}

	blockIndex, err := lib.ReadBlockIndexFromBuffer(b)
	if err != nil {
		return lib.Longtail_StoredBlock{}, lib.EBADF
	}

	storedBlock, err := lib.CreateStoredBlock(
		blockIndex.GetBlockHash(),
		blockIndex.GetCompressionType(),
		blockIndex.GetChunkHashes(),
		blockIndex.GetChunkSizes(),
		b,
		true)
	if err != nil {
		return lib.Longtail_StoredBlock{}, lib.ENOMEM
	}
	return storedBlock, 0
}

// GetIndex ...
func (s gcsBlockStore) GetIndex(defaultHashAPIIdentifier uint32, jobAPI lib.Longtail_JobAPI, progress lib.Longtail_ProgressAPI) (lib.Longtail_ContentIndex, int) {
	contentIndex, err := s.backingBlockStore.GetIndex(defaultHashAPIIdentifier, jobAPI, &progress)
	if err == nil {
		return contentIndex, 0
	}
	return lib.Longtail_ContentIndex{}, 2
}

// GetStoredBlockPath ...
func (s gcsBlockStore) GetStoredBlockPath(blockHash uint64) (string, int) {
	return getBlockPath("chunks", blockHash), 0
}

// Close ...
func (s gcsBlockStore) Close() {
	s.contentIndexMux.Lock()
	defer s.contentIndexMux.Unlock()

	if len(*s.unsavedBlocks) == 0 {
		return
	}

	contentIndex, err := s.backingBlockStore.GetIndex(defaultHashAPIIdentifier, jobAPI, &progress)
	if err != nil {
		return
	}

	newBlocks := []lib.Longtail_StoredBlock{}
	for k, v := range *s.unsavedBlocks {
		newBlocks = append(newBlocks, v)
	}

	newContentIndex, err := lib.CreateContentIndexFromBlocks(contentIndex.GetHashAPI(), uint64(len(newBlocks)), newBlocks)
	if err != nil {
		return
	}
	contentIndex, err = lib.MergeContentIndex(contentIndex, newContentIndex)
	if err != nil {
		return
	}
}
