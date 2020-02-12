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

	defaultHashAPI uint32

	contentIndexMux sync.Mutex
	contentIndex    lib.Longtail_ContentIndex
	knownBlocks     map[uint64]bool
	//	unsavedBlocks   map[uint64]lib.Longtail_BlockIndex

	// Fake it:
	backingStorage lib.Longtail_StorageAPI
}

func readContentIndex(storageAPI lib.Longtail_StorageAPI) (lib.Longtail_ContentIndex, int) {
	contentIndex, err := lib.ReadContentIndex(storageAPI, "fake_storage/store.lci")
	if err == nil {
		return contentIndex, 0
	} else {
		hashAPI := lib.CreateBlake3HashAPI()
		defer hashAPI.Dispose()
		contentIndex, _ := lib.CreateContentIndex(
			hashAPI,
			[]uint64{},
			[]uint32{},
			[]uint32{},
			32768,
			65536)
		return contentIndex, 0
	}
}

// String() ...
func (s *gcsBlockStore) String() string {
	return s.Location
}

// NewGCSBlockStore ...
func NewGCSBlockStore(u *url.URL, defaultHashAPI uint32) (lib.Longtail_BlockStoreAPI, error) {
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

	s := &gcsBlockStore{url: u, Location: u.String(), client: client, bucket: bucket, defaultHashAPI: defaultHashAPI, backingStorage: backingStorage}
	//	s.contentIndexMux = &sync.Mutex{}
	//	s.unsavedBlocks = &map[uint64]lib.Longtail_BlockIndex{}
	blockStoreAPI := lib.CreateBlockStoreAPI(s)
	contentIndex, _ := readContentIndex(backingStorage)
	s.contentIndex = contentIndex
	s.knownBlocks = make(map[uint64]bool)

	for b := uint64(0); b < contentIndex.GetBlockCount(); b++ {
		blockHash := contentIndex.GetBlockHash(b)
		s.knownBlocks[blockHash] = true
	}

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
func (s *gcsBlockStore) PutStoredBlock(storedBlock lib.Longtail_StoredBlock) int {
	blockIndex := storedBlock.GetBlockIndex()
	blockHash := blockIndex.GetBlockHash()
	key := getBlockPath("chunks", blockHash)
	ctx := context.Background()
	objHandle := s.bucket.Object(key)
	_, err := objHandle.Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		blockIndexBytes, err := lib.WriteBlockIndexToBuffer(storedBlock.GetBlockIndex())
		if err != nil {
			//return errors.Wrap(err, s.String()+"/"+key)
			return lib.ENOMEM
		}

		blockData := storedBlock.GetBlockData()
		blob := append(blockIndexBytes, blockData...)

		objWriter := objHandle.NewWriter(ctx)
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
	}

	s.contentIndexMux.Lock()
	defer s.contentIndexMux.Unlock()

	if _, ok := s.knownBlocks[blockHash]; ok {
		return 0
	}

	newBlocks := []lib.Longtail_BlockIndex{blockIndex}
	//	for _, v := range s.unsavedBlocks {
	//		newBlocks = append(newBlocks, blockIndex)
	//	}
	addedContentIndex, err := lib.CreateContentIndexFromBlocks(s.defaultHashAPI, uint64(len(newBlocks)), newBlocks)
	if err != nil {
		return lib.ENOMEM
	}
	defer addedContentIndex.Dispose()

	newContentIndex, err := lib.MergeContentIndex(s.contentIndex, addedContentIndex)
	if err != nil {
		return lib.ENOMEM
	}
	s.contentIndex.Dispose()
	s.contentIndex = newContentIndex

	s.knownBlocks[blockHash] = true
	return 0
}

func (s *gcsBlockStore) HasStoredBlock(blockHash uint64) int {
	s.contentIndexMux.Lock()
	defer s.contentIndexMux.Unlock()
	if _, ok := s.knownBlocks[blockHash]; ok {
		return 0
	}
	return int(lib.ENOENT)
}

// GetStoredBlock ...
func (s *gcsBlockStore) GetStoredBlock(blockHash uint64) (lib.Longtail_StoredBlock, int) {
	key := getBlockPath("chunks", blockHash)
	ctx := context.Background()
	objHandle := s.bucket.Object(key)
	obj, err := objHandle.NewReader(ctx)
	if err != nil {
		//return nil, errors.Wrap(err, s.String()+"/"+key)
		return lib.Longtail_StoredBlock{}, lib.ENOENT
	}
	defer obj.Close()

	storedBlockData, err := ioutil.ReadAll(obj)

	if err != nil {
		//		return nil, err
		return lib.Longtail_StoredBlock{}, lib.EIO
	}

	storedBlock, err := lib.InitStoredBlockFromData(storedBlockData)
	if err != nil {
		return lib.Longtail_StoredBlock{}, lib.ENOMEM
	}
	return storedBlock, 0
}

// GetIndex ...
func (s *gcsBlockStore) GetIndex(defaultHashAPIIdentifier uint32, jobAPI lib.Longtail_JobAPI, progress lib.Longtail_ProgressAPI) (lib.Longtail_ContentIndex, int) {
	buf, err := lib.WriteContentIndexToBuffer(s.contentIndex)
	if err != nil {
		return lib.Longtail_ContentIndex{}, lib.ENOMEM
	}
	copyContentIndex, err := lib.ReadContentIndexFromBuffer(buf)
	if err != nil {
		return lib.Longtail_ContentIndex{}, lib.ENOMEM
	}
	return copyContentIndex, 0
}

// GetStoredBlockPath ...
func (s *gcsBlockStore) GetStoredBlockPath(blockHash uint64) (string, int) {
	return getBlockPath("chunks", blockHash), 0
}

// Close ...
func (s *gcsBlockStore) Close() {
	s.contentIndexMux.Lock()
	defer s.contentIndexMux.Unlock()
	_ = lib.WriteContentIndex(s.backingStorage, s.contentIndex, "fake_storage/store.lci")
	s.contentIndex.Dispose()
	//
	//	unsavedBlockCount := len(s.unsavedBlocks)
	//	if unsavedBlockCount == 0 {
	//		return
	//	}
	//
	//	contentIndex, err := lib.ReadContentIndex(s.backingStorage, "fake_storage/store.lci")
	//	if err != nil {
	//		return
	//	}
	//
	//	newBlocks := []lib.Longtail_BlockIndex{}
	//	for _, v := range s.unsavedBlocks {
	//		newBlocks = append(newBlocks, v)
	//	}
	//
	//	newContentIndex, err := lib.CreateContentIndexFromBlocks(contentIndex.GetHashAPI(), uint64(len(newBlocks)), newBlocks)
	//	if err != nil {
	//		return
	//	}
	//	contentIndex, err = lib.MergeContentIndex(contentIndex, newContentIndex)
	//	if err != nil {
	//		return
	//	}
}
