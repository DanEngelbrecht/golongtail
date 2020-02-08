package lib

import (
	"fmt"
	"runtime"
	"testing"
)

type progressData struct {
	inited     bool
	oldPercent int
	task       string
	t          *testing.T
}

func progress(context interface{}, total int, current int) {
	p := context.(*progressData)
	if current < total {
		if !p.inited {
			p.t.Logf("%s: ", p.task)
			p.inited = true
		}
		percentDone := (100 * current) / total
		if (percentDone - p.oldPercent) >= 5 {
			p.t.Logf("%d%% ", percentDone)
			p.oldPercent = percentDone
		}
		return
	}
	if p.inited {
		if p.oldPercent != 100 {
			p.t.Logf("100%%")
		}
		p.t.Logf(" Done\n")
	}
}

type loggerData struct {
	t *testing.T
}

func logger(context interface{}, level int, log string) {
	p := context.(*loggerData)
	p.t.Logf("%d: %s", level, log)
	//	fmt.Printf("%d: %s\n", level, log)
}

type assertData struct {
	t *testing.T
}

func testAssertFunc(context interface{}, expression string, file string, line int) {
	fmt.Printf("ASSERT: %s %s:%d", expression, file, line)
}

func TestDebugging(t *testing.T) {
	l := SetLogger(logger, &loggerData{t: t})
	defer ClearLogger(l)
	SetAssert(testAssertFunc, &assertData{t: t})
	defer ClearAssert()
	SetLogLevel(3)
}

func TestInMemStorage(t *testing.T) {
	storageAPI := CreateInMemStorageAPI()
	defer storageAPI.Dispose()
	myString := "my string"
	err := storageAPI.WriteToStorage("folder", "file", []byte(myString))
	expected := error(nil)
	if err != nil {
		t.Errorf("WriteToStorage() %q != %q", err, expected)
	}

	rbytes, err := storageAPI.ReadFromStorage("folder", "file")
	testString := string(rbytes)
	if myString != myString {
		t.Errorf("ReadFromStorage() %s != %s", rbytes, testString)
	}
}

func TestAPICreate(t *testing.T) {
	blake2 := CreateBlake2HashAPI()
	defer blake2.Dispose()

	blake3 := CreateBlake3HashAPI()
	defer blake3.Dispose()

	meow := CreateMeowHashAPI()
	defer meow.Dispose()

	lz4 := CreateLZ4CompressionAPI()
	defer lz4.Dispose()

	brotli := CreateBrotliCompressionAPI()
	defer brotli.Dispose()

	zstd := CreateZStdCompressionAPI()
	defer zstd.Dispose()

	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()

	compressionRegistry := CreateDefaultCompressionRegistry()
	defer compressionRegistry.Dispose()
}

func TestStoredblock(t *testing.T) {
	blockHash := uint64(0xdeadbeef500177aa)
	chunkHashes := []uint64{0x10, 0x20}
	chunkSizes := []uint32{10, 20}
	blockData := [30]uint8{}
	for index, _ := range blockData {
		blockData[index] = uint8(index + 1)
	}

	storedBlock, err := CreateStoredBlock(
		blockHash,
		0,
		chunkHashes,
		chunkSizes,
		blockData[:])
	defer storedBlock.Dispose()
	expected := error(nil)
	if err != nil {
		t.Errorf("CreateStoredBlock() %q != %q", err, expected)
	}
	if storedBlock.GetBlockHash() != blockHash {
		t.Errorf("CreateStoredBlock() GetBlockHash() %d != %d", blockHash, storedBlock.GetBlockHash())
	}
	if storedBlock.GetChunkCount() != uint32(len(chunkHashes)) {
		t.Errorf("CreateStoredBlock() GetChunkCount() %d != %d", uint32(len(chunkHashes)), storedBlock.GetChunkCount())
	}
	if storedBlock.GetCompressionType() != 0 {
		t.Errorf("CreateStoredBlock() GetChunkCount() %d != %d", 0, storedBlock.GetCompressionType())
	}
	verfyChunkHashes := storedBlock.GetChunkHashes()
	if len(chunkHashes) != len(verfyChunkHashes) {
		t.Errorf("CreateStoredBlock() len(chunkHashes) %d != %d", len(verfyChunkHashes), len(chunkHashes))
	}
	if verfyChunkHashes[0] != chunkHashes[0] {
		t.Errorf("CreateStoredBlock() chunkHashes %d != %d", verfyChunkHashes[0], chunkHashes[0])
	}
	if verfyChunkHashes[1] != chunkHashes[1] {
		t.Errorf("CreateStoredBlock() chunkHashes %d != %d", verfyChunkHashes[1], chunkHashes[1])
	}
	verifyChunkSizes := storedBlock.GetChunkSizes()
	if len(chunkSizes) != len(verifyChunkSizes) {
		t.Errorf("CreateStoredBlock() len(chunkSizes) %d != %d", len(verifyChunkSizes), len(chunkSizes))
	}
	if verifyChunkSizes[0] != chunkSizes[0] {
		t.Errorf("CreateStoredBlock() chunkSizes %d != %d", verifyChunkSizes[0], chunkSizes[0])
	}
	if verifyChunkSizes[1] != chunkSizes[1] {
		t.Errorf("CreateStoredBlock() chunkSizes %d != %d", verifyChunkSizes[1], chunkSizes[1])
	}
	verifyBlockData := storedBlock.GetBlockData()
	if len(blockData) != len(verifyBlockData) {
		t.Errorf("CreateStoredBlock() len(blockData) %d != %d", len(verifyBlockData), len(blockData))
	}
	for index, _ := range blockData {
		if blockData[index] != verifyBlockData[index] {
			t.Errorf("CreateStoredBlock() blockData[%d] %d != %d", index, verifyBlockData[index], blockData[index])
		}
	}
}

func TestFSBlockStore(t *testing.T) {
	storageAPI := CreateFSStorageAPI()
	defer storageAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()
	blockStoreAPI := CreateFSBlockStore(storageAPI, jobAPI, "content")
	defer blockStoreAPI.Dispose()
	blake3 := CreateBlake3HashAPI()
	defer blake3.Dispose()

	contentIndex, err := blockStoreAPI.GetIndex(jobAPI, blake3.GetIdentifier(), nil, nil)
	defer contentIndex.Dispose()
	expected := error(nil)
	if err != nil {
		t.Errorf("WriteToStorage() %q != %q", err, expected)
	}
}

/*
// CreateVersionIndexUtil ...
func CreateVersionIndexUtil(
	storageAPI Longtail_StorageAPI,
	hashAPI Longtail_HashAPI,
	jobAPI Longtail_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	versionPath string,
	compressionType uint32,
	targetChunkSize uint32) (Longtail_VersionIndex, error) {

	fileInfos, err := GetFilesRecursively(storageAPI, versionPath)
	if err != nil {
		return Longtail_VersionIndex{cVersionIndex: nil}, err
	}
	defer fileInfos.Dispose()

	pathCount := fileInfos.GetFileCount()
	compressionTypes := make([]uint32, pathCount)
	for i := uint32(0); i < pathCount; i++ {
		compressionTypes[i] = compressionType
	}

	vindex, err := CreateVersionIndex(
		storageAPI,
		hashAPI,
		jobAPI,
		progressFunc,
		progressContext,
		versionPath,
		fileInfos.GetPaths(),
		fileInfos.GetFileSizes(),
		fileInfos.GetFilePermissions(),
		compressionTypes,
		targetChunkSize)

	return vindex, err
}

//GetMissingContentUtil ... this is handy, but not what we should expose other than for tests!
func GetMissingContentUtil(
	contentBlockStoreAPI Longtail_BlockStoreAPI,
	missingContentBlockStoreAPI Longtail_BlockStoreAPI,
	versionStorageAPI Longtail_StorageAPI,
	hashAPI Longtail_HashAPI,
	jobAPI Longtail_JobAPI,
	progressFunc ProgressFunc,
	progressContext interface{},
	versionPath string,
	versionIndexPath string,
	contentIndexStorageAPI Longtail_StorageAPI,
	contentIndexPath string,
	compressionType uint32,
	maxChunksPerBlock uint32,
	targetBlockSize uint32,
	targetChunkSize uint32) (Longtail_ContentIndex, error) {

	var vindex Longtail_VersionIndex
	err := error(nil)

	if len(versionIndexPath) > 0 {
		vindex, err = ReadVersionIndex(versionStorageAPI, versionIndexPath)
	}
	if err != nil {
		vindex, err = CreateVersionIndexUtil(
			versionStorageAPI,
			hashAPI,
			jobAPI,
			progressFunc,
			progressContext,
			versionPath,
			compressionType,
			targetChunkSize)

		if err != nil {
			return Longtail_ContentIndex{cContentIndex: nil}, err
		}
	}
	defer vindex.Dispose()

	var cindex Longtail_ContentIndex

	if len(contentIndexPath) > 0 {
		cindex, err = ReadContentIndex(contentIndexStorageAPI, contentIndexPath)
	}
	if err != nil {
		cindex, err = contentBlockStoreAPI.GetIndex(
			jobAPI,
			hashAPI.GetIdentifier(),
			progressFunc,
			progressContext)
		if err != nil {
			return Longtail_ContentIndex{cContentIndex: nil}, err
		}
	}
	defer cindex.Dispose()

	missingContentIndex, err := CreateMissingContent(
		hashAPI,
		cindex,
		vindex,
		targetBlockSize,
		maxChunksPerBlock)

	if err != nil {
		return Longtail_ContentIndex{cContentIndex: nil}, err
	}

	compressionRegistry := CreateDefaultCompressionRegistry()
	defer compressionRegistry.Dispose()

	err = WriteContent(
		versionStorageAPI,
		missingContentBlockStoreAPI,
		compressionRegistry,
		jobAPI,
		progressFunc,
		progressContext,
		missingContentIndex,
		vindex,
		versionPath)

	if err != nil {
		missingContentIndex.Dispose()
		return Longtail_ContentIndex{cContentIndex: nil}, err
	}

	if len(versionIndexPath) > 0 {
		err = WriteVersionIndex(
			contentIndexStorageAPI,
			vindex,
			versionIndexPath)
		if err != nil {
			missingContentIndex.Dispose()
			return Longtail_ContentIndex{cContentIndex: nil}, err
		}
	}

	if len(contentIndexPath) > 0 {
		err = WriteContentIndex(
			contentIndexStorageAPI,
			cindex,
			contentIndexPath)
		if err != nil {
			missingContentIndex.Dispose()
			return Longtail_ContentIndex{cContentIndex: nil}, err
		}
	}
	return missingContentIndex, nil
}

func TestInit(t *testing.T) {
	l := SetLogger(logger, &loggerData{t: t})
	defer ClearLogger(l)
	SetLogLevel(3)

	storageAPI := CreateInMemStorageAPI()
	if storageAPI.cStorageAPI == nil {
		t.Errorf("CreateInMemStorageAPI() storageAPI.cStorageAPI == nil")
	}
	defer storageAPI.Dispose()
	blake2API := CreateBlake2HashAPI()
	if blake2API.cHashAPI == nil {
		t.Errorf("CreateBlake2HashAPI() hashAPI.cHashAPI == nil")
	}
	defer blake2API.Dispose()
	blake3API := CreateBlake3HashAPI()
	if blake3API.cHashAPI == nil {
		t.Errorf("CreateBlake3HashAPI() hashAPI.cHashAPI == nil")
	}
	defer blake3API.Dispose()
	meowAPI := CreateMeowHashAPI()
	if meowAPI.cHashAPI == nil {
		t.Errorf("CreateMeowHashAPI() hashAPI.cHashAPI == nil")
	}
	defer meowAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	if jobAPI.cJobAPI == nil {
		t.Errorf("CreateBikeshedJobAPI() jobAPI.cJobAPI == nil")
	}
	defer jobAPI.Dispose()
}

func TestCreateVersionIndex(t *testing.T) {
	l := SetLogger(logger, &loggerData{t: t})
	defer ClearLogger(l)
	SetLogLevel(3)

	storageAPI := CreateInMemStorageAPI()
	defer storageAPI.Dispose()
	hashAPI := CreateBlake2HashAPI()
	defer hashAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()

	WriteToStorage(storageAPI, "", "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(storageAPI, "", "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(storageAPI, "", "top_level.txt", []byte("the top level file is also a text file with dummy content"))
	WriteToStorage(storageAPI, "", "first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})

	vi, err := CreateVersionIndexUtil(
		storageAPI,
		hashAPI,
		jobAPI,
		progress,
		&progressData{task: "Indexing", t: t},
		"",
		GetBrotliGenericDefaultCompressionType(),
		32768)

	expected := error(nil)
	if err != nil {
		t.Errorf("CreateVersionIndex() %q != %q", err, expected)
	}
	defer vi.Dispose()

	expectedAssetCount := uint32(14)
	if ret := vi.GetAssetCount(); ret != expectedAssetCount {
		t.Errorf("CreateVersionIndex() asset count = %d, want %d", ret, expectedAssetCount)
	}
	expectedChunkCount := uint32(3)
	if ret := vi.GetChunkCount(); ret != expectedChunkCount {
		t.Errorf("CreateVersionIndex() chunk count = %d, want %d", ret, expectedChunkCount)
	}
}

func TestReadWriteVersionIndex(t *testing.T) {
	l := SetLogger(logger, &loggerData{t: t})
	defer ClearLogger(l)
	SetLogLevel(3)

	storageAPI := CreateInMemStorageAPI()
	defer storageAPI.Dispose()
	hashAPI := CreateBlake3HashAPI()
	defer hashAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()

	WriteToStorage(storageAPI, "", "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(storageAPI, "", "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(storageAPI, "", "top_level.txt", []byte("the top level file is also a text file with dummy content"))
	WriteToStorage(storageAPI, "", "first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})

	fileInfos, err := GetFilesRecursively(storageAPI, "")
	expected := error(nil)
	if err != nil {
		t.Errorf("GetFilesRecursively() %q != %q", err, expected)
	}
	defer fileInfos.Dispose()

	pathCount := fileInfos.GetFileCount()
	compressionTypes := make([]uint32, pathCount)
	for i := uint32(0); i < pathCount; i++ {
		compressionTypes[i] = GetLZ4DefaultCompressionType()
	}

	vi, err := CreateVersionIndex(
		storageAPI,
		hashAPI,
		jobAPI,
		progress,
		&progressData{task: "Indexing", t: t},
		"",
		fileInfos.GetPaths(),
		fileInfos.GetFileSizes(),
		fileInfos.GetFilePermissions(),
		compressionTypes,
		32768)

	if err != nil {
		t.Errorf("CreateVersionIndex() %q != %q", err, expected)
	}
	defer vi.Dispose()

	if ret := WriteVersionIndex(storageAPI, vi, "test.lvi"); ret != expected {
		t.Errorf("WriteVersionIndex() = %q, want %q", ret, expected)
	}

	vi2, ret := ReadVersionIndex(storageAPI, "test.lvi")
	if ret != expected {
		t.Errorf("WriteVersionIndex() = %q, want %q", ret, expected)
	}
	defer vi2.Dispose()

	if vi2.GetAssetCount() != vi.GetAssetCount() {
		t.Errorf("ReadVersionIndex() asset count = %d, want %d", vi2.GetAssetCount(), vi.GetAssetCount())
	}

	for i := uint32(0); i < vi.GetAssetCount(); i++ {
		expected := GetVersionIndexPath(vi, i)
		if ret := GetVersionIndexPath(vi2, i); ret != expected {
			t.Errorf("ReadVersionIndex() path %d = %s, want %s", int(i), ret, expected)
		}
	}

	buf, err := WriteVersionIndexToBuffer(vi)
	if err != nil {
		t.Errorf("WriteVersionIndexToBuffer() %q != %q", err, expected)
	}
	viCopy, err := ReadVersionIndexFromBuffer(buf)
	if err != nil {
		t.Errorf("WriteVersionIndexToBuffer() %q != %q", err, expected)
	}
	defer viCopy.Dispose()
}

type assertData struct {
	t *testing.T
}

func testAssertFunc(context interface{}, expression string, file string, line int) {
	fmt.Printf("ASSERT: %s %s:%d", expression, file, line)
}

func TestUpSyncVersion(t *testing.T) {
	l := SetLogger(logger, &loggerData{t: t})
	defer ClearLogger(l)
	SetLogLevel(0)

	SetAssert(testAssertFunc, &assertData{t: t})
	defer ClearAssert()

	upsyncStorageAPI := CreateInMemStorageAPI()
	defer upsyncStorageAPI.Dispose()
	hashAPI := CreateMeowHashAPI()
	defer hashAPI.Dispose()
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer jobAPI.Dispose()
	remoteStorageAPI := CreateInMemStorageAPI()
	defer remoteStorageAPI.Dispose()

	t.Logf("Creating `current`")
	WriteToStorage(upsyncStorageAPI, "current", "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(upsyncStorageAPI, "current", "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(upsyncStorageAPI, "current", "top_level.txt", []byte("the top level file is also a text file with dummy content"))
	WriteToStorage(upsyncStorageAPI, "current", "first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})

	t.Logf("Reading remote `store.lci`")
	storeIndex, err := ReadContentIndex(remoteStorageAPI, "store.lci")
	if err != nil {
		storeIndex, err = CreateContentIndex(hashAPI, 0, nil, nil, nil, 32768*12, 4096)
		if err != nil {
			t.Errorf("CreateContentIndex() err = %q, want %q", err, error(nil))
		}
	}
	defer storeIndex.Dispose()
	err = WriteContentIndex(upsyncStorageAPI, storeIndex, "store.lci")
	if err != nil {
		t.Errorf("WriteContentIndex() err = %q, want %q", err, error(nil))
	}

	blockStore := CreateFSBlockStore(upsyncStorageAPI, jobAPI, "cache")
	defer blockStore.Dispose()

	t.Logf("Get missing content for `current` / `cache`")
	missingContentIndex, err := GetMissingContentUtil(
		blockStore,
		blockStore,
		upsyncStorageAPI,
		hashAPI,
		jobAPI,
		progress,
		&progressData{task: "Indexing", t: t},
		"current",
		"current.lvi",
		upsyncStorageAPI,
		"cache.lci",
		GetZStdDefaultCompressionType(),
		4096,
		32768,
		32758*12)
	if err != nil {
		t.Errorf("GetMissingContent() err = %q, want %q", err, error(nil))
	}
	defer missingContentIndex.Dispose()

	expected := error(nil)
	buf, err := WriteContentIndexToBuffer(missingContentIndex)
	if err != nil {
		t.Errorf("WriteContentIndexToBuffer() %q != %q", err, expected)
	}
	ciCopy, err := ReadContentIndexFromBuffer(buf)
	if err != nil {
		t.Errorf("ReadContentIndexFromBuffer() %q != %q", err, expected)
	}
	defer ciCopy.Dispose()

	var expectedBlockCount uint64 = 1
	if missingContentIndex.GetBlockCount() != expectedBlockCount {
		t.Errorf("UpSyncVersion() len(blockHashes) = %d, want %d", missingContentIndex.GetBlockCount(), expectedBlockCount)
	}

	t.Logf("Copying blocks from `cache` / `store`")
	for i := uint64(0); i < missingContentIndex.GetBlockCount(); i++ {
		blockHash := missingContentIndex.GetBlockHash(i)
		path, err := blockStore.GetStoredBlockPath(blockHash)
		if err != nil {
			t.Errorf("UpSyncVersion() ReadFromStorage(%d, %s) = %q, want %q", "cache", blockHash, err, error(nil))
		}
		block, err := ReadFromStorage(upsyncStorageAPI, "cache", path)
		if err != nil {
			t.Errorf("UpSyncVersion() ReadFromStorage(%s, %s) = %q, want %q", "cache", path, err, error(nil))
		}
		err = WriteToStorage(remoteStorageAPI, "store", path, block)
		if err != nil {
			t.Errorf("UpSyncVersion() WriteToStorage(%s, %s) = %q, want %q", "store", path, err, error(nil))
		}
		t.Logf("Copied block: `%s` from `%s` to `%s`", path, "cache", "store")
	}

	t.Logf("Reading remote index from `store`")
	version, err := ReadFromStorage(upsyncStorageAPI, "", "current.lvi")
	if err != nil {
		t.Errorf("UpSyncVersion() ReadFromStorage(%s, %s) = %q, want %q", "", "local.lci", err, error(nil))
	}
	err = WriteToStorage(remoteStorageAPI, "", "version.lvi", version)
	if err != nil {
		t.Errorf("UpSyncVersion() WriteToStorage(%s, %s) = %q, want %q", "", "version.lvi", err, error(nil))
	}

	t.Logf("Updating remote index from `store`")
	storeIndex, err = MergeContentIndex(storeIndex, missingContentIndex)
	if err != nil {
		t.Errorf("UpSyncVersion() MergeContentIndex() err = %q, want %q", err, error(nil))
	}
	WriteContentIndex(remoteStorageAPI, storeIndex, "store.lci")

	t.Logf("Starting downsync to `current`")
	downSyncStorageAPI := CreateInMemStorageAPI()
	defer downSyncStorageAPI.Dispose()

	t.Logf("Reading remote index from `store`")
	remoteStorageIndex, err := ReadContentIndex(remoteStorageAPI, "store.lci")
	if err != nil {
		t.Errorf("UpSyncVersion() ReadContentIndex(%s) = %q, want %q", "store.lci", err, error(nil))
	}
	defer remoteStorageIndex.Dispose()
	t.Logf("Blocks in store: %d", remoteStorageIndex.GetBlockCount())

	t.Logf("Reading version index from `version.lvi`")
	targetVersionIndex, err := ReadVersionIndex(remoteStorageAPI, "version.lvi")
	if err != nil {
		t.Errorf("UpSyncVersion() ReadVersionIndex(%s) = %q, want %q", "version.lvi", err, error(nil))
	}
	defer targetVersionIndex.Dispose()
	t.Logf("Assets in version: %d", targetVersionIndex.GetAssetCount())

	cacheContentIndex, err := ReadContentIndex(downSyncStorageAPI, "cache.lci")
	if err != nil {
		cacheContentIndex, err = ReadContent(
			downSyncStorageAPI,
			hashAPI,
			jobAPI,
			progress,
			&progressData{task: "Reading local cache", t: t},
			"cache")
		if err != nil {
			t.Errorf("UpSyncVersion() ReadContent(%s) = %q, want %q", "cache", err, error(nil))
		}
	}
	defer cacheContentIndex.Dispose()
	t.Logf("Blocks in cacheContentIndex: %d", cacheContentIndex.GetBlockCount())

	missingContentIndex, err = CreateMissingContent(
		hashAPI,
		cacheContentIndex,
		targetVersionIndex,
		32758*12,
		4096)
	if err != nil {
		t.Errorf("UpSyncVersion() CreateMissingContent() = %q, want %q", err, error(nil))
	}
	t.Logf("Blocks in missingContentIndex: %d", missingContentIndex.GetBlockCount())

	requestContent, err := RetargetContent(
		remoteStorageIndex,
		missingContentIndex)
	if err != nil {
		t.Errorf("UpSyncVersion() RetargetContent() = %q, want %q", err, error(nil))
	}
	defer requestContent.Dispose()
	t.Logf("Blocks in requestContent: %d", requestContent.GetBlockCount())

	missingPaths, err = GetPathsForContentBlocks(requestContent)
	if err != nil {
		t.Errorf("UpSyncVersion() GetPathsForContentBlocks() = %q, want %q", err, error(nil))
	}
	t.Logf("Path count for content: %d", missingPaths.GetPathCount())
	for i := uint32(0); i < missingPaths.GetPathCount(); i++ {
		path := GetPath(missingPaths, uint32(i))
		block, err := ReadFromStorage(remoteStorageAPI, "store", path)
		if err != nil {
			t.Errorf("UpSyncVersion() ReadFromStorage(%s, %s) = %q, want %q", "store", path, err, error(nil))
		}
		err = WriteToStorage(downSyncStorageAPI, "cache", path, block)
		if err != nil {
			t.Errorf("UpSyncVersion() WriteToStorage(%s, %s) = %q, want %q", "cache", path, err, error(nil))
		}
		t.Logf("Copied block: `%s` from `%s` to `%s`", path, "store", "cache")
	}
	defer missingPaths.Dispose()

	mergedCacheContentIndex, err := MergeContentIndex(cacheContentIndex, requestContent)
	if err != nil {
		t.Errorf("UpSyncVersion() MergeContentIndex(%s, %s) = %q, want %q", "cache", "store", err, error(nil))
	}
	t.Logf("Blocks in cacheContentIndex after merge: %d", mergedCacheContentIndex.GetBlockCount())
	defer mergedCacheContentIndex.Dispose()

	compressionRegistry := CreateDefaultCompressionRegistry()
	defer compressionRegistry.Dispose()
	t.Log("Created compression registry")

	currentVersionIndex, err := CreateVersionIndexUtil(
		downSyncStorageAPI,
		hashAPI,
		jobAPI,
		progress,
		&progressData{task: "Indexing version", t: t},
		"current",
		GetBrotliGenericDefaultCompressionType(),
		32768)
	if err != nil {
		t.Errorf("UpSyncVersion() CreateVersionIndex(%s) = %q, want %q", "current", err, error(nil))
	}
	defer currentVersionIndex.Dispose()
	t.Logf("Asset count for currentVersionIndex: %d", currentVersionIndex.GetAssetCount())

	versionDiff, err := CreateVersionDiff(currentVersionIndex, targetVersionIndex)
	if err != nil {
		t.Errorf("UpSyncVersion() CreateVersionDiff(%s, %s) = %q, want %q", "current", "version.lvi", err, error(nil))
	}
	defer versionDiff.Dispose()

	err = ChangeVersion(
		downSyncStorageAPI,
		downSyncStorageAPI,
		hashAPI,
		jobAPI,
		progress,
		&progressData{task: "Updating version", t: t},
		compressionRegistry,
		mergedCacheContentIndex,
		currentVersionIndex,
		targetVersionIndex,
		versionDiff,
		"cache",
		"current",
		true)
	if err != nil {
		t.Errorf("UpSyncVersion() ChangeVersion(%s, %s) = %q, want %q", "cache", "current", err, error(nil))
	}
}
*/
