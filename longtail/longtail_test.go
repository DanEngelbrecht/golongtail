package longtail

import (
	//	"fmt"
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

// CreateVersionIndexUtil ...
func CreateVersionIndexUtil(
	storageAPI Longtail_StorageAPI,
	hashAPI Longtail_HashAPI,
	jobAPI Longtail_JobAPI,
	progressFunc progressFunc,
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
		compressionTypes,
		targetChunkSize)

	return vindex, err
}

//GetMissingContentUtil ... this is handy, but not what we should expose other than for tests!
func GetMissingContentUtil(
	contentStorageAPI Longtail_StorageAPI,
	versionStorageAPI Longtail_StorageAPI,
	hashAPI Longtail_HashAPI,
	jobAPI Longtail_JobAPI,
	progressFunc progressFunc,
	progressContext interface{},
	versionPath string,
	versionIndexPath string,
	contentPath string,
	contentIndexPath string,
	missingContentPath string,
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
		cindex, err = ReadContentIndex(contentStorageAPI, contentIndexPath)
	}
	if err != nil {
		cindex, err = ReadContent(
			contentStorageAPI,
			hashAPI,
			jobAPI,
			progressFunc,
			progressContext,
			contentPath)
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
		contentStorageAPI,
		compressionRegistry,
		jobAPI,
		progressFunc,
		progressContext,
		missingContentIndex,
		vindex,
		versionPath,
		missingContentPath)

	if err != nil {
		missingContentIndex.Dispose()
		return Longtail_ContentIndex{cContentIndex: nil}, err
	}

	if len(versionIndexPath) > 0 {
		err = WriteVersionIndex(
			contentStorageAPI,
			vindex,
			versionIndexPath)
		if err != nil {
			missingContentIndex.Dispose()
			return Longtail_ContentIndex{cContentIndex: nil}, err
		}
	}

	if len(contentIndexPath) > 0 {
		err = WriteContentIndex(
			contentStorageAPI,
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
	hashAPI := CreateXXHashAPI()
	if hashAPI.cHashAPI == nil {
		t.Errorf("CreateXXHashAPI() hashAPI.cHashAPI == nil")
	}
	defer hashAPI.Dispose()
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
	hashAPI := CreateXXHashAPI()
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
		GetLizardDefaultCompressionType(),
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
	hashAPI := CreateXXHashAPI()
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
		compressionTypes[i] = GetLizardDefaultCompressionType()
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
}

func TestUpSyncVersion(t *testing.T) {
	l := SetLogger(logger, &loggerData{t: t})
	defer ClearLogger(l)
	SetLogLevel(0)

	upsyncStorageAPI := CreateInMemStorageAPI()
	defer upsyncStorageAPI.Dispose()
	hashAPI := CreateXXHashAPI()
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

	t.Logf("Get missing content for `current` / `cache`")
	missingContentIndex, err := GetMissingContentUtil(
		upsyncStorageAPI,
		upsyncStorageAPI,
		hashAPI,
		jobAPI,
		progress,
		&progressData{task: "Indexing", t: t},
		"current",
		"current.lvi",
		"cache",
		"cache.lci",
		"cache",
		GetLizardDefaultCompressionType(),
		4096,
		32768,
		32758*12)
	if err != nil {
		t.Errorf("GetMissingContent() err = %q, want %q", err, error(nil))
	}
	defer missingContentIndex.Dispose()

	var expectedBlockCount uint64 = 1
	if missingContentIndex.GetBlockCount() != expectedBlockCount {
		t.Errorf("UpSyncVersion() len(blockHashes) = %d, want %d", missingContentIndex.GetBlockCount(), expectedBlockCount)
	}

	t.Logf("Copying blocks from `cache` / `store`")
	missingPaths, err := GetPathsForContentBlocks(missingContentIndex)
	if err != nil {
		t.Errorf("UpSyncVersion() GetPathsForContentBlocks(%s, %s) = %q, want %q", "", "local.lci", err, error(nil))
	}
	for i := uint32(0); i < missingPaths.GetPathCount(); i++ {
		path := GetPath(missingPaths, uint32(i))
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
		GetLizardDefaultCompressionType(),
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
		"current")
	if err != nil {
		t.Errorf("UpSyncVersion() ChangeVersion(%s, %s) = %q, want %q", "cache", "current", err, error(nil))
	}
}
