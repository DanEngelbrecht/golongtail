package golongtail

import (
	//	"fmt"
	"runtime"
	"testing"
	"unsafe"
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

func TestCreateVersionIndex(t *testing.T) {
	l := SetLogger(logger, &loggerData{t: t})
	defer ClearLogger(l)
	SetLogLevel(3)

	storageAPI := CreateInMemStorageAPI()
	defer DestroyStorageAPI(storageAPI)
	hashAPI := CreateMeowHashAPI()
	defer DestroyHashAPI(hashAPI)
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer DestroyJobAPI(jobAPI)

	WriteToStorage(storageAPI, "", "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(storageAPI, "", "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(storageAPI, "", "top_level.txt", []byte("the top level file is also a text file with dummy content"))
	WriteToStorage(storageAPI, "", "first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})

	vi, err := CreateVersionIndex(
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
	defer LongtailFree(unsafe.Pointer(vi))

	expectedAssetCount := uint32(14)
	if ret := uint32(*vi.m_AssetCount); ret != expectedAssetCount {
		t.Errorf("CreateVersionIndex() asset count = %d, want %d", ret, expectedAssetCount)
	}
	expectedChunkCount := uint32(3)
	if ret := uint32(*vi.m_ChunkCount); ret != expectedChunkCount {
		t.Errorf("CreateVersionIndex() chunk count = %d, want %d", ret, expectedChunkCount)
	}
}

func TestReadWriteVersionIndex(t *testing.T) {
	l := SetLogger(logger, &loggerData{t: t})
	defer ClearLogger(l)
	SetLogLevel(3)

	storageAPI := CreateInMemStorageAPI()
	defer DestroyStorageAPI(storageAPI)
	hashAPI := CreateMeowHashAPI()
	defer DestroyHashAPI(hashAPI)
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer DestroyJobAPI(jobAPI)

	WriteToStorage(storageAPI, "", "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(storageAPI, "", "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(storageAPI, "", "top_level.txt", []byte("the top level file is also a text file with dummy content"))
	WriteToStorage(storageAPI, "", "first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})

	vi, err := CreateVersionIndex(
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
	defer LongtailFree(unsafe.Pointer(vi))

	expectedErr := error(nil)
	if ret := WriteVersionIndex(storageAPI, vi, "test.lvi"); ret != expectedErr {
		t.Errorf("WriteVersionIndex() = %q, want %q", ret, expectedErr)
	}

	vi2, ret := ReadVersionIndex(storageAPI, "test.lvi")
	if ret != nil {
		t.Errorf("WriteVersionIndex() = %q, want %q", ret, expectedErr)
	}
	defer LongtailFree(unsafe.Pointer(vi2))

	if (*vi2.m_AssetCount) != (*vi.m_AssetCount) {
		t.Errorf("ReadVersionIndex() asset count = %d, want %d", (*vi2.m_AssetCount), (*vi.m_AssetCount))
	}

	for i := uint32(0); i < uint32(*vi.m_AssetCount); i++ {
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
	defer DestroyStorageAPI(upsyncStorageAPI)
	hashAPI := CreateMeowHashAPI()
	defer DestroyHashAPI(hashAPI)
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer DestroyJobAPI(jobAPI)
	remoteStorageAPI := CreateInMemStorageAPI()
	defer DestroyStorageAPI(remoteStorageAPI)

	t.Logf("Creating `current`")
	WriteToStorage(upsyncStorageAPI, "current", "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(upsyncStorageAPI, "current", "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(upsyncStorageAPI, "current", "top_level.txt", []byte("the top level file is also a text file with dummy content"))
	WriteToStorage(upsyncStorageAPI, "current", "first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})

	t.Logf("Reading remote `store.lci`")
	storeIndex, _ := ReadContentIndex(remoteStorageAPI, "store.lci")
	if storeIndex == nil {
		var err error
		storeIndex, err = CreateContentIndex(hashAPI, 0, nil, nil, nil, 32768*12, 4096)
		if err != nil {
			t.Errorf("CreateContentIndex() err = %q, want %q", err, error(nil))
		}
	}
	defer LongtailFree(unsafe.Pointer(storeIndex))
	err := WriteContentIndex(upsyncStorageAPI, storeIndex, "store.lci")
	if err != nil {
		t.Errorf("WriteContentIndex() err = %q, want %q", err, error(nil))
	}

	t.Logf("Get missing content for `current` / `cache`")
	missingContentIndex, err := GetMissingContent(
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
	defer LongtailFree(unsafe.Pointer(missingContentIndex))

	expectedBlockCount := 1
	if int(*missingContentIndex.m_BlockCount) != expectedBlockCount {
		t.Errorf("UpSyncVersion() len(blockHashes) = %d, want %d", int(*missingContentIndex.m_BlockCount), expectedBlockCount)
	}

	t.Logf("Copying blocks from `cache` / `store`")
	missingPaths, err := GetPathsForContentBlocks(missingContentIndex)
	t.Errorf("UpSyncVersion() GetPathsForContentBlocks(%s, %s) = %q, want %q", "", "local.lci", err, error(nil))
	for i := 0; i < int(*missingPaths.m_PathCount); i++ {
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
	defer DestroyStorageAPI(downSyncStorageAPI)

	t.Logf("Reading remote index from `store`")
	remoteStorageIndex, err := ReadContentIndex(remoteStorageAPI, "store.lci")
	if err != nil {
		t.Errorf("UpSyncVersion() ReadContentIndex(%s) = %q, want %q", "store.lci", err, error(nil))
	}
	defer LongtailFree(unsafe.Pointer(remoteStorageIndex))
	t.Logf("Blocks in store: %d", int(*remoteStorageIndex.m_BlockCount))

	t.Logf("Reading version index from `version.lvi`")
	targetVersionIndex, err := ReadVersionIndex(remoteStorageAPI, "version.lvi")
	if err != nil {
		t.Errorf("UpSyncVersion() ReadVersionIndex(%s) = %q, want %q", "version.lvi", err, error(nil))
	}
	if targetVersionIndex == nil {
		t.Errorf("UpSyncVersion() ReadVersionIndex(%s) = targetVersionIndex == nil", "version.lvi")
	}
	defer LongtailFree(unsafe.Pointer(targetVersionIndex))
	t.Logf("Assets in version: %d", int(*targetVersionIndex.m_AssetCount))

	cacheContentIndex, _ := ReadContentIndex(downSyncStorageAPI, "cache.lci")
	if cacheContentIndex == nil {
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
	defer LongtailFree(unsafe.Pointer(cacheContentIndex))
	t.Logf("Blocks in cacheContentIndex: %d", int(*cacheContentIndex.m_BlockCount))

	missingContentIndex, err = CreateMissingContent(
		hashAPI,
		cacheContentIndex,
		targetVersionIndex,
		32758*12,
		4096)
	if err != nil {
		t.Errorf("UpSyncVersion() CreateMissingContent() = %q, want %q", err, error(nil))
	}
	t.Logf("Blocks in missingContentIndex: %d", int(*missingContentIndex.m_BlockCount))

	requestContent, err := RetargetContent(
		remoteStorageIndex,
		missingContentIndex)
	if err != nil {
		t.Errorf("UpSyncVersion() RetargetContent() = %q, want %q", err, error(nil))
	}
	defer LongtailFree(unsafe.Pointer(requestContent))
	t.Logf("Blocks in requestContent: %d", int(*requestContent.m_BlockCount))

	missingPaths, err = GetPathsForContentBlocks(requestContent)
	if err != nil {
		t.Errorf("UpSyncVersion() GetPathsForContentBlocks() = %q, want %q", err, error(nil))
	}
	t.Logf("Path count for content: %d", int(*missingPaths.m_PathCount))
	for i := 0; i < int(*missingPaths.m_PathCount); i++ {
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
	defer LongtailFree(unsafe.Pointer(missingPaths))

	mergedCacheContentIndex, err := MergeContentIndex(cacheContentIndex, requestContent)
	if err != nil {
		t.Errorf("UpSyncVersion() MergeContentIndex(%s, %s) = %q, want %q", "cache", "store", err, error(nil))
	}
	t.Logf("Blocks in cacheContentIndex after merge: %d", int(*mergedCacheContentIndex.m_BlockCount))
	defer LongtailFree(unsafe.Pointer(mergedCacheContentIndex))

	compressionRegistry := CreateDefaultCompressionRegistry()
	defer DestroyCompressionRegistry(compressionRegistry)
	t.Log("Created compression registry")

	currentVersionIndex, err := CreateVersionIndex(
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
	defer LongtailFree(unsafe.Pointer(currentVersionIndex))
	t.Logf("Asset count for currentVersionIndex: %d", int(*currentVersionIndex.m_AssetCount))

	versionDiff, err := CreateVersionDiff(currentVersionIndex, targetVersionIndex)
	if err != nil {
		t.Errorf("UpSyncVersion() CreateVersionDiff(%s, %s) = %q, want %q", "current", "version.lvi", err, error(nil))
	}
	defer LongtailFree(unsafe.Pointer(versionDiff))

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
