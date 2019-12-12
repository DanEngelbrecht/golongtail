package golongtail

import (
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
	t	*testing.T
}

func logger(context interface{}, level int, log string) {
	p := context.(*loggerData)
	p.t.Logf("%d: %s: ", level, log)
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

	WriteToStorage(storageAPI, "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(storageAPI, "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(storageAPI, "top_level.txt", []byte("the top level file is also a text file with dummy content"))
	WriteToStorage(storageAPI, "first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})

	err, vi := CreateVersionIndex(
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
		t.Errorf("CreateVersionIndexFromFolder() %q != %q", err, expected)
	}
	defer LongtailFree(unsafe.Pointer(vi))

	expectedAssetCount := uint32(14)
	if ret := uint32(*vi.m_AssetCount); ret != expectedAssetCount {
		t.Errorf("CreateVersionIndexFromFolder() asset count = %d, want %d", ret, expectedAssetCount)
	}
	expectedChunkCount := uint32(3)
	if ret := uint32(*vi.m_ChunkCount); ret != expectedChunkCount {
		t.Errorf("CreateVersionIndexFromFolder() chunk count = %d, want %d", ret, expectedChunkCount)
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

	WriteToStorage(storageAPI, "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(storageAPI, "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(storageAPI, "top_level.txt", []byte("the top level file is also a text file with dummy content"))
	WriteToStorage(storageAPI, "first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})

	err, vi := CreateVersionIndex(
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
		t.Errorf("CreateVersionIndexFromFolder() %q != %q", err, expected)
	}
	defer LongtailFree(unsafe.Pointer(vi))

	expectedErr := error(nil)
	if ret := WriteVersionIndex(storageAPI, vi, "test.lvi"); ret != expectedErr {
		t.Errorf("WriteVersionIndex() = %q, want %q", ret, expectedErr)
	}

	vi2 := ReadVersionIndex(storageAPI, "test.lvi")
	if vi2 == nil {
		t.Errorf("ReadVersionIndex() = nil, want !nil")
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
	SetLogLevel(3)

	versionStorageAPI := CreateInMemStorageAPI()
	defer DestroyStorageAPI(versionStorageAPI)
	hashAPI := CreateMeowHashAPI()
	defer DestroyHashAPI(hashAPI)
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer DestroyJobAPI(jobAPI)
	contentStorageAPI := CreateInMemStorageAPI()
	defer DestroyStorageAPI(contentStorageAPI)

	WriteToStorage(versionStorageAPI, "source/current/first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(versionStorageAPI, "source/current/second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(versionStorageAPI, "source/current/top_level.txt", []byte("the top level file is also a text file with dummy content"))
	WriteToStorage(versionStorageAPI, "source/current/first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})

	storeIndex := ReadContentIndex(contentStorageAPI, "remote/store.lci")
	if storeIndex == nil {
		var err error
		err, storeIndex = CreateContentIndex(hashAPI, 0, nil, nil, nil, 32768*12, 4096)
		if err != nil {
			t.Errorf("CreateContentIndex() err = %q, want %q", err, error(nil))
		}
	}
	defer LongtailFree(unsafe.Pointer(storeIndex))

	err, missingContentIndex := GetMissingContent(
		contentStorageAPI,
		versionStorageAPI,
		hashAPI,
		jobAPI,
		progress,
		&progressData{task: "Upsyncing", t: t},
		"source/current",
		"current.lvi",
		"source/cache",
		"remote/store.lci",
		"source/cache",
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

	missingPaths := GetPathsForContentBlocks(missingContentIndex)
	for i := 0; i < int(*missingPaths.m_PathCount); i++ {
		path := GetPath(missingPaths, uint32(i))
		t.Logf("New block path: `%s`", path)
	}

	err, mergedContentIndex := MergeContentIndex(storeIndex, missingContentIndex)
	if err != nil {
		t.Errorf("MergeContentIndex() err = %q, want %q", err, error(nil))
	}
	defer LongtailFree(unsafe.Pointer(mergedContentIndex))

	WriteContentIndex(contentStorageAPI, mergedContentIndex, "remote/store.lci")
}
