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

func TestCreateVersionIndex(t *testing.T) {
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
	versionStorageAPI := CreateInMemStorageAPI()
	defer DestroyStorageAPI(versionStorageAPI)
	hashAPI := CreateMeowHashAPI()
	defer DestroyHashAPI(hashAPI)
	jobAPI := CreateBikeshedJobAPI(uint32(runtime.NumCPU()))
	defer DestroyJobAPI(jobAPI)
	contentStorageAPI := CreateInMemStorageAPI()
	defer DestroyStorageAPI(contentStorageAPI)

	WriteToStorage(versionStorageAPI, "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(versionStorageAPI, "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(versionStorageAPI, "top_level.txt", []byte("the top level file is also a text file with dummy content"))
	WriteToStorage(versionStorageAPI, "first_folder/empty/file/deeply/nested/file/in/lots/of/nests.txt", []byte{})

	err, blockHashes := GetMissingBlocks(
		contentStorageAPI,
		versionStorageAPI,
		hashAPI,
		jobAPI,
		progress,
		&progressData{task: "Upsyncing", t: t},
		"",
		"version.lvi",
		"store",
		"store.lci",
		"upload",
		"missing.lci",
		GetLizardDefaultCompressionType(),
		4096,
		32768,
		32758*12)
	expectedErr := error(nil)
	if err != expectedErr {
		t.Errorf("UpSyncVersion() err = %q, want %q", err, expectedErr)
	}
	expectedBlockCount := 1
	if len(blockHashes) != expectedBlockCount {
		t.Errorf("UpSyncVersion() len(blockHashes) = %d, want %d", len(blockHashes), expectedBlockCount)
	}
}
