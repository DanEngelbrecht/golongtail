package golongtail

import (
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

func TestCreateVersionIndexFromFolder(t *testing.T) {
	fs := CreateInMemStorageAPI()
	defer DestroyStorageAPI(fs)

	WriteToStorage(fs, "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(fs, "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(fs, "top_level.txt", []byte("the top level file is also a text file with dummy content"))

	vi := CreateVersionIndexFromFolder(fs, "", MakeProgressProxy(progress, &progressData{task: "Indexing", t: t}))
	if vi == nil {
		t.Errorf("CreateVersionIndexFromFolder() = nil, want !nil")
	}
	defer LongtailFree(unsafe.Pointer(vi))

	expected_asset_count := uint32(5)
	if ret := uint32(*vi.m_AssetCount); ret != expected_asset_count {
		t.Errorf("CreateVersionIndexFromFolder() asset count = %q, want %q", ret, expected_asset_count)
	}
	expected_chunk_count := uint32(3)
	if ret := uint32(*vi.m_ChunkCount); ret != expected_chunk_count {
		t.Errorf("CreateVersionIndexFromFolder() chunk count = %q, want %q", ret, expected_chunk_count)
	}
}

func TestReadWriteVersionIndex(t *testing.T) {
	fs := CreateInMemStorageAPI()
	defer DestroyStorageAPI(fs)

	WriteToStorage(fs, "first_folder/my_file.txt", []byte("the content of my_file"))
	WriteToStorage(fs, "second_folder/my_second_file.txt", []byte("second file has different content than my_file"))
	WriteToStorage(fs, "top_level.txt", []byte("the top level file is also a text file with dummy content"))

	vi := CreateVersionIndexFromFolder(fs, "", MakeProgressProxy(progress, &progressData{task: "Indexing", t: t}))
	if vi == nil {
		t.Errorf("CreateVersionIndexFromFolder() = nil, want !nil")
	}
	defer LongtailFree(unsafe.Pointer(vi))

	expected_err := error(nil)
	if ret := WriteVersionIndex(fs, vi, "test.lvi"); ret != expected_err {
		t.Errorf("WriteVersionIndex() = %q, want %q", ret, expected_err)
	}

	vi2 := ReadVersionIndex(fs, "test.lvi")
	if vi2 == nil {
		t.Errorf("ReadVersionIndex() = nil, want !nil")
	}
	defer LongtailFree(unsafe.Pointer(vi2))

	if (*vi2.m_AssetCount) != (*vi.m_AssetCount) {
		t.Errorf("ReadVersionIndex() asset count = %q, want %q", (*vi2.m_AssetCount), (*vi.m_AssetCount))
	}

	for i := uint32(0); i < uint32(*vi.m_AssetCount); i++ {
		expected := GetVersionIndexPath(vi, i)
		if ret := GetVersionIndexPath(vi2, i); ret != expected {
			t.Errorf("ReadVersionIndex() path %d = %q, want %q", int(i), ret, expected)
		}
	}
}
