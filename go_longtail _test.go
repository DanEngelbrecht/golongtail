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
	vi := CreateVersionIndexFromFolder("C:\\Temp\\longtail\\local\\WinEditor\\git2f7f84a05fc290c717c8b5c0e59f8121481151e6_Win64_Editor", MakeProgressProxy(progress, &progressData{task: "Indexing", t: t}))
	if vi == nil {
		t.Errorf("CreateVersionIndexFromFolder() = nil, want !nil")
	}
	LongtailFree(unsafe.Pointer(vi))
}

/*
func TestChunkFolder(t *testing.T) {
	expected := int32(194061)
	if ret := ChunkFolder("C:\\Temp\\longtail\\local\\WinEditor\\git2f7f84a05fc290c717c8b5c0e59f8121481151e6_Win64_Editor"); ret != expected {
		t.Errorf("ChunkFolder() = %q, want %q", ret, expected)
	}
}
*/
