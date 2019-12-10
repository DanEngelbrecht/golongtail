package golongtail

import "testing"

func TestChunkFolder(t *testing.T) {
	expected := int32(194061)
	if ret := ChunkFolder("C:\\Temp\\longtail\\local\\WinEditor\\git2f7f84a05fc290c717c8b5c0e59f8121481151e6_Win64_Editor"); ret != expected {
		t.Errorf("ChunkFolder() = %q, want %q", ret, expected)
	}
}
