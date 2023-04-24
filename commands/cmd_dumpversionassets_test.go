package commands

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDumpVersionAssets(t *testing.T) {
	testPath, _ := os.MkdirTemp("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")

	cmd, err := executeCommandLine("dump-version-assets", "--version-index-path", fsBlobPathPrefix+"/index/v1.lvi")
	assert.Equal(t, nil, err, cmd)
	cmd, err = executeCommandLine("dump-version-assets", "--version-index-path", fsBlobPathPrefix+"/index/v2.lvi")
	assert.Equal(t, nil, err, cmd)
	cmd, err = executeCommandLine("dump-version-assets", "--version-index-path", fsBlobPathPrefix+"/index/v3.lvi")
	assert.Equal(t, nil, err, cmd)
}

func TestDumpVersionAssetsWithDetails(t *testing.T) {
	testPath, _ := os.MkdirTemp("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")

	cmd, err := executeCommandLine("dump-version-assets", "--version-index-path", fsBlobPathPrefix+"/index/v1.lvi", "--details")
	assert.Equal(t, nil, err, cmd)
	cmd, err = executeCommandLine("dump-version-assets", "--version-index-path", fsBlobPathPrefix+"/index/v2.lvi", "--details")
	assert.Equal(t, nil, err, cmd)
	cmd, err = executeCommandLine("dump-version-assets", "--version-index-path", fsBlobPathPrefix+"/index/v3.lvi", "--details")
	assert.Equal(t, nil, err, cmd)
}
