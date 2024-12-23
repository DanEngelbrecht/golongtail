package commands

import (
	"os"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestCreateVersionStoreIndex(t *testing.T) {
	testPath, _ := os.MkdirTemp("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")

	cmd, err := executeCommandLine("create-version-store-index", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v1.lsi", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
	cmd, err = executeCommandLine("create-version-store-index", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v2.lsi", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
	cmd, err = executeCommandLine("create-version-store-index", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v3.lsi", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v1.lsi")
	assert.NoError(t, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v2.lsi")
	assert.NoError(t, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v3.lsi")
	assert.NoError(t, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}
