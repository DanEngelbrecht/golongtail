package commands

import (
	"os"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestPrintStoreIndex(t *testing.T) {
	testPath, _ := os.MkdirTemp("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")

	cmd, err := executeCommandLine("print-store", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi")
	assert.NoError(t, err, cmd)
	cmd, err = executeCommandLine("print-store", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--compact")
	assert.NoError(t, err, cmd)
	cmd, err = executeCommandLine("print-store", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--details")
	assert.NoError(t, err, cmd)
	cmd, err = executeCommandLine("print-store", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--compact", "--details")
	assert.NoError(t, err, cmd)
}
