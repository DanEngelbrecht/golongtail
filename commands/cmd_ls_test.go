package commands

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLs(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")

	cmd, err := executeCommandLine("ls", "--version-index-path", fsBlobPathPrefix+"/index/v1.lvi", ".")
	assert.Equal(t, nil, err, cmd)

	cmd, err = executeCommandLine("ls", "--version-index-path", fsBlobPathPrefix+"/index/v2.lvi", "folder")
	assert.Equal(t, nil, err, cmd)

	cmd, err = executeCommandLine("ls", "--version-index-path", fsBlobPathPrefix+"/index/v3.lvi", "folder2")
	assert.Equal(t, nil, err, cmd)
}
