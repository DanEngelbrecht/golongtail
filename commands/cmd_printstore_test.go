package commands

import (
	"io/ioutil"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/stretchr/testify/assert"
)

func TestPrintStoreIndex(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")

	lsis, err := longtailutils.GetObjectsByURI(fsBlobPathPrefix+"/storage", "store", ".lsi")
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(lsis))
	lsiName := lsis[0].Name

	cmd, err := executeCommandLine("print-store", "--store-index-path", fsBlobPathPrefix+"/"+lsiName)
	assert.Equal(t, nil, err, cmd)

	cmd, err = executeCommandLine("print-store", "--store-index-path", fsBlobPathPrefix+"/"+lsiName, "--compact")
	assert.Equal(t, nil, err, cmd)

	cmd, err = executeCommandLine("print-store", "--store-index-path", fsBlobPathPrefix+"/"+lsiName, "--details")
	assert.Equal(t, nil, err, cmd)

	cmd, err = executeCommandLine("print-store", "--store-index-path", fsBlobPathPrefix+"/"+lsiName, "--compact", "--details")
	assert.Equal(t, nil, err, cmd)
}
