package commands

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPack(t *testing.T) {

	testPath, _ := ioutil.TempDir("", "test")
	createVersionData(t, testPath)
	cmd, err := executeCommandLine("pack", "--source-path", testPath+"/version/v1", "--target-path", testPath+"/index/v1.la")
	assert.Equal(t, err, nil, cmd)

	cmd, err = executeCommandLine("pack", "--source-path", testPath+"/version/v2", "--target-path", testPath+"/index/v2.la")
	assert.Equal(t, err, nil, cmd)

	cmd, err = executeCommandLine("pack", "--source-path", testPath+"/version/v3", "--target-path", testPath+"/index/v3.la")
	assert.Equal(t, err, nil, cmd)
}

func TestPackCompressionAlgos(t *testing.T) {

	testPath, _ := ioutil.TempDir("", "test")
	createVersionData(t, testPath)
	cmd, err := executeCommandLine("pack", "--source-path", testPath+"/version/v1", "--target-path", testPath+"/index/v1.la", "--compression-algorithm", "none")
	assert.Equal(t, err, nil, cmd)

	cmd, err = executeCommandLine("pack", "--source-path", testPath+"/version/v2", "--target-path", testPath+"/index/v2.la", "--compression-algorithm", "brotli_min")
	assert.Equal(t, err, nil, cmd)

	cmd, err = executeCommandLine("pack", "--source-path", testPath+"/version/v3", "--target-path", testPath+"/index/v3.la", "--compression-algorithm", "zstd_max")
	assert.Equal(t, err, nil, cmd)
}
