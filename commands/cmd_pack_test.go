package commands

import (
	"os"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestPack(t *testing.T) {

	testPath, _ := os.MkdirTemp("", "test")
	createVersionData(t, testPath)
	cmd, err := executeCommandLine("pack", "--source-path", testPath+"/version/v1", "--target-path", testPath+"/index/v1.la")
	assert.NoError(t, err, cmd)
	cmd, err = executeCommandLine("pack", "--source-path", testPath+"/version/v2", "--target-path", testPath+"/index/v2.la")
	assert.NoError(t, err, cmd)
	cmd, err = executeCommandLine("pack", "--source-path", testPath+"/version/v3", "--target-path", testPath+"/index/v3.la")
	assert.NoError(t, err, cmd)
}

func TestPackCompressionAlgos(t *testing.T) {

	testPath, _ := os.MkdirTemp("", "test")
	createVersionData(t, testPath)
	cmd, err := executeCommandLine("pack", "--source-path", testPath+"/version/v1", "--target-path", testPath+"/index/v1.la", "--compression-algorithm", "none")
	assert.NoError(t, err, cmd)
	cmd, err = executeCommandLine("pack", "--source-path", testPath+"/version/v2", "--target-path", testPath+"/index/v2.la", "--compression-algorithm", "brotli_min")
	assert.NoError(t, err, cmd)
	cmd, err = executeCommandLine("pack", "--source-path", testPath+"/version/v3", "--target-path", testPath+"/index/v3.la", "--compression-algorithm", "zstd_max")
	assert.NoError(t, err, cmd)
}
