package commands

import (
	"os"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestUnpack(t *testing.T) {

	testPath, _ := os.MkdirTemp("", "test")
	createVersionData(t, testPath)
	executeCommandLine("pack", "--source-path", testPath+"/version/v1", "--target-path", testPath+"/index/v1.la")
	executeCommandLine("pack", "--source-path", testPath+"/version/v2", "--target-path", testPath+"/index/v2.la")
	executeCommandLine("pack", "--source-path", testPath+"/version/v3", "--target-path", testPath+"/index/v3.la")

	cmd, err := executeCommandLine("unpack", "--source-path", testPath+"/index/v1.la", "--target-path", testPath+"/version/current")
	assert.NoError(t, err, cmd)
	validateContent(t, testPath, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("unpack", "--source-path", testPath+"/index/v2.la", "--target-path", testPath+"/version/current")
	assert.NoError(t, err, cmd)
	validateContent(t, testPath, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("unpack", "--source-path", testPath+"/index/v3.la", "--target-path", testPath+"/version/current")
	assert.NoError(t, err, cmd)
	validateContent(t, testPath, "version/current", v3FilesCreate)
}

func TestUnpackWithValidate(t *testing.T) {

	testPath, _ := os.MkdirTemp("", "test")
	createVersionData(t, testPath)
	executeCommandLine("pack", "--source-path", testPath+"/version/v1", "--target-path", testPath+"/index/v1.la")
	executeCommandLine("pack", "--source-path", testPath+"/version/v2", "--target-path", testPath+"/index/v2.la")
	executeCommandLine("pack", "--source-path", testPath+"/version/v3", "--target-path", testPath+"/index/v3.la")

	cmd, err := executeCommandLine("unpack", "--source-path", testPath+"/index/v1.la", "--target-path", testPath+"/version/current", "--validate")
	assert.NoError(t, err, cmd)
	validateContent(t, testPath, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("unpack", "--source-path", testPath+"/index/v2.la", "--target-path", testPath+"/version/current", "--validate")
	assert.NoError(t, err, cmd)
	validateContent(t, testPath, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("unpack", "--source-path", testPath+"/index/v3.la", "--target-path", testPath+"/version/current", "--validate")
	assert.NoError(t, err, cmd)
	validateContent(t, testPath, "version/current", v3FilesCreate)
}
