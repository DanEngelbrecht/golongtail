package commands

import (
	"io/ioutil"
	"testing"
)

func TestUnpack(t *testing.T) {

	testPath, _ := ioutil.TempDir("", "test")
	createVersionData(t, testPath)
	executeCommandLine("pack", "--source-path", testPath+"/version/v1", "--target-path", testPath+"/index/v1.la")
	executeCommandLine("pack", "--source-path", testPath+"/version/v2", "--target-path", testPath+"/index/v2.la")
	executeCommandLine("pack", "--source-path", testPath+"/version/v3", "--target-path", testPath+"/index/v3.la")

	cmd, err := executeCommandLine("unpack", "--source-path", testPath+"/index/v1.la", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, testPath, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("unpack", "--source-path", testPath+"/index/v2.la", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, testPath, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("unpack", "--source-path", testPath+"/index/v3.la", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, testPath, "version/current", v3FilesCreate)
}

func TestUnpackWithValidate(t *testing.T) {

	testPath, _ := ioutil.TempDir("", "test")
	createVersionData(t, testPath)
	executeCommandLine("pack", "--source-path", testPath+"/version/v1", "--target-path", testPath+"/index/v1.la")
	executeCommandLine("pack", "--source-path", testPath+"/version/v2", "--target-path", testPath+"/index/v2.la")
	executeCommandLine("pack", "--source-path", testPath+"/version/v3", "--target-path", testPath+"/index/v3.la")

	cmd, err := executeCommandLine("unpack", "--source-path", testPath+"/index/v1.la", "--target-path", testPath+"/version/current", "--validate")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, testPath, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("unpack", "--source-path", testPath+"/index/v2.la", "--target-path", testPath+"/version/current", "--validate")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, testPath, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("unpack", "--source-path", testPath+"/index/v3.la", "--target-path", testPath+"/version/current", "--validate")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, testPath, "version/current", v3FilesCreate)
}
