package commands

import (
	"io/ioutil"
	"testing"
)

func TestPack(t *testing.T) {

	testPath, _ := ioutil.TempDir("", "test")
	createVersionData(t, testPath)
	cmd, err := executeCommandLine("pack", "--source-path", testPath+"/version/v1", "--target-path", testPath+"/index/v1.la")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	cmd, err = executeCommandLine("pack", "--source-path", testPath+"/version/v2", "--target-path", testPath+"/index/v2.la")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	cmd, err = executeCommandLine("pack", "--source-path", testPath+"/version/v3", "--target-path", testPath+"/index/v3.la")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
}

func TestPackCompressionAlgos(t *testing.T) {

	testPath, _ := ioutil.TempDir("", "test")
	createVersionData(t, testPath)
	cmd, err := executeCommandLine("pack", "--source-path", testPath+"/version/v1", "--target-path", testPath+"/index/v1.la", "--compression-algorithm", "none")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	cmd, err = executeCommandLine("pack", "--source-path", testPath+"/version/v2", "--target-path", testPath+"/index/v2.la", "--compression-algorithm", "brotli_min")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	cmd, err = executeCommandLine("pack", "--source-path", testPath+"/version/v3", "--target-path", testPath+"/index/v3.la", "--compression-algorithm", "zstd_max")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
}
