package commands

import (
	"io/ioutil"
	"testing"
)

func TestPrintStoreIndex(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")

	cmd, err := executeCommandLine("print-store", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	cmd, err = executeCommandLine("print-store", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--compact")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	cmd, err = executeCommandLine("print-store", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--details")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	cmd, err = executeCommandLine("print-store", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--compact", "--details")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
}
