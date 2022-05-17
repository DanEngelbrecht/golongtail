package commands

import (
	"io/ioutil"
	"testing"
)

func TestGet(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)

	executeCommandLine("put", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.json", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("put", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.json", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("put", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.json", "--storage-uri", fsBlobPathPrefix+"/storage")

	cmd, err := executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}

func TestGetWithVersionLSI(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)

	executeCommandLine("put", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v1.lsi")
	executeCommandLine("put", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v2.lsi")
	executeCommandLine("put", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v3.lsi")

	cmd, err := executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}

func TestGetWithCache(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("put", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.json", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("put", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.json", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("put", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.json", "--storage-uri", fsBlobPathPrefix+"/storage")

	cmd, err := executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}

func TestGetWithLSIAndCache(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("put", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v1.lsi")
	executeCommandLine("put", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v2.lsi")
	executeCommandLine("put", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v3.lsi")

	cmd, err := executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}
