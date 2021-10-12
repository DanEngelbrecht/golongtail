package commands

import (
	"io/ioutil"
	"testing"
)

func TestGet(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)

	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--get-config-path", fsBlobPathPrefix+"/index/v1.json")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--get-config-path", fsBlobPathPrefix+"/index/v2.json")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--get-config-path", fsBlobPathPrefix+"/index/v3.json")

	cmd, err := executeCommandLine("get", "--get-config-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("get", "--get-config-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("get", "--get-config-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}

func TestGetWithVersionLSI(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)

	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--get-config-path", fsBlobPathPrefix+"/index/v1.json", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v1.lsi")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--get-config-path", fsBlobPathPrefix+"/index/v2.json", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v2.lsi")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--get-config-path", fsBlobPathPrefix+"/index/v3.json", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v3.lsi")

	cmd, err := executeCommandLine("get", "--get-config-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("get", "--get-config-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("get", "--get-config-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}

func TestGetWithCache(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--get-config-path", fsBlobPathPrefix+"/index/v1.json")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--get-config-path", fsBlobPathPrefix+"/index/v2.json")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--get-config-path", fsBlobPathPrefix+"/index/v3.json")

	cmd, err := executeCommandLine("get", "--get-config-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("get", "--get-config-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("get", "--get-config-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}

func TestGetWithLSIAndCache(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--get-config-path", fsBlobPathPrefix+"/index/v1.json", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v1.lsi")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--get-config-path", fsBlobPathPrefix+"/index/v2.json", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v2.lsi")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--get-config-path", fsBlobPathPrefix+"/index/v3.json", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v3.lsi")

	cmd, err := executeCommandLine("get", "--get-config-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("get", "--get-config-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("get", "--get-config-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}
