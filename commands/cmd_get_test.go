package commands

import (
	"io/ioutil"
	"runtime"
	"testing"

	"github.com/alecthomas/kong"
)

func getVersion(t *testing.T, getConfigPath string, targetPath string, optionalCachePath string) {
	parser, err := kong.New(&Cli)
	if err != nil {
		t.Errorf("kong.New(Cli) failed with %s", err)
	}
	args := []string{
		"get",
		"--get-config-path", getConfigPath,
		"--target-path", targetPath,
	}
	if optionalCachePath != "" {
		args = append(args, "--cache-path")
		args = append(args, optionalCachePath)
	}
	ctx, err := parser.Parse(args)
	if err != nil {
		t.Errorf("parser.Parse() failed with %s", err)
	}

	context := &Context{
		NumWorkerCount: runtime.NumCPU(),
	}
	err = ctx.Run(context)
	if err != nil {
		t.Errorf("ctx.Run(context) failed with %s", err)
	}
}

func TestGet(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", "", fsBlobPathPrefix+"/index/v1.json")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", "", fsBlobPathPrefix+"/index/v2.json")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", "", fsBlobPathPrefix+"/index/v3.json")

	getVersion(t, fsBlobPathPrefix+"/index/v1.json", testPath+"/version/current", "")
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	getVersion(t, fsBlobPathPrefix+"/index/v2.json", testPath+"/version/current", "")
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	getVersion(t, fsBlobPathPrefix+"/index/v3.json", testPath+"/version/current", "")
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}

func TestGetWithVersionLSI(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v1.lsi", fsBlobPathPrefix+"/index/v1.json")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v2.lsi", fsBlobPathPrefix+"/index/v2.json")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v3.lsi", fsBlobPathPrefix+"/index/v3.json")

	getVersion(t, fsBlobPathPrefix+"/index/v1.json", testPath+"/version/current", "")
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	getVersion(t, fsBlobPathPrefix+"/index/v2.json", testPath+"/version/current", "")
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	getVersion(t, fsBlobPathPrefix+"/index/v3.json", testPath+"/version/current", "")
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}

func TestGetWithCache(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", "", fsBlobPathPrefix+"/index/v1.json")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", "", fsBlobPathPrefix+"/index/v2.json")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", "", fsBlobPathPrefix+"/index/v3.json")

	getVersion(t, fsBlobPathPrefix+"/index/v1.json", testPath+"/version/current", testPath+"/cache")
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	getVersion(t, fsBlobPathPrefix+"/index/v2.json", testPath+"/version/current", testPath+"/cache")
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	getVersion(t, fsBlobPathPrefix+"/index/v3.json", testPath+"/version/current", testPath+"/cache")
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}

func TestGetWithLSIAndCache(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v1.lsi", fsBlobPathPrefix+"/index/v1.json")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v2.lsi", fsBlobPathPrefix+"/index/v2.json")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v3.lsi", fsBlobPathPrefix+"/index/v3.json")

	getVersion(t, fsBlobPathPrefix+"/index/v1.json", testPath+"/version/current", testPath+"/cache")
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	getVersion(t, fsBlobPathPrefix+"/index/v2.json", testPath+"/version/current", testPath+"/cache")
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	getVersion(t, fsBlobPathPrefix+"/index/v3.json", testPath+"/version/current", testPath+"/cache")
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}
