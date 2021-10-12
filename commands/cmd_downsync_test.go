package commands

import (
	"io/ioutil"
	"testing"

	"github.com/alecthomas/kong"
)

func downsyncVersion(t *testing.T, sourcePath string, targetPath string, storageURI string, optionalVersionLocalStoreIndexPath string, optionalCachePath string) {
	parser, err := kong.New(&Cli)
	if err != nil {
		t.Errorf("kong.New(Cli) failed with %s", err)
	}
	args := []string{
		"downsync",
		"--source-path", sourcePath,
		"--target-path", targetPath,
		"--storage-uri", storageURI,
	}
	if optionalVersionLocalStoreIndexPath != "" {
		args = append(args, "--version-local-store-index-path")
		args = append(args, optionalVersionLocalStoreIndexPath)
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
		NumWorkerCount: 4,
	}
	err = ctx.Run(context)
	if err != nil {
		t.Errorf("ctx.Run(context) failed with %s", err)
	}
}

func TestDownsync(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", "", "")

	downsyncVersion(t, fsBlobPathPrefix+"/index/v1.lvi", testPath+"/version/current", fsBlobPathPrefix+"/storage", "", "")
	if !validateContent(fsBlobPathPrefix, "version/current", v1FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v1FilesCreate)
	}
	downsyncVersion(t, fsBlobPathPrefix+"/index/v2.lvi", testPath+"/version/current", fsBlobPathPrefix+"/storage", "", "")
	if !validateContent(fsBlobPathPrefix, "version/current", v2FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v2FilesCreate)
	}
	downsyncVersion(t, fsBlobPathPrefix+"/index/v3.lvi", testPath+"/version/current", fsBlobPathPrefix+"/storage", "", "")
	if !validateContent(fsBlobPathPrefix, "version/current", v3FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v3FilesCreate)
	}
}

func TestDownsyncWithVersionLSI(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v1.lsi", "")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v2.lsi", "")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v3.lsi", "")

	downsyncVersion(t, fsBlobPathPrefix+"/index/v1.lvi", testPath+"/version/current", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v1.lsi", "")
	if !validateContent(fsBlobPathPrefix, "version/current", v1FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v1FilesCreate)
	}
	downsyncVersion(t, fsBlobPathPrefix+"/index/v2.lvi", testPath+"/version/current", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v2.lsi", "")
	if !validateContent(fsBlobPathPrefix, "version/current", v2FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v2FilesCreate)
	}
	downsyncVersion(t, fsBlobPathPrefix+"/index/v3.lvi", testPath+"/version/current", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v3.lsi", "")
	if !validateContent(fsBlobPathPrefix, "version/current", v3FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v3FilesCreate)
	}
}

func TestDownsyncWithCache(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", "", "")

	downsyncVersion(t, fsBlobPathPrefix+"/index/v1.lvi", testPath+"/version/current", fsBlobPathPrefix+"/storage", "", testPath+"/cache")
	if !validateContent(fsBlobPathPrefix, "version/current", v1FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v1FilesCreate)
	}
	downsyncVersion(t, fsBlobPathPrefix+"/index/v2.lvi", testPath+"/version/current", fsBlobPathPrefix+"/storage", "", testPath+"/cache")
	if !validateContent(fsBlobPathPrefix, "version/current", v2FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v2FilesCreate)
	}
	downsyncVersion(t, fsBlobPathPrefix+"/index/v3.lvi", testPath+"/version/current", fsBlobPathPrefix+"/storage", "", testPath+"/cache")
	if !validateContent(fsBlobPathPrefix, "version/current", v3FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v3FilesCreate)
	}
}

func TestDownsyncWithLSIAndCache(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v1.lsi", "")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v2.lsi", "")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v3.lsi", "")

	downsyncVersion(t, fsBlobPathPrefix+"/index/v1.lvi", testPath+"/version/current", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v1.lsi", testPath+"/cache")
	if !validateContent(fsBlobPathPrefix, "version/current", v1FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v1FilesCreate)
	}
	downsyncVersion(t, fsBlobPathPrefix+"/index/v2.lvi", testPath+"/version/current", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v2.lsi", testPath+"/cache")
	if !validateContent(fsBlobPathPrefix, "version/current", v2FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v2FilesCreate)
	}
	downsyncVersion(t, fsBlobPathPrefix+"/index/v3.lvi", testPath+"/version/current", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v3.lsi", testPath+"/cache")
	if !validateContent(fsBlobPathPrefix, "version/current", v3FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v3FilesCreate)
	}
}
