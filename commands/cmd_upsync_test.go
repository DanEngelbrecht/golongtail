package commands

import (
	"io/ioutil"
	"testing"

	"github.com/alecthomas/kong"
)

func upsyncVersion(t *testing.T, sourcePath string, targetPath string, storageURI string, optionalVersionLocalStoreIndexPath string, optionalGetConfigPath string) {
	parser, err := kong.New(&Cli)
	if err != nil {
		t.Errorf("kong.New(Cli) failed with %s", err)
	}
	args := []string{
		"upsync",
		"--source-path", sourcePath,
		"--target-path", targetPath,
		"--storage-uri", storageURI,
	}
	if optionalVersionLocalStoreIndexPath != "" {
		args = append(args, "--version-local-store-index-path")
		args = append(args, optionalVersionLocalStoreIndexPath)
	}
	if optionalGetConfigPath != "" {
		args = append(args, "--get-config-path")
		args = append(args, optionalGetConfigPath)
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

func TestUpsync(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", "", "")
}

func TestUpsyncWithLSI(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v1.lsi", "")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v2.lsi", "")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v3.lsi", "")
}

func TestUpsyncWithGetConfig(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", "", fsBlobPathPrefix+"/index/v1.json")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", "", fsBlobPathPrefix+"/index/v1.json")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", "", fsBlobPathPrefix+"/index/v1.json")
}

func TestUpsyncWithGetConfigAndLSI(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v1.lsi", fsBlobPathPrefix+"/index/v1.json")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v2.lsi", fsBlobPathPrefix+"/index/v1.json")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v3.lsi", fsBlobPathPrefix+"/index/v1.json")
}
