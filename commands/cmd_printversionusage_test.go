package commands

import (
	"io/ioutil"
	"runtime"
	"testing"

	"github.com/alecthomas/kong"
)

func runPrintVersionUsage(t *testing.T, storageURI string, versionIndexPath string, optionalCachePath string) {
	parser, err := kong.New(&Cli)
	if err != nil {
		t.Errorf("kong.New(Cli) failed with %s", err)
	}
	args := []string{
		"print-version-usage",
		"--storage-uri", storageURI,
		"--version-index-path", versionIndexPath,
	}
	if optionalCachePath != optionalCachePath {
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

func TestPrintVersionUsage(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", "", "")

	runPrintVersionUsage(t, fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v1.lvi", "")
	runPrintVersionUsage(t, fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v2.lvi", "")
	runPrintVersionUsage(t, fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v3.lvi", "")

	runPrintVersionUsage(t, fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v1.lvi", testPath+"/cache")
	runPrintVersionUsage(t, fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v2.lvi", testPath+"/cache")
	runPrintVersionUsage(t, fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v3.lvi", testPath+"/cache")
}
