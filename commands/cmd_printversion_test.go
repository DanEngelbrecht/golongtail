package commands

import (
	"io/ioutil"
	"runtime"
	"testing"

	"github.com/alecthomas/kong"
)

func runPrintVersion(t *testing.T, versionIndexPath string, compact bool) {
	parser, err := kong.New(&Cli)
	if err != nil {
		t.Errorf("kong.New(Cli) failed with %s", err)
	}
	args := []string{
		"print-version",
		"--version-index-path", versionIndexPath,
	}
	if compact {
		args = append(args, "--compact")
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

func TestPrintVersionIndex(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", "", "")

	runPrintVersion(t, fsBlobPathPrefix+"/index/v1.lvi", false)
	runPrintVersion(t, fsBlobPathPrefix+"/index/v2.lvi", false)
	runPrintVersion(t, fsBlobPathPrefix+"/index/v3.lvi", false)

	runPrintVersion(t, fsBlobPathPrefix+"/index/v1.lvi", true)
	runPrintVersion(t, fsBlobPathPrefix+"/index/v2.lvi", true)
	runPrintVersion(t, fsBlobPathPrefix+"/index/v3.lvi", true)
}
