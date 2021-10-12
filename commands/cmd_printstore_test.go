package commands

import (
	"io/ioutil"
	"runtime"
	"testing"

	"github.com/alecthomas/kong"
)

func runPrintStore(t *testing.T, storeIndexPath string, compact bool, details bool) {
	parser, err := kong.New(&Cli)
	if err != nil {
		t.Errorf("kong.New(Cli) failed with %s", err)
	}
	args := []string{
		"print-store",
		"--store-index-path", storeIndexPath,
	}
	if compact {
		args = append(args, "--compact")
	}
	if details {
		args = append(args, "--details")
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

func TestPrintStoreIndex(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", "", "")

	runPrintStore(t, fsBlobPathPrefix+"/storage/store.lsi", false, false)
	runPrintStore(t, fsBlobPathPrefix+"/storage/store.lsi", true, false)
	runPrintStore(t, fsBlobPathPrefix+"/storage/store.lsi", false, true)
	runPrintStore(t, fsBlobPathPrefix+"/storage/store.lsi", true, true)
}
