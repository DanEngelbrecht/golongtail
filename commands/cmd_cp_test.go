package commands

import (
	"context"
	"io/ioutil"
	"runtime"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/alecthomas/kong"
)

func runCp(t *testing.T, storageURI string, versionIndexPath string, sourcePath string, targetPath string, optionalCachePath string) {
	parser, err := kong.New(&Cli)
	if err != nil {
		t.Errorf("kong.New(Cli) failed with %s", err)
	}
	args := []string{
		"cp",
		"--storage-uri", storageURI,
		"--version-index-path", versionIndexPath,
		sourcePath,
		targetPath,
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

func validateFileContent(t *testing.T, baseURI string, sourcePath string, expectedContent string) {
	store, _ := longtailstorelib.CreateBlobStoreForURI(baseURI)
	client, _ := store.NewClient(context.Background())
	defer client.Close()
	o, _ := client.NewObject(sourcePath)
	d, _ := o.Read()
	s := string(d)
	if s != expectedContent {
		t.Errorf("`%s` content `%s` does not match `%s`", sourcePath, s, expectedContent)
	}
}

func TestCp(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", "", "")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", "", "")

	runCp(t, fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v1.lvi", "folder/abitoftextinasubfolder.txt", fsBlobPathPrefix+"/current/abitoftextinasubfolder.txt", "")
	validateFileContent(t, fsBlobPathPrefix, "current/abitoftextinasubfolder.txt", v1FilesCreate["folder/abitoftextinasubfolder.txt"])

	runCp(t, fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v2.lvi", "stuff.txt", fsBlobPathPrefix+"/current/stuff.txt", "")
	validateFileContent(t, fsBlobPathPrefix, "current/stuff.txt", v2FilesCreate["stuff.txt"])

	runCp(t, fsBlobPathPrefix+"/storage", fsBlobPathPrefix+"/index/v3.lvi", "morestuff.txt", fsBlobPathPrefix+"/current/morestuff.txt", "")
	validateFileContent(t, fsBlobPathPrefix, "current/morestuff.txt", v3FilesCreate["morestuff.txt"])
}
