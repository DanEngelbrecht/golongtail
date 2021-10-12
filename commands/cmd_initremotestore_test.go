package commands

import (
	"context"
	"io/ioutil"
	"runtime"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/alecthomas/kong"
)

func runInitRemoteStore(t *testing.T, storageURI string) {
	parser, err := kong.New(&Cli)
	if err != nil {
		t.Errorf("kong.New(Cli) failed with %s", err)
	}
	args := []string{
		"init-remote-store",
		"--storage-uri", storageURI,
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

func TestInitRemoteStore(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath

	// Init an empty store
	runInitRemoteStore(t, fsBlobPathPrefix+"/storage")

	createVersionData(t, fsBlobPathPrefix)
	upsyncVersion(t, testPath+"/version/v1", fsBlobPathPrefix+"/index/v1.lvi", fsBlobPathPrefix+"/storage", "", fsBlobPathPrefix+"/index/v1.json")
	upsyncVersion(t, testPath+"/version/v2", fsBlobPathPrefix+"/index/v2.lvi", fsBlobPathPrefix+"/storage", "", fsBlobPathPrefix+"/index/v2.json")
	upsyncVersion(t, testPath+"/version/v3", fsBlobPathPrefix+"/index/v3.lvi", fsBlobPathPrefix+"/storage", "", fsBlobPathPrefix+"/index/v3.json")

	// Kill the index file so we can do init again
	store, _ := longtailstorelib.CreateBlobStoreForURI(fsBlobPathPrefix)
	client, _ := store.NewClient(context.Background())
	defer client.Close()
	storeIndexObject, _ := client.NewObject("storage/store.lsi")
	storeIndexObject.Delete()

	// Init the store again to pick up existing blocks
	runInitRemoteStore(t, fsBlobPathPrefix+"/storage")

	getVersion(t, fsBlobPathPrefix+"/index/v1.json", testPath+"/version/current", "")
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	getVersion(t, fsBlobPathPrefix+"/index/v2.json", testPath+"/version/current", "")
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	getVersion(t, fsBlobPathPrefix+"/index/v3.json", testPath+"/version/current", "")
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)

	storeIndexObject.Delete()
	emptyStoreIndex, _ := longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
	emptyStoreIndexBytes, _ := longtaillib.WriteStoreIndexToBuffer(emptyStoreIndex)
	storeIndexObject.Write(emptyStoreIndexBytes)

	// Force rebuilding the index even though it exists
	runInitRemoteStore(t, fsBlobPathPrefix+"/storage")

	getVersion(t, fsBlobPathPrefix+"/index/v1.json", testPath+"/version/current", "")
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	getVersion(t, fsBlobPathPrefix+"/index/v2.json", testPath+"/version/current", "")
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	getVersion(t, fsBlobPathPrefix+"/index/v3.json", testPath+"/version/current", "")
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}
