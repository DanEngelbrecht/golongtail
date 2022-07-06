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

	executeCommandLine("put", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.json", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("put", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.json", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("put", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.json", "--storage-uri", fsBlobPathPrefix+"/storage")

	// Kill the index file so we can do init again
	store, _ := longtailstorelib.CreateBlobStoreForURI(fsBlobPathPrefix)
	client, _ := store.NewClient(context.Background())
	defer client.Close()
	storeIndexObject, _ := client.NewObject("storage/store.lsi")
	storeIndexObject.Delete()

	// Init the store again to pick up existing blocks
	cmd, err := executeCommandLine("init-remote-store", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current")
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current")
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current")
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)

	storeIndexObject.Delete()
	emptyStoreIndex, _ := longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
	defer emptyStoreIndex.Dispose()
	emptyStoreIndexBytes, _ := longtaillib.WriteStoreIndexToBuffer(emptyStoreIndex)
	defer emptyStoreIndexBytes.Dispose()
	storeIndexObject.Write(emptyStoreIndexBytes.ToBuffer())

	// Force rebuilding the index even though it exists
	cmd, err = executeCommandLine("init-remote-store", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current")
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current")
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current")
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}
