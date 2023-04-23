package commands

import (
	"context"
	"runtime"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
)

func runInitRemoteStore(t *testing.T, storageURI string) {
	parser, err := kong.New(&Cli)
	assert.Equal(t, nil, err)
	args := []string{
		"init-remote-store",
		"--storage-uri", storageURI,
	}
	ctx, err := parser.Parse(args)
	assert.Equal(t, nil, err)

	context := &Context{
		NumWorkerCount: runtime.NumCPU(),
	}
	err = ctx.Run(context)
	assert.Equal(t, nil, err)
}

func TestInitRemoteStore(t *testing.T) {
	testPath := t.TempDir()
	fsBlobPathPrefix := "fsblob://" + testPath

	// Init an empty store
	runInitRemoteStore(t, fsBlobPathPrefix+"/storage")

	createVersionData(t, fsBlobPathPrefix)

	executeCommandLine("put", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.json", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("put", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.json", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("put", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.json", "--storage-uri", fsBlobPathPrefix+"/storage")

	// Kill the index file so we can do init again
	lsis, err := longtailutils.GetObjectsByURI(fsBlobPathPrefix+"/storage", "store", ".lsi")
	assert.Equal(t, nil, err)
	for _, lsi := range lsis {
		longtailutils.DeleteByURI(fsBlobPathPrefix + "/" + lsi.Name)
	}

	// Init the store again to pick up existing blocks
	cmd, err := executeCommandLine("init-remote-store", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.Equal(t, nil, err, cmd)

	executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current")
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current")
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current")
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)

	lsis, err = longtailutils.GetObjectsByURI(fsBlobPathPrefix+"/storage", "store", ".lsi")
	assert.Equal(t, nil, err)
	for _, lsi := range lsis {
		longtailutils.DeleteByURI(fsBlobPathPrefix + "/" + lsi.Name)
	}

	emptyStoreIndex, _ := longtaillib.CreateStoreIndexFromBlocks([]longtaillib.Longtail_BlockIndex{})
	defer emptyStoreIndex.Dispose()
	emptyStoreIndexBytes, _ := longtaillib.WriteStoreIndexToBuffer(emptyStoreIndex)
	defer emptyStoreIndexBytes.Dispose()

	store, _ := longtailstorelib.CreateBlobStoreForURI(fsBlobPathPrefix)
	client, _ := store.NewClient(context.Background())
	defer client.Close()
	storeIndexObject, _ := client.NewObject("storage/store.lsi")
	storeIndexObject.Write(emptyStoreIndexBytes.ToBuffer())

	// Force rebuilding the index even though it exists
	cmd, err = executeCommandLine("init-remote-store", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.Equal(t, nil, err, cmd)

	executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current")
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current")
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current")
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}
