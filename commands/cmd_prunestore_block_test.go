package commands

import (
	"context"
	"os"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/alecthomas/assert/v2"
)

func TestPruneStoreBlocks(t *testing.T) {
	testPath, _ := os.MkdirTemp("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")

	sourceFilesContent := []byte(
		fsBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsBlobPathPrefix + "/index/v2.lvi" + "\n")
	longtailutils.WriteToURI(fsBlobPathPrefix+"/files.txt", sourceFilesContent)

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi")
	assert.NoError(t, err, cmd)

	blobStore, err := longtailstorelib.CreateBlobStoreForURI(fsBlobPathPrefix)
	assert.NoError(t, err, cmd)

	blobClient, err := blobStore.NewClient(context.Background())
	assert.NoError(t, err, cmd)
	defer blobClient.Close()

	blobObjects, err := blobClient.GetObjects("storage/chunks")
	assert.NoError(t, err, cmd)

	assert.Equal(t, len(blobObjects), 3)

	cmd, err = executeCommandLine("prune-store-blocks", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--blocks-root-path", fsBlobPathPrefix+"/storage/chunks")
	assert.NoError(t, err, cmd)

	blobObjects, err = blobClient.GetObjects("storage/chunks")
	assert.NoError(t, err, "blobClient.GetObjects(%s)", "storage/chunks")
	assert.Equal(t, len(blobObjects), 2)
}

func TestPruneStoreBlocksDryRun(t *testing.T) {
	testPath, _ := os.MkdirTemp("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")

	sourceFilesContent := []byte(
		fsBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsBlobPathPrefix + "/index/v2.lvi" + "\n")
	longtailutils.WriteToURI(fsBlobPathPrefix+"/files.txt", sourceFilesContent)

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi")
	assert.NoError(t, err, cmd)

	blobStore, err := longtailstorelib.CreateBlobStoreForURI(fsBlobPathPrefix)
	assert.NoError(t, err, "longtailstorelib.CreateBlobStoreForURI(%s)", fsBlobPathPrefix)

	blobClient, err := blobStore.NewClient(context.Background())
	assert.NoError(t, err, "blobStore.NewClient")
	defer blobClient.Close()

	blobObjects, err := blobClient.GetObjects("storage/chunks")
	assert.NoError(t, err, "blobClient.GetObjects(%s)", "storage/chunks")

	assert.Equal(t, len(blobObjects), 3)

	cmd, err = executeCommandLine("prune-store-blocks", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--blocks-root-path", fsBlobPathPrefix+"/storage/chunks", "--dry-run")
	assert.NoError(t, err, cmd)

	blobObjects, err = blobClient.GetObjects("storage/chunks")
	assert.NoError(t, err, "blobClient.GetObjects(%s)", "storage/chunks")

	assert.Equal(t, len(blobObjects), 3)
}
