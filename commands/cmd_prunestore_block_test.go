package commands

import (
	"context"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/stretchr/testify/assert"
)

func TestPruneStoreBlocks(t *testing.T) {
	testPath := t.TempDir()
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")

	sourceFilesContent := []byte(
		fsBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsBlobPathPrefix + "/index/v2.lvi" + "\n")
	longtailutils.WriteToURI(fsBlobPathPrefix+"/files.txt", sourceFilesContent)

	lsis, err := longtailutils.GetObjectsByURI(fsBlobPathPrefix+"/storage", "store", ".lsi")
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(lsis))
	storeIndexName := lsis[0].Name

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--store-index-path", fsBlobPathPrefix+"/"+storeIndexName)
	assert.Equal(t, nil, err, cmd)

	blobStore, err := longtailstorelib.CreateBlobStoreForURI(fsBlobPathPrefix)
	assert.Equal(t, nil, err)

	blobClient, err := blobStore.NewClient(context.Background())
	assert.Equal(t, nil, err)
	defer blobClient.Close()

	blobObjects, err := blobClient.GetObjects("storage/chunks", "")
	assert.Equal(t, nil, err)

	assert.Equal(t, 3, len(blobObjects))

	cmd, err = executeCommandLine("prune-store-blocks", "--store-index-path", fsBlobPathPrefix+"/"+storeIndexName, "--blocks-root-path", fsBlobPathPrefix+"/storage/chunks")
	assert.Equal(t, nil, err, cmd)

	blobObjects, err = blobClient.GetObjects("storage/chunks", "")
	assert.Equal(t, nil, err)

	assert.Equal(t, 2, len(blobObjects))
}

func TestPruneStoreBlocksDryRun(t *testing.T) {
	testPath := t.TempDir()
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")

	sourceFilesContent := []byte(
		fsBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsBlobPathPrefix + "/index/v2.lvi" + "\n")
	longtailutils.WriteToURI(fsBlobPathPrefix+"/files.txt", sourceFilesContent)

	lsis, err := longtailutils.GetObjectsByURI(fsBlobPathPrefix+"/storage", "store", ".lsi")
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(lsis))
	storeIndexName := lsis[0].Name

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--store-index-path", fsBlobPathPrefix+"/"+storeIndexName)
	assert.Equal(t, nil, err, cmd)

	blobStore, err := longtailstorelib.CreateBlobStoreForURI(fsBlobPathPrefix)
	assert.Equal(t, nil, err)

	blobClient, err := blobStore.NewClient(context.Background())
	assert.Equal(t, nil, err)
	defer blobClient.Close()

	blobObjects, err := blobClient.GetObjects("storage/chunks", "")
	assert.Equal(t, nil, err)

	assert.Equal(t, 3, len(blobObjects))

	cmd, err = executeCommandLine("prune-store-blocks", "--store-index-path", fsBlobPathPrefix+"/"+storeIndexName, "--blocks-root-path", fsBlobPathPrefix+"/storage/chunks", "--dry-run")
	assert.Equal(t, nil, err, cmd)

	blobObjects, err = blobClient.GetObjects("storage/chunks", "")
	assert.Equal(t, nil, err)

	assert.Equal(t, 3, len(blobObjects))
}
