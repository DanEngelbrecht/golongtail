package commands

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
)

func TestPruneStoreBlocks(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
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
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	blobStore, err := longtailstorelib.CreateBlobStoreForURI(fsBlobPathPrefix)
	if err != nil {
		t.Errorf("%s: %s", "longtailstorelib.CreateBlobStoreForURI", err)
	}

	blobClient, err := blobStore.NewClient(context.Background())
	if err != nil {
		t.Errorf("%s: %s", "blobStore.CreateClient", err)
	}
	defer blobClient.Close()

	blobObjects, err := blobClient.GetObjects("storage/chunks")
	if err != nil {
		t.Errorf("%s: %s", "blobClient.NewClient", err)
	}

	if len(blobObjects) != 3 {
		t.Errorf("%d: %d", len(blobObjects), 3)
	}

	cmd, err = executeCommandLine("prune-store-blocks", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--blocks-root-path", fsBlobPathPrefix+"/storage/chunks")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	blobObjects, err = blobClient.GetObjects("storage/chunks")
	if err != nil {
		t.Errorf("%s: %s", "blobClient.GetObjects", err)
	}

	if len(blobObjects) != 2 {
		t.Errorf("%d: %d", len(blobObjects), 2)
	}
}

func TestPruneStoreBlocksDryRun(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
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
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	blobStore, err := longtailstorelib.CreateBlobStoreForURI(fsBlobPathPrefix)
	if err != nil {
		t.Errorf("%s: %s", "longtailstorelib.CreateBlobStoreForURI", err)
	}

	blobClient, err := blobStore.NewClient(context.Background())
	if err != nil {
		t.Errorf("%s: %s", "blobStore.NewClient", err)
	}
	defer blobClient.Close()

	blobObjects, err := blobClient.GetObjects("storage/chunks")
	if err != nil {
		t.Errorf("%s: %s", "blobClient.GetObjects", err)
	}

	if len(blobObjects) != 3 {
		t.Errorf("%d: %d", len(blobObjects), 3)
	}

	cmd, err = executeCommandLine("prune-store-blocks", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--blocks-root-path", fsBlobPathPrefix+"/storage/chunks", "--dry-run")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	blobObjects, err = blobClient.GetObjects("storage/chunks")
	if err != nil {
		t.Errorf("%s: %s", "blobClient.GetObjects", err)
	}

	if len(blobObjects) != 3 {
		t.Errorf("%d: %d", len(blobObjects), 3)
	}
}
