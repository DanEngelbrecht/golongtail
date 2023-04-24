package commands

import (
	"os"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/stretchr/testify/assert"
)

func TestPruneIndex(t *testing.T) {
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

	lsis, err := longtailutils.GetObjectsByURI(fsBlobPathPrefix+"/storage", "store", ".lsi")
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(lsis))
	storeIndexName := lsis[0].Name

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--store-index-path", fsBlobPathPrefix+"/"+storeIndexName)
	assert.Equal(t, nil, err, cmd)

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NotEqual(t, nil, err, cmd)
}

func TestPruneIndexWithLSI(t *testing.T) {
	testPath, _ := os.MkdirTemp("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v1.lsi")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v2.lsi")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v3.lsi")

	sourceFilesContent := []byte(
		fsBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsBlobPathPrefix + "/index/v2.lvi" + "\n")
	longtailutils.WriteToURI(fsBlobPathPrefix+"/files.txt", sourceFilesContent)

	lsiFilesContent := []byte(
		fsBlobPathPrefix + "/index/v1.lsi" + "\n" +
			fsBlobPathPrefix + "/index/v2.lsi" + "\n")
	longtailutils.WriteToURI(fsBlobPathPrefix+"/files-lsi.txt", lsiFilesContent)

	lsis, err := longtailutils.GetObjectsByURI(fsBlobPathPrefix+"/storage", "store", ".lsi")
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(lsis))
	storeIndexName := lsis[0].Name

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--version-local-store-index-paths", testPath+"/files-lsi.txt", "--store-index-path", fsBlobPathPrefix+"/"+storeIndexName)
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err == nil {
		t.Errorf("%s: %s", cmd, err)
	}
}

func TestPruneIndexWithLSIAndWriteLSI(t *testing.T) {
	testPath, _ := os.MkdirTemp("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v1.lsi")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v2.lsi")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v3.lsi")

	sourceFilesContent := []byte(
		fsBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsBlobPathPrefix + "/index/v2.lvi" + "\n")
	longtailutils.WriteToURI(fsBlobPathPrefix+"/files.txt", sourceFilesContent)

	lsiFilesContent := []byte(
		fsBlobPathPrefix + "/index/v1.lsi" + "\n" +
			fsBlobPathPrefix + "/index/v2.lsi" + "\n")
	longtailutils.WriteToURI(fsBlobPathPrefix+"/files-lsi.txt", lsiFilesContent)

	lsis, err := longtailutils.GetObjectsByURI(fsBlobPathPrefix+"/storage", "store", ".lsi")
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(lsis))
	storeIndexName := lsis[0].Name

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--version-local-store-index-paths", testPath+"/files-lsi.txt", "--store-index-path", fsBlobPathPrefix+"/"+storeIndexName, "--write-version-local-store-index")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err == nil {
		t.Errorf("%s: %s", cmd, err)
	}
}

func TestPruneIndexDryRun(t *testing.T) {
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

	lsis, err := longtailutils.GetObjectsByURI(fsBlobPathPrefix+"/storage", "store", ".lsi")
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(lsis))
	storeIndexName := lsis[0].Name

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--store-index-path", fsBlobPathPrefix+"/"+storeIndexName, "--dry-run")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
}

func TestPruneIndexWithLSIDryRun(t *testing.T) {
	testPath, _ := os.MkdirTemp("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v1.lsi")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v2.lsi")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v3.lsi")

	sourceFilesContent := []byte(
		fsBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsBlobPathPrefix + "/index/v2.lvi" + "\n")
	longtailutils.WriteToURI(fsBlobPathPrefix+"/files.txt", sourceFilesContent)

	lsiFilesContent := []byte(
		fsBlobPathPrefix + "/index/v1.lsi" + "\n" +
			fsBlobPathPrefix + "/index/v2.lsi" + "\n")
	longtailutils.WriteToURI(fsBlobPathPrefix+"/files-lsi.txt", lsiFilesContent)

	lsis, err := longtailutils.GetObjectsByURI(fsBlobPathPrefix+"/storage", "store", ".lsi")
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(lsis))
	storeIndexName := lsis[0].Name

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--version-local-store-index-paths", testPath+"/files-lsi.txt", "--store-index-path", fsBlobPathPrefix+"/"+storeIndexName, "--dry-run")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
}

func TestPruneIndexWithLSIAndWriteLSIDryRun(t *testing.T) {
	testPath, _ := os.MkdirTemp("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v1.lsi")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v2.lsi")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v3.lsi")

	sourceFilesContent := []byte(
		fsBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsBlobPathPrefix + "/index/v2.lvi" + "\n")
	longtailutils.WriteToURI(fsBlobPathPrefix+"/files.txt", sourceFilesContent)

	lsiFilesContent := []byte(
		fsBlobPathPrefix + "/index/v1.lsi" + "\n" +
			fsBlobPathPrefix + "/index/v2.lsi" + "\n")
	longtailutils.WriteToURI(fsBlobPathPrefix+"/files-lsi.txt", lsiFilesContent)

	lsis, err := longtailutils.GetObjectsByURI(fsBlobPathPrefix+"/storage", "store", ".lsi")
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(lsis))
	storeIndexName := lsis[0].Name

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--version-local-store-index-paths", testPath+"/files-lsi.txt", "--store-index-path", fsBlobPathPrefix+"/"+storeIndexName, "--write-version-local-store-index", "--dry-run")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
}
