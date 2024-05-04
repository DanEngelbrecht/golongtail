package commands

import (
	"os"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/alecthomas/assert/v2"
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

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi")
	assert.NoError(t, err, cmd)

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.Error(t, err, cmd)
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

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--version-local-store-index-paths", testPath+"/files-lsi.txt", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi")
	assert.NoError(t, err, cmd)

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.Error(t, err, cmd)
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

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--version-local-store-index-paths", testPath+"/files-lsi.txt", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--write-version-local-store-index")
	assert.NoError(t, err, cmd)

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)

	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)

	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.Error(t, err, cmd)
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

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--dry-run")
	assert.NoError(t, err, cmd)

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
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

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--version-local-store-index-paths", testPath+"/files-lsi.txt", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--dry-run")
	assert.NoError(t, err, cmd)

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
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

	cmd, err := executeCommandLine("prune-store-index", "--source-paths", testPath+"/files.txt", "--version-local-store-index-paths", testPath+"/files-lsi.txt", "--store-index-path", fsBlobPathPrefix+"/storage/store.lsi", "--write-version-local-store-index", "--dry-run")
	assert.NoError(t, err, cmd)

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v1.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)

	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v2.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsBlobPathPrefix+"/index/v3.lvi", "--target-path", testPath+"/version/current", "--storage-uri", fsBlobPathPrefix+"/storage")
	assert.NoError(t, err, cmd)
}
