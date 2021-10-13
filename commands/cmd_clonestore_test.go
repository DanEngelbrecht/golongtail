package commands

import (
	"io/ioutil"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtailutils"
)

func TestCloneStore(t *testing.T) {
	sourcePath, _ := ioutil.TempDir("", "source")
	fsSourceBlobPathPrefix := "fsblob://" + sourcePath
	createVersionData(t, fsSourceBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", sourcePath+"/version/v1", "--target-path", fsSourceBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsSourceBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", sourcePath+"/version/v2", "--target-path", fsSourceBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsSourceBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", sourcePath+"/version/v3", "--target-path", fsSourceBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsSourceBlobPathPrefix+"/storage")
	targetPath, _ := ioutil.TempDir("", "target")
	fsTargetBlobPathPrefix := "fsblob://" + targetPath

	sourceFilesContent := []byte(
		fsSourceBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsSourceBlobPathPrefix + "/index/v2.lvi" + "\n" +
			fsSourceBlobPathPrefix + "/index/v3.lvi" + "\n")
	longtailutils.WriteToURI(fsSourceBlobPathPrefix+"/source-files.txt", sourceFilesContent)

	targetFilesContent := []byte(
		fsTargetBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsTargetBlobPathPrefix + "/index/v2.lvi" + "\n" +
			fsTargetBlobPathPrefix + "/index/v3.lvi" + "\n")
	longtailutils.WriteToURI(fsTargetBlobPathPrefix+"/target-files.txt", targetFilesContent)

	cmd, err := executeCommandLine("clone-store",
		"--source-storage-uri", fsSourceBlobPathPrefix+"/storage",
		"--target-storage-uri", fsTargetBlobPathPrefix+"/storage",
		"--source-paths", sourcePath+"/source-files.txt",
		"--target-paths", targetPath+"/target-files.txt",
		"--target-path", sourcePath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	cmd, err = executeCommandLine("downsync", "--source-path", fsTargetBlobPathPrefix+"/index/v1.lvi", "--target-path", targetPath+"/version/current", "--storage-uri", fsTargetBlobPathPrefix+"/storage", "--cache-path", targetPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsTargetBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsTargetBlobPathPrefix+"/index/v2.lvi", "--target-path", targetPath+"/version/current", "--storage-uri", fsTargetBlobPathPrefix+"/storage", "--cache-path", targetPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsTargetBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsTargetBlobPathPrefix+"/index/v3.lvi", "--target-path", targetPath+"/version/current", "--storage-uri", fsTargetBlobPathPrefix+"/storage", "--cache-path", targetPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsTargetBlobPathPrefix, "version/current", v3FilesCreate)

	// Run again, now it will skip all existing
	cmd, err = executeCommandLine("clone-store",
		"--source-storage-uri", fsSourceBlobPathPrefix+"/storage",
		"--target-storage-uri", fsTargetBlobPathPrefix+"/storage",
		"--source-paths", sourcePath+"/source-files.txt",
		"--target-paths", targetPath+"/target-files.txt",
		"--target-path", sourcePath+"/version/current",
		"--skip-validate")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	// Run again, now it will validate all existing
	cmd, err = executeCommandLine("clone-store",
		"--source-storage-uri", fsSourceBlobPathPrefix+"/storage",
		"--target-storage-uri", fsTargetBlobPathPrefix+"/storage",
		"--source-paths", sourcePath+"/source-files.txt",
		"--target-paths", targetPath+"/target-files.txt",
		"--target-path", sourcePath+"/version/current")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
}

func TestCloneStoreCreateVersionLocalStoreIndex(t *testing.T) {
	sourcePath, _ := ioutil.TempDir("", "source")
	fsSourceBlobPathPrefix := "fsblob://" + sourcePath
	createVersionData(t, fsSourceBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", sourcePath+"/version/v1", "--target-path", fsSourceBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsSourceBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", sourcePath+"/version/v2", "--target-path", fsSourceBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsSourceBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", sourcePath+"/version/v3", "--target-path", fsSourceBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsSourceBlobPathPrefix+"/storage")
	targetPath, _ := ioutil.TempDir("", "target")
	fsTargetBlobPathPrefix := "fsblob://" + targetPath

	sourceFilesContent := []byte(
		fsSourceBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsSourceBlobPathPrefix + "/index/v2.lvi" + "\n" +
			fsSourceBlobPathPrefix + "/index/v3.lvi" + "\n")
	longtailutils.WriteToURI(fsSourceBlobPathPrefix+"/source-files.txt", sourceFilesContent)

	targetFilesContent := []byte(
		fsTargetBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsTargetBlobPathPrefix + "/index/v2.lvi" + "\n" +
			fsTargetBlobPathPrefix + "/index/v3.lvi" + "\n")
	longtailutils.WriteToURI(fsTargetBlobPathPrefix+"/target-files.txt", targetFilesContent)

	cmd, err := executeCommandLine("clone-store",
		"--source-storage-uri", fsSourceBlobPathPrefix+"/storage",
		"--target-storage-uri", fsTargetBlobPathPrefix+"/storage",
		"--source-paths", sourcePath+"/source-files.txt",
		"--target-paths", targetPath+"/target-files.txt",
		"--target-path", sourcePath+"/version/current",
		"--create-version-local-store-index")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}

	cmd, err = executeCommandLine("downsync", "--source-path", fsTargetBlobPathPrefix+"/index/v1.lvi", "--target-path", targetPath+"/version/current", "--storage-uri", fsTargetBlobPathPrefix+"/storage", "--version-local-store-index-path", fsTargetBlobPathPrefix+"/index/v1.lsi", "--cache-path", targetPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsTargetBlobPathPrefix, "version/current", v1FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsTargetBlobPathPrefix+"/index/v2.lvi", "--target-path", targetPath+"/version/current", "--storage-uri", fsTargetBlobPathPrefix+"/storage", "--version-local-store-index-path", fsTargetBlobPathPrefix+"/index/v2.lsi", "--cache-path", targetPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsTargetBlobPathPrefix, "version/current", v2FilesCreate)
	cmd, err = executeCommandLine("downsync", "--source-path", fsTargetBlobPathPrefix+"/index/v3.lvi", "--target-path", targetPath+"/version/current", "--storage-uri", fsTargetBlobPathPrefix+"/storage", "--version-local-store-index-path", fsTargetBlobPathPrefix+"/index/v3.lsi", "--cache-path", targetPath+"/cache")
	if err != nil {
		t.Errorf("%s: %s", cmd, err)
	}
	validateContent(t, fsTargetBlobPathPrefix, "version/current", v3FilesCreate)
}
