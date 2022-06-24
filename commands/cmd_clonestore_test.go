package commands

import (
	"archive/zip"
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/pkg/errors"
)

func createZipForFolder(searchPath string) (bytes.Buffer, error) {
	const fname = "createZipForFolder"
	var zipBuffer bytes.Buffer
	f := bufio.NewWriter(&zipBuffer)
	zw := zip.NewWriter(f)
	err := filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		subPath := path[len(searchPath)+1:]
		fw, err := zw.Create(longtailstorelib.NormalizeFileSystemPath(subPath))

		if err != nil {
			return err
		}
		rw, err := os.Open(path)
		if err != nil {
			return err
		}
		io.Copy(fw, rw)
		return nil
	})
	zw.Close()
	if err != nil {
		return bytes.Buffer{}, errors.Wrap(err, fname)
	}
	return zipBuffer, nil
}

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

func TestCloneStoreZipFallback(t *testing.T) {
	sourcePath, _ := ioutil.TempDir("", "source")
	fsSourceBlobPathPrefix := "fsblob://" + sourcePath
	createVersionData(t, fsSourceBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", sourcePath+"/version/v1", "--target-path", fsSourceBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsSourceBlobPathPrefix+"/storage")
	v1ZipBuffer, _ := createZipForFolder(sourcePath + "/version/v1")
	os.WriteFile(sourcePath+"/index/v1.zip", v1ZipBuffer.Bytes(), 0644)
	executeCommandLine("upsync", "--source-path", sourcePath+"/version/v2", "--target-path", fsSourceBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsSourceBlobPathPrefix+"/storage-wrong-store")
	v2ZipBuffer, _ := createZipForFolder(sourcePath + "/version/v2")
	os.WriteFile(sourcePath+"/index/v2.zip", v2ZipBuffer.Bytes(), 0644)
	executeCommandLine("upsync", "--source-path", sourcePath+"/version/v3", "--target-path", fsSourceBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsSourceBlobPathPrefix+"/storage-wrong-store")
	v3ZipBuffer, _ := createZipForFolder(sourcePath + "/version/v3")
	os.WriteFile(sourcePath+"/index/v3.zip", v3ZipBuffer.Bytes(), 0644)

	targetPath, _ := ioutil.TempDir("", "target")
	fsTargetBlobPathPrefix := "fsblob://" + targetPath

	sourceFilesContent := []byte(
		fsSourceBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsSourceBlobPathPrefix + "/index/v2.lvi" + "\n" +
			fsSourceBlobPathPrefix + "/index/v3.lvi" + "\n")
	longtailutils.WriteToURI(fsSourceBlobPathPrefix+"/source-files.txt", sourceFilesContent)

	zipSourceFilesContent := []byte(
		fsSourceBlobPathPrefix + "/index/v1.zip" + "\n" +
			fsSourceBlobPathPrefix + "/index/v2.zip" + "\n" +
			fsSourceBlobPathPrefix + "/index/v3.zip" + "\n")
	longtailutils.WriteToURI(fsSourceBlobPathPrefix+"/source-zip-files.txt", zipSourceFilesContent)

	targetFilesContent := []byte(
		fsTargetBlobPathPrefix + "/index/v1.lvi" + "\n" +
			fsTargetBlobPathPrefix + "/index/v2.lvi" + "\n" +
			fsTargetBlobPathPrefix + "/index/v3.lvi" + "\n")
	longtailutils.WriteToURI(fsTargetBlobPathPrefix+"/target-files.txt", targetFilesContent)

	cmd, err := executeCommandLine("clone-store",
		"--source-storage-uri", fsSourceBlobPathPrefix+"/storage",
		"--target-storage-uri", fsTargetBlobPathPrefix+"/storage",
		"--source-paths", sourcePath+"/source-files.txt",
		"--source-zip-paths", sourcePath+"/source-zip-files.txt",
		"--target-paths", targetPath+"/target-files.txt",
		"--target-path", sourcePath+"/version/current",
		"--create-version-local-store-index")
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
}
