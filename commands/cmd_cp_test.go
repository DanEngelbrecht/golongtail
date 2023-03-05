package commands

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/stretchr/testify/assert"
)

func validateFileContentAndDelete(t *testing.T, baseURI string, sourcePath string, expectedContent string) {
	store, _ := longtailstorelib.CreateBlobStoreForURI(baseURI)
	client, _ := store.NewClient(context.Background())
	defer client.Close()
	o, _ := client.NewObject(sourcePath)
	d, _ := o.Read()
	s := string(d)
	if s != expectedContent {
		t.Errorf("`%s` content `%s` does not match `%s`", sourcePath, s, expectedContent)
	}
	o.Delete()
}

func TestCp(t *testing.T) {
	testPath, _ := ioutil.TempDir("", "test")
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("upsync", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("upsync", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.lvi", "--storage-uri", fsBlobPathPrefix+"/storage")

	cmd, err := executeCommandLine("cp", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-index-path", fsBlobPathPrefix+"/index/v1.lvi", "folder/abitoftextinasubfolder.txt", fsBlobPathPrefix+"/current/abitoftextinasubfolder.txt")
	assert.Equal(t, err, nil, cmd)
	validateFileContentAndDelete(t, fsBlobPathPrefix, "current/abitoftextinasubfolder.txt", v1FilesCreate["folder/abitoftextinasubfolder.txt"])

	cmd, err = executeCommandLine("cp", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-index-path", fsBlobPathPrefix+"/index/v2.lvi", "stuff.txt", fsBlobPathPrefix+"/current/stuff.txt")
	assert.Equal(t, err, nil, cmd)
	validateFileContentAndDelete(t, fsBlobPathPrefix, "current/stuff.txt", v2FilesCreate["stuff.txt"])

	cmd, err = executeCommandLine("cp", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-index-path", fsBlobPathPrefix+"/index/v3.lvi", "morestuff.txt", fsBlobPathPrefix+"/current/morestuff.txt")
	assert.Equal(t, err, nil, cmd)
	validateFileContentAndDelete(t, fsBlobPathPrefix, "current/morestuff.txt", v3FilesCreate["morestuff.txt"])

	cmd, err = executeCommandLine("cp", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-index-path", fsBlobPathPrefix+"/index/v1.lvi", "folder/abitoftextinasubfolder.txt", fsBlobPathPrefix+"/current/abitoftextinasubfolder.txt", "--cache-path", testPath+"/cache")
	assert.Equal(t, err, nil, cmd)
	validateFileContentAndDelete(t, fsBlobPathPrefix, "current/abitoftextinasubfolder.txt", v1FilesCreate["folder/abitoftextinasubfolder.txt"])

	cmd, err = executeCommandLine("cp", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-index-path", fsBlobPathPrefix+"/index/v2.lvi", "stuff.txt", fsBlobPathPrefix+"/current/stuff.txt", "--cache-path", testPath+"/cache")
	assert.Equal(t, err, nil, cmd)
	validateFileContentAndDelete(t, fsBlobPathPrefix, "current/stuff.txt", v2FilesCreate["stuff.txt"])

	cmd, err = executeCommandLine("cp", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-index-path", fsBlobPathPrefix+"/index/v3.lvi", "morestuff.txt", fsBlobPathPrefix+"/current/morestuff.txt", "--cache-path", testPath+"/cache")
	assert.Equal(t, err, nil, cmd)
	validateFileContentAndDelete(t, fsBlobPathPrefix, "current/morestuff.txt", v3FilesCreate["morestuff.txt"])
}
