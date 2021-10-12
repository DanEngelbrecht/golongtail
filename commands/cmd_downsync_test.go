package commands

import (
	"os"
	"testing"

	"github.com/alecthomas/kong"
)

func downsyncVersion(t *testing.T, sourcePath string, targetPath string, storageURI string, optionalVersionLocalStoreIndexPath string, optionalCachePath string) {
	parser, err := kong.New(&Cli)
	if err != nil {
		t.Errorf("kong.New(Cli) failed with %s", err)
	}
	args := []string{
		"downsync",
		"--source-path", sourcePath,
		"--target-path", targetPath,
		"--storage-uri", storageURI,
	}
	if optionalVersionLocalStoreIndexPath != "" {
		args = append(args, "--version-local-store-index-path")
		args = append(args, optionalVersionLocalStoreIndexPath)
	}
	if optionalCachePath != "" {
		args = append(args, "--cache-path")
		args = append(args, optionalCachePath)
	}
	ctx, err := parser.Parse(args)
	if err != nil {
		t.Errorf("parser.Parse() failed with %s", err)
	}

	context := &Context{
		NumWorkerCount: 4,
	}
	err = ctx.Run(context)
	if err != nil {
		t.Errorf("ctx.Run(context) failed with %s", err)
	}
}

func TestDownsync(t *testing.T) {
	os.RemoveAll("./test/")
	createVersionData(t, "fsblob://test")
	upsyncVersion(t, "test/version/v1", "fsblob://test/index/v1.lvi", "fsblob://test/storage", "")
	upsyncVersion(t, "test/version/v2", "fsblob://test/index/v2.lvi", "fsblob://test/storage", "")
	upsyncVersion(t, "test/version/v3", "fsblob://test/index/v3.lvi", "fsblob://test/storage", "")

	downsyncVersion(t, "fsblob://test/index/v1.lvi", "test/version/current", "fsblob://test/storage", "", "")
	if !validateContent("fsblob://test", "version/current", v1FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v1FilesCreate)
	}
	downsyncVersion(t, "fsblob://test/index/v2.lvi", "test/version/current", "fsblob://test/storage", "", "")
	if !validateContent("fsblob://test", "version/current", v2FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v2FilesCreate)
	}
	downsyncVersion(t, "fsblob://test/index/v3.lvi", "test/version/current", "fsblob://test/storage", "", "")
	if !validateContent("fsblob://test", "version/current", v3FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v3FilesCreate)
	}
}
