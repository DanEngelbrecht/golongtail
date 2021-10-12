package commands

import (
	"os"
	"testing"

	"github.com/alecthomas/kong"
)

func upsyncVersion(t *testing.T, sourcePath string, targetPath string, storageURI string, optionalVersionLocalStoreIndexPath string, optionalGetConfigPath string) {
	parser, err := kong.New(&Cli)
	if err != nil {
		t.Errorf("kong.New(Cli) failed with %s", err)
	}
	args := []string{
		"upsync",
		"--source-path", sourcePath,
		"--target-path", targetPath,
		"--storage-uri", storageURI,
	}
	if optionalVersionLocalStoreIndexPath != "" {
		args = append(args, "--version-local-store-index-path")
		args = append(args, optionalVersionLocalStoreIndexPath)
	}
	if optionalGetConfigPath != "" {
		args = append(args, "--get-config-path")
		args = append(args, optionalGetConfigPath)
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

func TestUpsync(t *testing.T) {
	os.RemoveAll("./test/")
	createVersionData(t, "fsblob://test")
	upsyncVersion(t, "test/version/v1", "fsblob://test/index/v1.lvi", "fsblob://test/storage", "", "")
	upsyncVersion(t, "test/version/v2", "fsblob://test/index/v2.lvi", "fsblob://test/storage", "", "")
	upsyncVersion(t, "test/version/v3", "fsblob://test/index/v3.lvi", "fsblob://test/storage", "", "")
}

func TestUpsyncWithLSI(t *testing.T) {
	os.RemoveAll("./test/")
	createVersionData(t, "fsblob://test")
	upsyncVersion(t, "test/version/v1", "fsblob://test/index/v1.lvi", "fsblob://test/storage", "fsblob://test/index/v1.lsi", "")
	upsyncVersion(t, "test/version/v2", "fsblob://test/index/v2.lvi", "fsblob://test/storage", "fsblob://test/index/v2.lsi", "")
	upsyncVersion(t, "test/version/v3", "fsblob://test/index/v3.lvi", "fsblob://test/storage", "fsblob://test/index/v3.lsi", "")
}

func TestUpsyncWithGetConfig(t *testing.T) {
	os.RemoveAll("./test/")
	createVersionData(t, "fsblob://test")
	upsyncVersion(t, "test/version/v1", "fsblob://test/index/v1.lvi", "fsblob://test/storage", "", "fsblob://test/index/v1.json")
	upsyncVersion(t, "test/version/v2", "fsblob://test/index/v2.lvi", "fsblob://test/storage", "", "fsblob://test/index/v1.json")
	upsyncVersion(t, "test/version/v3", "fsblob://test/index/v3.lvi", "fsblob://test/storage", "", "fsblob://test/index/v1.json")
}

func TestUpsyncWithGetConfigAndLSI(t *testing.T) {
	os.RemoveAll("./test/")
	createVersionData(t, "fsblob://test")
	upsyncVersion(t, "test/version/v1", "fsblob://test/index/v1.lvi", "fsblob://test/storage", "fsblob://test/index/v1.lsi", "fsblob://test/index/v1.json")
	upsyncVersion(t, "test/version/v2", "fsblob://test/index/v2.lvi", "fsblob://test/storage", "fsblob://test/index/v2.lsi", "fsblob://test/index/v1.json")
	upsyncVersion(t, "test/version/v3", "fsblob://test/index/v3.lvi", "fsblob://test/storage", "fsblob://test/index/v3.lsi", "fsblob://test/index/v1.json")
}
