package commands

import (
	"os"
	"testing"

	"github.com/alecthomas/kong"
)

func getVersion(t *testing.T, getConfigPath string, targetPath string, optionalCachePath string) {
	parser, err := kong.New(&Cli)
	if err != nil {
		t.Errorf("kong.New(Cli) failed with %s", err)
	}
	args := []string{
		"get",
		"--get-config-path", getConfigPath,
		"--target-path", targetPath,
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

func TestGet(t *testing.T) {
	os.RemoveAll("./test/")
	createVersionData(t, "fsblob://test")
	upsyncVersion(t, "test/version/v1", "fsblob://test/index/v1.lvi", "fsblob://test/storage", "", "fsblob://test/index/v1.json")
	upsyncVersion(t, "test/version/v2", "fsblob://test/index/v2.lvi", "fsblob://test/storage", "", "fsblob://test/index/v2.json")
	upsyncVersion(t, "test/version/v3", "fsblob://test/index/v3.lvi", "fsblob://test/storage", "", "fsblob://test/index/v3.json")

	getVersion(t, "fsblob://test/index/v1.json", "test/version/current", "")
	if !validateContent("fsblob://test", "version/current", v1FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v1FilesCreate)
	}
	getVersion(t, "fsblob://test/index/v2.json", "test/version/current", "")
	if !validateContent("fsblob://test", "version/current", v2FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v2FilesCreate)
	}
	getVersion(t, "fsblob://test/index/v3.json", "test/version/current", "")
	if !validateContent("fsblob://test", "version/current", v3FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v3FilesCreate)
	}
}

func TestGetWithVersionLSI(t *testing.T) {
	os.RemoveAll("./test/")
	createVersionData(t, "fsblob://test")
	upsyncVersion(t, "test/version/v1", "fsblob://test/index/v1.lvi", "fsblob://test/storage", "fsblob://test/index/v1.lsi", "fsblob://test/index/v1.json")
	upsyncVersion(t, "test/version/v2", "fsblob://test/index/v2.lvi", "fsblob://test/storage", "fsblob://test/index/v2.lsi", "fsblob://test/index/v2.json")
	upsyncVersion(t, "test/version/v3", "fsblob://test/index/v3.lvi", "fsblob://test/storage", "fsblob://test/index/v3.lsi", "fsblob://test/index/v3.json")

	getVersion(t, "fsblob://test/index/v1.json", "test/version/current", "")
	if !validateContent("fsblob://test", "version/current", v1FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v1FilesCreate)
	}
	getVersion(t, "fsblob://test/index/v2.json", "test/version/current", "")
	if !validateContent("fsblob://test", "version/current", v2FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v2FilesCreate)
	}
	getVersion(t, "fsblob://test/index/v3.json", "test/version/current", "")
	if !validateContent("fsblob://test", "version/current", v3FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v3FilesCreate)
	}
}

func TestGetWithCache(t *testing.T) {
	os.RemoveAll("./test/")
	createVersionData(t, "fsblob://test")
	upsyncVersion(t, "test/version/v1", "fsblob://test/index/v1.lvi", "fsblob://test/storage", "", "fsblob://test/index/v1.json")
	upsyncVersion(t, "test/version/v2", "fsblob://test/index/v2.lvi", "fsblob://test/storage", "", "fsblob://test/index/v2.json")
	upsyncVersion(t, "test/version/v3", "fsblob://test/index/v3.lvi", "fsblob://test/storage", "", "fsblob://test/index/v3.json")

	getVersion(t, "fsblob://test/index/v1.json", "test/version/current", "test/cache")
	if !validateContent("fsblob://test", "version/current", v1FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v1FilesCreate)
	}
	getVersion(t, "fsblob://test/index/v2.json", "test/version/current", "test/cache")
	if !validateContent("fsblob://test", "version/current", v2FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v2FilesCreate)
	}
	getVersion(t, "fsblob://test/index/v3.json", "test/version/current", "test/cache")
	if !validateContent("fsblob://test", "version/current", v3FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v3FilesCreate)
	}
}

func TestGetWithLSIAndCache(t *testing.T) {
	os.RemoveAll("./test/")
	createVersionData(t, "fsblob://test")
	upsyncVersion(t, "test/version/v1", "fsblob://test/index/v1.lvi", "fsblob://test/storage", "fsblob://test/index/v1.lsi", "fsblob://test/index/v1.json")
	upsyncVersion(t, "test/version/v2", "fsblob://test/index/v2.lvi", "fsblob://test/storage", "fsblob://test/index/v2.lsi", "fsblob://test/index/v2.json")
	upsyncVersion(t, "test/version/v3", "fsblob://test/index/v3.lvi", "fsblob://test/storage", "fsblob://test/index/v3.lsi", "fsblob://test/index/v3.json")

	getVersion(t, "fsblob://test/index/v1.json", "test/version/current", "test/cache")
	if !validateContent("fsblob://test", "version/current", v1FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v1FilesCreate)
	}
	getVersion(t, "fsblob://test/index/v2.json", "test/version/current", "test/cache")
	if !validateContent("fsblob://test", "version/current", v2FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v2FilesCreate)
	}
	getVersion(t, "fsblob://test/index/v3.json", "test/version/current", "test/cache")
	if !validateContent("fsblob://test", "version/current", v3FilesCreate) {
		t.Errorf("validateContent() content does not match %q", v3FilesCreate)
	}
}
