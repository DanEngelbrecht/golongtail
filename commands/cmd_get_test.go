package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	testPath := t.TempDir()
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)

	executeCommandLine("put", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.json", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("put", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.json", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("put", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.json", "--storage-uri", fsBlobPathPrefix+"/storage")

	cmd, err := executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)

	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)

	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}

func TestGetWithVersionLSI(t *testing.T) {
	testPath := t.TempDir()
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)

	executeCommandLine("put", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v1.lsi")
	executeCommandLine("put", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v2.lsi")
	executeCommandLine("put", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v3.lsi")

	cmd, err := executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)

	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)

	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}

func TestGetWithCache(t *testing.T) {
	testPath := t.TempDir()
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("put", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.json", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("put", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.json", "--storage-uri", fsBlobPathPrefix+"/storage")
	executeCommandLine("put", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.json", "--storage-uri", fsBlobPathPrefix+"/storage")

	cmd, err := executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)

	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)

	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}

func TestGetWithLSIAndCache(t *testing.T) {
	testPath := t.TempDir()
	fsBlobPathPrefix := "fsblob://" + testPath
	createVersionData(t, fsBlobPathPrefix)
	executeCommandLine("put", "--source-path", testPath+"/version/v1", "--target-path", fsBlobPathPrefix+"/index/v1.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v1.lsi")
	executeCommandLine("put", "--source-path", testPath+"/version/v2", "--target-path", fsBlobPathPrefix+"/index/v2.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v2.lsi")
	executeCommandLine("put", "--source-path", testPath+"/version/v3", "--target-path", fsBlobPathPrefix+"/index/v3.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/v3.lsi")

	cmd, err := executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v1.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v1FilesCreate)

	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v2.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v2FilesCreate)

	cmd, err = executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/v3.json", "--target-path", testPath+"/version/current", "--cache-path", testPath+"/cache")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "version/current", v3FilesCreate)
}

func TestMultiVersionGet(t *testing.T) {
	testPath := t.TempDir()
	fsBlobPathPrefix := "fsblob://" + testPath
	createLayeredData(t, fsBlobPathPrefix)
	executeCommandLine("put", "--exclude-filter-regex", ".*layer2$**.*layer3$", "--source-path", testPath+"/source", "--target-path", fsBlobPathPrefix+"/index/base.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/base.lsi")
	executeCommandLine("put", "--include-filter-regex", ".*/$**.*\\.layer2$", "--source-path", testPath+"/source", "--target-path", fsBlobPathPrefix+"/index/layer2.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/layer2.lsi")
	executeCommandLine("put", "--include-filter-regex", ".*/$**.*\\.layer3$", "--source-path", testPath+"/source", "--target-path", fsBlobPathPrefix+"/index/layer3.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/layer3.lsi")

	cmd, err := executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/base.json "+fsBlobPathPrefix+"/index/layer2.json "+fsBlobPathPrefix+"/index/layer3.json", "--target-path", testPath+"/target")
	assert.Equal(t, nil, err, cmd)
	validateContent(t, fsBlobPathPrefix, "target", layerData)
}

func TestMultiVersionGetMismatchStoreURI(t *testing.T) {
	testPath := t.TempDir()
	fsBlobPathPrefix := "fsblob://" + testPath
	createLayeredData(t, fsBlobPathPrefix)
	executeCommandLine("put", "--exclude-filter-regex", ".*layer2$**.*layer3$", "--source-path", testPath+"/source", "--target-path", fsBlobPathPrefix+"/index/base.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/base.lsi")
	executeCommandLine("put", "--include-filter-regex", ".*/$**.*\\.layer2$", "--source-path", testPath+"/source", "--target-path", fsBlobPathPrefix+"/index/layer2.json", "--storage-uri", fsBlobPathPrefix+"/storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/layer2.lsi")
	executeCommandLine("put", "--include-filter-regex", ".*/$**.*\\.layer3$", "--source-path", testPath+"/source", "--target-path", fsBlobPathPrefix+"/index/layer3.json", "--storage-uri", fsBlobPathPrefix+"/bad_storage", "--version-local-store-index-path", fsBlobPathPrefix+"/index/layer3.lsi")

	cmd, err := executeCommandLine("get", "--source-path", fsBlobPathPrefix+"/index/base.json "+fsBlobPathPrefix+"/index/layer2.json "+fsBlobPathPrefix+"/index/layer3.json", "--target-path", testPath+"/target")
	assert.NotEqual(t, nil, err, cmd)
}
