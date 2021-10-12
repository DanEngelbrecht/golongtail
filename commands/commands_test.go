package commands

import (
	"context"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
)

func createContent(store longtailstorelib.BlobStore, path string, content map[string]string) {
	client, _ := store.NewClient(context.Background())
	defer client.Close()
	for f, d := range content {
		o, _ := client.NewObject(path + f)
		b := []byte(d)
		o.Write(b)
	}
}

func validateContent(baseURI string, path string, content map[string]string) bool {
	store, _ := longtailstorelib.CreateBlobStoreForURI(baseURI)
	client, _ := store.NewClient(context.Background())
	defer client.Close()
	items, _ := client.GetObjects(path)
	foundItems := map[string]string{}
	for _, f := range items {
		n := f.Name[len(path)+1:]
		if c, exists := content[n]; exists {
			o, _ := client.NewObject(f.Name)
			b, _ := o.Read()
			d := string(b)
			if d != c {
				return false
			}
			foundItems[n] = string(b)
		}
	}
	return len(foundItems) == len(content)
}

var (
	v1FilesCreate = map[string]string{
		"empty-file":                               "",
		"abitoftext.txt":                           "this is a test file",
		"folder/abitoftextinasubfolder.txt":        "this is a test file in a subfolder",
		"folder/anotherabitoftextinasubfolder.txt": "this is a second test file in a subfolder",
	}
	v2FilesCreate = map[string]string{
		"empty-file":                                 "",
		"abitoftext.txt":                             "this is a test file",
		"folder/abitoftextinasubfolder.txt":          "this is a test file in a subfolder",
		"folder/anotherabitoftextinasubfolder.txt":   "this is a second test file in a subfolder",
		"stuff.txt":                                  "we have some stuff",
		"folder2/anotherabitoftextinasubfolder2.txt": "and some more text that we need",
	}
	v3FilesCreate = map[string]string{
		"empty-file":                                 "",
		"abitoftext.txt":                             "this is a test file",
		"folder/abitoftextmvinasubfolder.txt":        "this is a test file in a subfolder",
		"folder/anotherabitoftextinasubfolder.txt":   "this is a second test file in a subfolder",
		"stuff.txt":                                  "we have some stuff",
		"morestuff.txt":                              "we have some more stuff",
		"folder2/anotherabitoftextinasubfolder2.txt": "and some more text that we need",
	}
)

func createVersionData(t *testing.T, baseURI string) {
	store, _ := longtailstorelib.CreateBlobStoreForURI(baseURI)
	createContent(store, "version/v1/", v1FilesCreate)
	createContent(store, "version/v2/", v2FilesCreate)
	createContent(store, "version/v3/", v3FilesCreate)
	/*
		if !validateContent(store, "version/v1", v1FilesCreate) {
			t.Errorf("validateContent() content does not match %q", v1FilesCreate)
		}
		if !validateContent(store, "version/v2", v2FilesCreate) {
			t.Errorf("validateContent() content does not match %q", v2FilesCreate)
		}
		if !validateContent(store, "version/v3", v3FilesCreate) {
			t.Errorf("validateContent() content does not match %q", v3FilesCreate)
		}
	*/
}
