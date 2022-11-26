package commands

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/DanEngelbrecht/golongtail/longtailstorelib"
	"github.com/alecthomas/kong"
)

func executeCommandLine(command string, params ...string) (string, error) {
	args := []string{command}
	args = append(args, params...)
	//	args := strings.Split(commandLine, " ")
	parser, err := kong.New(&Cli)
	if err != nil {
		//		t.Errorf("kong.New(Cli) failed with %s", err)
		return strings.Join(args, " "), err
	}

	ctx, err := parser.Parse(args)
	if err != nil {
		return strings.Join(args, " "), err
		//		t.Errorf("parser.Parse() failed with %s", err)
	}

	context := &Context{
		NumWorkerCount: runtime.NumCPU(),
	}
	err = ctx.Run(context)
	if err != nil {
		return strings.Join(args, " "), err
		//		t.Errorf("ctx.Run(context) failed with %s", err)
	}
	return strings.Join(args, " "), nil
}

func createContent(store longtailstorelib.BlobStore, path string, content map[string]string) {
	client, _ := store.NewClient(context.Background())
	defer client.Close()
	for f, d := range content {
		o, _ := client.NewObject(path + f)
		b := []byte(d)
		o.Write(b)
	}
}

func validateContent(t *testing.T, baseURI string, path string, content map[string]string) {
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
				t.Errorf("Content of file `%s` does not match. Expected `%s`, got `%s`", n, d, c)
				return
			}
			foundItems[n] = string(b)
		} else {
			if n != ".longtail.index.cache.lvi" {
				t.Errorf("Unexpected file `%s`", n)
			}
		}
	}
	if len(foundItems) != len(content) {
		t.Errorf("Expected `%d` files but found `%d`.", len(content), len(foundItems))
	}
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
	layerData = map[string]string{
		"empty-file":                                    "",
		"abitoftext.txt":                                "this is a test file",
		"abitoftext.layer2":                             "second layer test file",
		"folder/abitoftextmvinasubfolder.txt":           "this is a test file in a subfolder",
		"folder/abitoftextmvinasubfolder.layer2":        "layer 2 data in folder",
		"folder/anotherabitoftextinasubfolder.txt":      "this is a second test file in a subfolder",
		"stuff.txt":                                     "we have some stuff",
		"blobby/fluff.layer2":                           "more fluff is always essential",
		"glob.layer2":                                   "glob is all you need",
		"morestuff.txt":                                 "we have some more stuff",
		"folder2/anotherabitoftextinasubfolder2.txt":    "and some more text that we need",
		"folder2/anotherabitoftextinasubfolder2.layer3": "stuff for layer 3 is good stuff for any layer",
		"folder3/wefewef.layer3":                        "layer3 on top of the world",
	}
)

func createVersionData(t *testing.T, baseURI string) {
	store, _ := longtailstorelib.CreateBlobStoreForURI(baseURI)
	createContent(store, "version/v1/", v1FilesCreate)
	createContent(store, "version/v2/", v2FilesCreate)
	createContent(store, "version/v3/", v3FilesCreate)
}

func createLayeredData(t *testing.T, baseURI string) {
	store, _ := longtailstorelib.CreateBlobStoreForURI(baseURI)
	createContent(store, "source/", layerData)
}
