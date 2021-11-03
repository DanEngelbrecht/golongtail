package commands

import "fmt"

var (
	BuildVersion string = "<dev>"
)

type VersionCmd struct {
}

func (r *VersionCmd) Run(ctx *Context) error {
	fmt.Println(BuildVersion)
	return nil
}
