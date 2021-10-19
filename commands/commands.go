package commands

// Cli ...
var Cli struct {
	LogLevel                string                     `name:"log-level" help:"Log level [debug, info, warn, error]" enum:"debug, info, warn, error" default:"warn" `
	ShowStats               bool                       `name:"show-stats" help:"Output brief stats summary"`
	ShowStoreStats          bool                       `name:"show-store-stats" help:"Output detailed stats for block stores"`
	MemTrace                bool                       `name:"mem-trace" help:"Output summary memory statistics from longtail"`
	MemTraceDetailed        bool                       `name:"mem-trace-detailed" help:"Output detailed memory statistics from longtail"`
	MemTraceCSV             string                     `name:"mem-trace-csv" help:"Output path for detailed memory statistics from longtail in csv format"`
	WorkerCount             int                        `name:"worker-count" help:"Limit number of workers created, defaults to match number of logical CPUs (zero for default count)" default:"0"`
	Upsync                  UpsyncCmd                  `cmd:"" name:"upsync" help:"Upload a folder"`
	Downsync                DownsyncCmd                `cmd:"" name:"downsync" help:"Download a folder"`
	Get                     GetCmd                     `cmd:"" name:"get" help:"Download a folder using a get-config"`
	ValidateVersion         ValidateVersionCmd         `cmd:"" name:"validate-version" help:"Validate a version index against a content store making sure all content needed is in the store" aliases:"validate"`
	PrintVersion            PrintVersionCmd            `cmd:"" name:"print-version" help:"Print info about a version index" aliases:"printVersionIndex"`
	PrintStore              PrintStoreCmd              `cmd:"" name:"print-store" help:"Print info about a store index" aliases:"printStoreIndex"`
	PrintVersionUsage       PrintVersionUsageCmd       `cmd:"" name:"print-version-usage" help:"Shows block usage and asset fragmentaiton stats about a version index" aliases:"stats"`
	DumpVersionAssets       DumpVersionAssetsCmd       `cmd:"" name:"dump-version-assets" help:"Lists all the asset paths inside a version index" aliases:"dump"`
	Ls                      LsCmd                      `cmd:"" name:"ls" help:"List the content of a path inside a version index"`
	Cp                      CpCmd                      `cmd:"" name:"cp" help:"Copies a file from inside a version index"`
	InitRemoteStore         InitRemoteStoreCmd         `cmd:"" name:"init-remote-store" help:"Open/create a remote store and force rebuild the store index" aliases:"init"`
	CreateVersionStoreIndex CreateVersionStoreIndexCmd `cmd:"" name:"create-version-store-index" help:"Create a store index optimized for a version index" aliases:"createVersionStoreIndex"`
	CloneStore              CloneStoreCmd              `cmd:"" name:"clone-store" help:"Clone all the data needed to cover a set of versions from one store into a new store" aliases:"cloneStore"`
	PruneStore              PruneStoreCmd              `cmd:"" name:"prune-store" help:"Prune blocks in a store which are not used by the files in the input list. CAUTION! Running uploads to a store that is being pruned may cause loss of the uploaded data" aliases:"pruneStore"`
	PruneStoreIndex         PruneStoreIndexCmd         `cmd:"" name:"prune-store-index" help:"Prune blocks in a store index which are not used by the files in the input list. CAUTION! Running uploads to a store that is being pruned may cause loss of the uploaded data"`
	PruneStoreBlocks        PruneStoreBlocksCmd        `cmd:"" name:"prune-store-blocks" help:"Prune blocks in a store which are not present in the store index. CAUTION! Running uploads to a store that is being pruned may cause loss of the uploaded data"`
}
