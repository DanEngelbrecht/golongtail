module github.com/DanEngelbrecht/golongtail/longtail

go 1.13

require (
	cloud.google.com/go/storage v1.4.0
	github.com/DanEngelbrecht/golongtail/golongtail v0.0.0
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/pkg/errors v0.8.1
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	google.golang.org/api v0.14.0
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
)

replace github.com/DanEngelbrecht/golongtail/golongtail => ../golongtail
