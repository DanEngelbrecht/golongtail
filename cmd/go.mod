module github.com/DanEngelbrecht/golongtail/cmd

go 1.13

require (
	cloud.google.com/go/storage v1.4.0
	github.com/DanEngelbrecht/golongtail/longtail v0.0.0
	github.com/pkg/errors v0.8.1
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	google.golang.org/api v0.14.0
)

replace github.com/DanEngelbrecht/golongtail/longtail => ../longtail
