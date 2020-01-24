module github.com/DanEngelbrecht/golongtail/store

go 1.13

require (
	cloud.google.com/go/storage v1.5.0
	github.com/DanEngelbrecht/golongtail/lib v0.0.0-00010101000000-000000000000
	github.com/pkg/errors v0.9.1
)

replace github.com/DanEngelbrecht/golongtail/lib => ../lib
