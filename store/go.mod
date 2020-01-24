module github.com/DanEngelbrecht/golongtail/store

go 1.13

require (
	cloud.google.com/go/storage v1.5.0
	github.com/DanEngelbrecht/golongtail/lib v0.0.0-20200124145854-4d9f8e82d4fe
	github.com/pkg/errors v0.9.1
)

replace github.com/DanEngelbrecht/golongtail/lib => ../lib
