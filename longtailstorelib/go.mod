module github.com/DanEngelbrecht/golongtail/longtailstorelib

go 1.13

require (
	cloud.google.com/go/storage v1.7.0
	github.com/DanEngelbrecht/golongtail/longtaillib v0.0.0-00010101000000-000000000000
	github.com/pkg/errors v0.9.1
	golang.org/x/net v0.0.0-20200501053045-e0ff5e5a1de5
	google.golang.org/api v0.22.0
)

replace github.com/DanEngelbrecht/golongtail/longtaillib => ../longtaillib
