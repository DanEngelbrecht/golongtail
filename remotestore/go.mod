module github.com/DanEngelbrecht/golongtail/remotestore

go 1.17

require (
	github.com/DanEngelbrecht/golongtail/longtaillib v0.0.0-00010101000000-000000000000
	github.com/DanEngelbrecht/golongtail/longtailstorelib v0.0.0-00010101000000-000000000000
	github.com/DanEngelbrecht/golongtail/longtailutils v0.0.0-00010101000000-000000000000
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
)

replace github.com/DanEngelbrecht/golongtail/longtaillib => ../longtaillib

replace github.com/DanEngelbrecht/golongtail/longtailstorelib => ../longtailstorelib

replace github.com/DanEngelbrecht/golongtail/longtailutils => ../longtailutils
