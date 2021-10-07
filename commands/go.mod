module github.com/DanEngelbrecht/golongtail/commands

go 1.15

require (
	github.com/DanEngelbrecht/golongtail/longtaillib v0.0.0-00010101000000-000000000000
	github.com/DanEngelbrecht/golongtail/longtailstorelib v0.0.0-00010101000000-000000000000
	github.com/DanEngelbrecht/golongtail/longtailutils v0.0.0-00010101000000-000000000000
	github.com/DanEngelbrecht/golongtail/remotestore v0.0.0-00010101000000-000000000000
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/viper v1.9.0
)

replace github.com/DanEngelbrecht/golongtail/longtaillib => ../longtaillib

replace github.com/DanEngelbrecht/golongtail/longtailstorelib => ../longtailstorelib

replace github.com/DanEngelbrecht/golongtail/longtailutils => ../longtailutils

replace github.com/DanEngelbrecht/golongtail/remotestore => ../remotestore
