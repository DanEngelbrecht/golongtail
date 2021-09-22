module github.com/DanEngelbrecht/golongtail/cmd/longtail

go 1.13

require (
	cloud.google.com/go v0.95.0 // indirect
	cloud.google.com/go/storage v1.16.1 // indirect
	github.com/BurntSushi/toml v0.4.1 // indirect
	github.com/DanEngelbrecht/golongtail/longtaillib v0.0.0-00010101000000-000000000000
	github.com/DanEngelbrecht/golongtail/longtailstorelib v0.0.0-00010101000000-000000000000
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20210912230133-d1bdfacee922 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/googleapis/gax-go/v2 v2.1.1 // indirect
	github.com/pkg/errors v0.9.1
	golang.org/x/mod v0.5.0 // indirect
	golang.org/x/net v0.0.0-20210917221730-978cfadd31cf // indirect
	golang.org/x/sys v0.0.0-20210921065528-437939a70204 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.1.6 // indirect
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	honnef.co/go/tools v0.2.1 // indirect
)

replace github.com/DanEngelbrecht/golongtail/longtailstorelib => ../../longtailstorelib

replace github.com/DanEngelbrecht/golongtail/longtaillib => ../../longtaillib
