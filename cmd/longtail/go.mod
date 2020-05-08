module github.com/DanEngelbrecht/golongtail/cmd/longtail

go 1.13

require (
	github.com/DanEngelbrecht/golongtail/longtaillib v0.0.0-00010101000000-000000000000
	github.com/DanEngelbrecht/golongtail/longtailstorelib v0.0.0-00010101000000-000000000000
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/stretchr/testify v1.5.1 // indirect
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
)

replace github.com/DanEngelbrecht/golongtail/longtailstorelib => ../../longtailstorelib

replace github.com/DanEngelbrecht/golongtail/longtaillib => ../../longtaillib
