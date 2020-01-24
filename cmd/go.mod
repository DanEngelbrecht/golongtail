module github.com/DanEngelbrecht/golongtail/longtail

go 1.13

require (
	github.com/DanEngelbrecht/golongtail/lib v0.0.0-20200124145854-4d9f8e82d4fe
	github.com/DanEngelbrecht/golongtail/store v0.0.0-00010101000000-000000000000
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/pkg/errors v0.9.1
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
)

replace github.com/DanEngelbrecht/golongtail/lib => ../lib

replace github.com/DanEngelbrecht/golongtail/store => ../store
