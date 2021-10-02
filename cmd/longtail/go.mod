module github.com/DanEngelbrecht/golongtail/cmd/longtail

go 1.13

require (
	github.com/DanEngelbrecht/golongtail/longtaillib v0.0.0-00010101000000-000000000000
	github.com/DanEngelbrecht/golongtail/longtailstorelib v0.0.0-00010101000000-000000000000
	github.com/alecthomas/kong v0.2.17
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20210912230133-d1bdfacee922 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.8.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.16.0 // indirect
	github.com/pkg/errors v0.9.1
	github.com/spf13/viper v1.9.0
)

replace github.com/DanEngelbrecht/golongtail/longtailstorelib => ../../longtailstorelib

replace github.com/DanEngelbrecht/golongtail/longtaillib => ../../longtaillib
