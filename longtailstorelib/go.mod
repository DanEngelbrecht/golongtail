module github.com/DanEngelbrecht/golongtail/longtailstorelib

go 1.13

require (
	cloud.google.com/go v0.95.0 // indirect
	cloud.google.com/go/storage v1.16.1
	github.com/BurntSushi/toml v0.4.1 // indirect
	github.com/DanEngelbrecht/golongtail/longtaillib v0.0.0-00010101000000-000000000000
	github.com/aws/aws-sdk-go-v2 v1.9.1
	github.com/aws/aws-sdk-go-v2/config v1.8.2
	github.com/aws/aws-sdk-go-v2/service/s3 v1.16.0
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/googleapis/gax-go/v2 v2.1.1 // indirect
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	golang.org/x/mod v0.5.0 // indirect
	golang.org/x/net v0.0.0-20210917221730-978cfadd31cf
	golang.org/x/sys v0.0.0-20210921065528-437939a70204 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.1.6 // indirect
	google.golang.org/api v0.57.0
	honnef.co/go/tools v0.2.1 // indirect
)

replace github.com/DanEngelbrecht/golongtail/longtaillib => ../longtaillib
