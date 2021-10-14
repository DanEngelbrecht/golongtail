module github.com/DanEngelbrecht/golongtail/cmd/longtail

go 1.17

require (
	github.com/DanEngelbrecht/golongtail/commands v0.0.0-00010101000000-000000000000
	github.com/DanEngelbrecht/golongtail/longtaillib v0.0.0-00010101000000-000000000000
	github.com/DanEngelbrecht/golongtail/longtailutils v0.0.0-00010101000000-000000000000
	github.com/alecthomas/kong v0.2.17
	github.com/sirupsen/logrus v1.8.1
)

require (
	cloud.google.com/go v0.95.0 // indirect
	cloud.google.com/go/storage v1.16.1 // indirect
	github.com/DanEngelbrecht/golongtail/longtailstorelib v0.0.0-00010101000000-000000000000 // indirect
	github.com/DanEngelbrecht/golongtail/remotestore v0.0.0-00010101000000-000000000000 // indirect
	github.com/aws/aws-sdk-go-v2 v1.9.1 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.8.2 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.4.2 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.5.1 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.2.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.3.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.3.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.7.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.16.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.4.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.7.1 // indirect
	github.com/aws/smithy-go v1.8.0 // indirect
	github.com/fsnotify/fsnotify v1.5.1 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.6 // indirect
	github.com/googleapis/gax-go/v2 v2.1.1 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/magiconair/properties v1.8.5 // indirect
	github.com/mitchellh/mapstructure v1.4.2 // indirect
	github.com/pelletier/go-toml v1.9.4 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/spf13/afero v1.6.0 // indirect
	github.com/spf13/cast v1.4.1 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/spf13/viper v1.9.0 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	go.opencensus.io v0.23.0 // indirect
	golang.org/x/net v0.0.0-20210917221730-978cfadd31cf // indirect
	golang.org/x/oauth2 v0.0.0-20210819190943-2bc19b11175f // indirect
	golang.org/x/sys v0.0.0-20210921065528-437939a70204 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/api v0.57.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20210921142501-181ce0d877f6 // indirect
	google.golang.org/grpc v1.40.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/ini.v1 v1.63.2 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/DanEngelbrecht/golongtail/commands => ../../commands

replace github.com/DanEngelbrecht/golongtail/longtaillib => ../../longtaillib

replace github.com/DanEngelbrecht/golongtail/longtailstorelib => ../../longtailstorelib

replace github.com/DanEngelbrecht/golongtail/longtailutils => ../../longtailutils

replace github.com/DanEngelbrecht/golongtail/remotestore => ../../remotestore
