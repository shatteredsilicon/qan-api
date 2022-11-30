module github.com/shatteredsilicon/qan-api

go 1.17

require (
	github.com/cactus/go-statsd-client v3.1.1+incompatible
	github.com/go-sql-driver/mysql v1.6.0
	github.com/hashicorp/go-version v1.5.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/nu7hatch/gouuid v0.0.0-20131221200532-179d4d0c4d8d
	github.com/percona/go-mysql v0.0.0-20180913152646-8863d30f944b
	github.com/pkg/errors v0.8.1
	github.com/revel/config v0.14.0
	github.com/revel/modules v0.14.0
	github.com/revel/revel v0.14.0
	github.com/shatteredsilicon/ssm v0.0.0-20220807201647-7a9f3c5b23e3
	github.com/stretchr/testify v1.7.1
	github.com/youtube/vitess v2.1.0-alpha.1.0.20180131153458-98f9189aa016+incompatible
	golang.org/x/net v0.0.0-20220520000938-2e3eb7b945c2
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
)

require (
	github.com/agtorre/gocolorize v1.0.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/klauspost/compress v1.15.12 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/robfig/pathtree v0.0.0-20140121041023-41257a1839e9 // indirect
	github.com/rogpeppe/go-internal v1.8.1 // indirect
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
	google.golang.org/genproto v0.0.0-20220519153652-3a47de7e79bd // indirect
	google.golang.org/grpc v1.46.2 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/fsnotify.v1 v1.5.4 // indirect
	gopkg.in/yaml.v3 v3.0.0 // indirect
)

replace gopkg.in/fsnotify.v1 v1.5.4 => github.com/fsnotify/fsnotify v1.5.4
