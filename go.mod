module github.com/shatteredsilicon/qan-api

go 1.23.8

toolchain go1.24.2

require (
	github.com/cactus/go-statsd-client v3.1.1+incompatible
	github.com/go-sql-driver/mysql v1.7.1
	github.com/hashicorp/go-version v1.7.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/nu7hatch/gouuid v0.0.0-20131221200532-179d4d0c4d8d
	github.com/pkg/errors v0.9.1
	github.com/revel/config v0.14.0
	github.com/revel/modules v0.14.0
	github.com/revel/revel v0.14.0
	github.com/shatteredsilicon/ssm v0.0.0-20250716161023-a4554c8f2a4c
	github.com/stretchr/testify v1.9.0
	golang.org/x/net v0.36.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	vitess.io/vitess v0.21.4
)

require (
	github.com/agtorre/gocolorize v1.0.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/golang/glog v1.2.4 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/lib/pq v1.10.2 // indirect
	github.com/mattn/go-sqlite3 v1.14.16 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/robfig/pathtree v0.0.0-20140121041023-41257a1839e9 // indirect
	github.com/rogpeppe/go-internal v1.12.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/sys v0.30.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/grpc v1.66.2 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/fsnotify.v1 v1.5.4 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace gopkg.in/fsnotify.v1 v1.5.4 => github.com/fsnotify/fsnotify v1.5.4
