module github.com/buildbarn/bb-remote-execution

go 1.15

replace github.com/golang/mock => github.com/golang/mock v1.4.4-0.20201026142858-99aa9272d551

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9

require (
	github.com/aws/aws-sdk-go v1.37.28
	github.com/bazelbuild/remote-apis v0.0.0-20210309154856-0943dc4e70e1
	github.com/buildbarn/bb-storage v0.0.0-20210312082612-b15cd2841ed1
	github.com/golang/protobuf v1.4.3
	github.com/google/uuid v1.2.0
	github.com/gorilla/mux v1.8.0
	github.com/hanwen/go-fuse/v2 v2.0.3
	github.com/prometheus/client_golang v1.9.0
	go.opencensus.io v0.23.0
	golang.org/x/sys v0.0.0-20210309074719-68d13333faf2
	google.golang.org/genproto v0.0.0-20210310155132-4ce2db91004e
	google.golang.org/grpc v1.36.0
	google.golang.org/protobuf v1.25.0
)
