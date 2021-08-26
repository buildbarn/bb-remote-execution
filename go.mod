module github.com/buildbarn/bb-remote-execution

go 1.16

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9

require (
	github.com/aws/aws-sdk-go v1.40.30
	github.com/bazelbuild/remote-apis v0.0.0-20210812183132-3e816456ee28
	github.com/buildbarn/bb-storage v0.0.0-20210824114323-bba43727809c
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/hanwen/go-fuse/v2 v2.1.0
	github.com/prometheus/client_golang v1.11.0
	go.opentelemetry.io/otel v1.0.0-RC2
	go.opentelemetry.io/otel/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/trace v1.0.0-RC2
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210823070655-63515b42dcdf
	google.golang.org/genproto v0.0.0-20210825212027-de86158e7fda
	google.golang.org/grpc v1.40.0
	google.golang.org/protobuf v1.27.1
)
