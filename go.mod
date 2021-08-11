module github.com/buildbarn/bb-remote-execution

go 1.16

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9

require (
	github.com/aws/aws-sdk-go v1.40.19
	github.com/bazelbuild/remote-apis v0.0.0-20210718193713-0ecef08215cf
	github.com/buildbarn/bb-storage v0.0.0-20210811060635-8cbbc0029847
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/hanwen/go-fuse/v2 v2.1.0
	github.com/prometheus/client_golang v1.11.0
	go.opentelemetry.io/otel v1.0.0-RC2
	go.opentelemetry.io/otel/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/trace v1.0.0-RC2
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210809222454-d867a43fc93e
	google.golang.org/genproto v0.0.0-20210811021853-ddbe55d93216
	google.golang.org/grpc v1.39.1
	google.golang.org/protobuf v1.27.1
)
