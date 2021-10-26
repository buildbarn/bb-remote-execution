module github.com/buildbarn/bb-remote-execution

go 1.16

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9

require (
	github.com/aws/aws-sdk-go-v2 v1.9.1
	github.com/aws/aws-sdk-go-v2/service/autoscaling v1.12.1
	github.com/aws/aws-sdk-go-v2/service/sqs v1.9.1
	github.com/aws/smithy-go v1.8.0
	github.com/bazelbuild/remote-apis v0.0.0-20211004185116-636121a32fa7
	github.com/buildbarn/bb-storage v0.0.0-20211025131958-f137922b0c40
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/hanwen/go-fuse/v2 v2.1.0
	github.com/prometheus/client_golang v1.11.0
	go.opentelemetry.io/otel v1.0.1
	go.opentelemetry.io/otel/trace v1.0.1
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20211023085530-d6a326fbbf70
	google.golang.org/genproto v0.0.0-20211008145708-270636b82663
	google.golang.org/grpc v1.41.0
	google.golang.org/protobuf v1.27.1
)
