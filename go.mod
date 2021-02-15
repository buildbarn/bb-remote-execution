module github.com/buildbarn/bb-remote-execution

go 1.15

require (
	github.com/aws/aws-sdk-go v1.37.6
	github.com/bazelbuild/remote-apis v0.0.0-20201209220655-9e72daff42c9
	github.com/buildbarn/bb-storage v0.0.0-20210215081330-49417e72a945
	github.com/golang/protobuf v1.4.3
	github.com/google/uuid v1.2.0
	github.com/gorilla/mux v1.8.0
	github.com/hanwen/go-fuse/v2 v2.0.3
	github.com/prometheus/client_golang v1.9.0
	go.opencensus.io v0.22.6
	golang.org/x/sys v0.0.0-20210124154548-22da62e12c0c
	google.golang.org/genproto v0.0.0-20210207032614-bba0dbe2a9ea
	google.golang.org/grpc v1.35.0
)

replace github.com/golang/mock => github.com/golang/mock v1.4.4-0.20201026142858-99aa9272d551

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9
