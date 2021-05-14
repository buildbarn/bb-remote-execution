module github.com/buildbarn/bb-remote-execution

go 1.16

replace github.com/golang/mock => github.com/golang/mock v1.4.4-0.20201026142858-99aa9272d551

replace github.com/gordonklaus/ineffassign => github.com/gordonklaus/ineffassign v0.0.0-20201223204552-cba2d2a1d5d9

require (
	github.com/aws/aws-sdk-go v1.38.2
	github.com/bazelbuild/remote-apis v0.0.0-20210505181611-ce7036ef5417
	github.com/buildbarn/bb-storage v0.0.0-20210507062155-c7b69db08ae0
	github.com/golang/mock v1.4.3
	github.com/golang/protobuf v1.5.1
	github.com/google/uuid v1.2.0
	github.com/gorilla/mux v1.8.0
	github.com/hanwen/go-fuse/v2 v2.1.0
	github.com/prometheus/client_golang v1.10.0
	github.com/stretchr/testify v1.7.0
	go.opencensus.io v0.23.0
	golang.org/x/sys v0.0.0-20210320140829-1e4c9ba3b0c4
	google.golang.org/genproto v0.0.0-20210310155132-4ce2db91004e
	google.golang.org/grpc v1.36.0
	google.golang.org/protobuf v1.26.0
)
