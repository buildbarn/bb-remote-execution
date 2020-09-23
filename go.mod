module github.com/buildbarn/bb-remote-execution

go 1.14

replace github.com/buildbarn/bb-storage => github.com/asuffield/bb-storage v0.0.0-20200923142652-e54eb57ee7ad

require (
	contrib.go.opencensus.io/exporter/jaeger v0.2.0 // indirect
	contrib.go.opencensus.io/exporter/prometheus v0.2.0 // indirect
	github.com/Azure/azure-storage-blob-go v0.10.0 // indirect
	github.com/aws/aws-sdk-go v1.34.9
	github.com/bazelbuild/remote-apis v0.0.0-20200708200203-1252343900d9
	github.com/buildbarn/bb-storage v0.0.0-20200917193456-be1ddb93b029
	github.com/golang/lint v0.0.0-20180702182130-06c8688daad7 // indirect
	github.com/golang/mock v1.4.4
	github.com/golang/protobuf v1.4.2
	github.com/google/go-jsonnet v0.16.0 // indirect
	github.com/google/uuid v1.1.1
	github.com/gorilla/mux v1.8.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/lazybeaver/xorshift v0.0.0-20170702203709-ce511d4823dd // indirect
	github.com/prometheus/client_golang v1.7.1
	github.com/stretchr/testify v1.6.1
	go.opencensus.io v0.22.4 // indirect
	go.opentelemetry.io/otel v0.11.0
	gocloud.dev v0.20.0 // indirect
	golang.org/x/sys v0.0.0-20200803210538-64077c9b5642
	google.golang.org/genproto v0.0.0-20200815001618-f69a88009b70
	google.golang.org/grpc v1.31.0
)
