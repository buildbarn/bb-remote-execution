module github.com/buildbarn/bb-remote-execution

go 1.20

// https://github.com/grpc-ecosystem/grpc-gateway/commit/5f9757f31b517d98095209636b2b88cd6326572f
replace github.com/grpc-ecosystem/grpc-gateway/v2 => github.com/grpc-ecosystem/grpc-gateway/v2 v2.16.1

require (
	cloud.google.com/go/longrunning v0.5.1
	github.com/bazelbuild/remote-apis v0.0.0-20230822133051-6c32c3b917cc
	github.com/buildbarn/bb-storage v0.0.0-20231030120605-519a8946d90d
	github.com/buildbarn/go-xdr v0.0.0-20231002195348-0d2d95eab08c
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.3
	github.com/google/uuid v1.3.1
	github.com/gorilla/mux v1.8.0
	github.com/hanwen/go-fuse/v2 v2.4.0
	github.com/jmespath/go-jmespath v0.4.0
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51
	github.com/prometheus/client_golang v1.17.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.8.4
	go.opentelemetry.io/otel v1.19.0
	go.opentelemetry.io/otel/trace v1.19.0
	golang.org/x/sync v0.4.0
	golang.org/x/sys v0.13.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231009173412-8bfb1ae86b6c
	google.golang.org/grpc v1.58.2
	google.golang.org/protobuf v1.31.0
)

require (
	cloud.google.com/go v0.110.8 // indirect
	cloud.google.com/go/compute v1.23.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v1.1.2 // indirect
	cloud.google.com/go/storage v1.33.0 // indirect
	github.com/aohorodnyk/mimeheader v0.0.6 // indirect
	github.com/aws/aws-sdk-go-v2 v1.21.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.13 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.18.41 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.13.39 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.13.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.41 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.42 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.1.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.14 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.36 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.35 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.15.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.39.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.14.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.17.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.22.0 // indirect
	github.com/aws/smithy-go v1.14.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fxtlabs/primes v0.0.0-20150821004651-dad82d10a449 // indirect
	github.com/go-jose/go-jose/v3 v3.0.0 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/go-jsonnet v0.20.0 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.5 // indirect
	github.com/googleapis/gax-go/v2 v2.12.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.16.0 // indirect
	github.com/klauspost/compress v1.17.0 // indirect
	github.com/lazybeaver/xorshift v0.0.0-20170702203709-ce511d4823dd // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.4.1-0.20230718164431-9a2bf3000d16 // indirect
	github.com/prometheus/common v0.44.0 // indirect
	github.com/prometheus/procfs v0.11.1 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.44.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.19.0 // indirect
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.18.0 // indirect
	go.opentelemetry.io/otel/metric v1.19.0 // indirect
	go.opentelemetry.io/otel/sdk v1.18.0 // indirect
	go.opentelemetry.io/proto/otlp v1.0.0 // indirect
	golang.org/x/crypto v0.13.0 // indirect
	golang.org/x/net v0.15.0 // indirect
	golang.org/x/oauth2 v0.12.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/api v0.142.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20231002182017-d307bd883b97 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230920204549-e6e6cdab5c13 // indirect
	google.golang.org/genproto/googleapis/bytestream v0.0.0-20230920204549-e6e6cdab5c13 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	sigs.k8s.io/yaml v1.1.0 // indirect
)
