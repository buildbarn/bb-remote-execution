// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v5.27.1
// source: pkg/proto/bazeloutputservice/bazel_output_service.proto

package bazeloutputservice

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	BazelOutputService_Clean_FullMethodName             = "/bazel_output_service.BazelOutputService/Clean"
	BazelOutputService_StartBuild_FullMethodName        = "/bazel_output_service.BazelOutputService/StartBuild"
	BazelOutputService_StageArtifacts_FullMethodName    = "/bazel_output_service.BazelOutputService/StageArtifacts"
	BazelOutputService_FinalizeArtifacts_FullMethodName = "/bazel_output_service.BazelOutputService/FinalizeArtifacts"
	BazelOutputService_FinalizeBuild_FullMethodName     = "/bazel_output_service.BazelOutputService/FinalizeBuild"
	BazelOutputService_BatchStat_FullMethodName         = "/bazel_output_service.BazelOutputService/BatchStat"
)

// BazelOutputServiceClient is the client API for BazelOutputService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type BazelOutputServiceClient interface {
	Clean(ctx context.Context, in *CleanRequest, opts ...grpc.CallOption) (*CleanResponse, error)
	StartBuild(ctx context.Context, in *StartBuildRequest, opts ...grpc.CallOption) (*StartBuildResponse, error)
	StageArtifacts(ctx context.Context, in *StageArtifactsRequest, opts ...grpc.CallOption) (*StageArtifactsResponse, error)
	FinalizeArtifacts(ctx context.Context, in *FinalizeArtifactsRequest, opts ...grpc.CallOption) (*FinalizeArtifactsResponse, error)
	FinalizeBuild(ctx context.Context, in *FinalizeBuildRequest, opts ...grpc.CallOption) (*FinalizeBuildResponse, error)
	BatchStat(ctx context.Context, in *BatchStatRequest, opts ...grpc.CallOption) (*BatchStatResponse, error)
}

type bazelOutputServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewBazelOutputServiceClient(cc grpc.ClientConnInterface) BazelOutputServiceClient {
	return &bazelOutputServiceClient{cc}
}

func (c *bazelOutputServiceClient) Clean(ctx context.Context, in *CleanRequest, opts ...grpc.CallOption) (*CleanResponse, error) {
	out := new(CleanResponse)
	err := c.cc.Invoke(ctx, BazelOutputService_Clean_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *bazelOutputServiceClient) StartBuild(ctx context.Context, in *StartBuildRequest, opts ...grpc.CallOption) (*StartBuildResponse, error) {
	out := new(StartBuildResponse)
	err := c.cc.Invoke(ctx, BazelOutputService_StartBuild_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *bazelOutputServiceClient) StageArtifacts(ctx context.Context, in *StageArtifactsRequest, opts ...grpc.CallOption) (*StageArtifactsResponse, error) {
	out := new(StageArtifactsResponse)
	err := c.cc.Invoke(ctx, BazelOutputService_StageArtifacts_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *bazelOutputServiceClient) FinalizeArtifacts(ctx context.Context, in *FinalizeArtifactsRequest, opts ...grpc.CallOption) (*FinalizeArtifactsResponse, error) {
	out := new(FinalizeArtifactsResponse)
	err := c.cc.Invoke(ctx, BazelOutputService_FinalizeArtifacts_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *bazelOutputServiceClient) FinalizeBuild(ctx context.Context, in *FinalizeBuildRequest, opts ...grpc.CallOption) (*FinalizeBuildResponse, error) {
	out := new(FinalizeBuildResponse)
	err := c.cc.Invoke(ctx, BazelOutputService_FinalizeBuild_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *bazelOutputServiceClient) BatchStat(ctx context.Context, in *BatchStatRequest, opts ...grpc.CallOption) (*BatchStatResponse, error) {
	out := new(BatchStatResponse)
	err := c.cc.Invoke(ctx, BazelOutputService_BatchStat_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BazelOutputServiceServer is the server API for BazelOutputService service.
// All implementations should embed UnimplementedBazelOutputServiceServer
// for forward compatibility
type BazelOutputServiceServer interface {
	Clean(context.Context, *CleanRequest) (*CleanResponse, error)
	StartBuild(context.Context, *StartBuildRequest) (*StartBuildResponse, error)
	StageArtifacts(context.Context, *StageArtifactsRequest) (*StageArtifactsResponse, error)
	FinalizeArtifacts(context.Context, *FinalizeArtifactsRequest) (*FinalizeArtifactsResponse, error)
	FinalizeBuild(context.Context, *FinalizeBuildRequest) (*FinalizeBuildResponse, error)
	BatchStat(context.Context, *BatchStatRequest) (*BatchStatResponse, error)
}

// UnimplementedBazelOutputServiceServer should be embedded to have forward compatible implementations.
type UnimplementedBazelOutputServiceServer struct {
}

func (UnimplementedBazelOutputServiceServer) Clean(context.Context, *CleanRequest) (*CleanResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Clean not implemented")
}
func (UnimplementedBazelOutputServiceServer) StartBuild(context.Context, *StartBuildRequest) (*StartBuildResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StartBuild not implemented")
}
func (UnimplementedBazelOutputServiceServer) StageArtifacts(context.Context, *StageArtifactsRequest) (*StageArtifactsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StageArtifacts not implemented")
}
func (UnimplementedBazelOutputServiceServer) FinalizeArtifacts(context.Context, *FinalizeArtifactsRequest) (*FinalizeArtifactsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FinalizeArtifacts not implemented")
}
func (UnimplementedBazelOutputServiceServer) FinalizeBuild(context.Context, *FinalizeBuildRequest) (*FinalizeBuildResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FinalizeBuild not implemented")
}
func (UnimplementedBazelOutputServiceServer) BatchStat(context.Context, *BatchStatRequest) (*BatchStatResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method BatchStat not implemented")
}

// UnsafeBazelOutputServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to BazelOutputServiceServer will
// result in compilation errors.
type UnsafeBazelOutputServiceServer interface {
	mustEmbedUnimplementedBazelOutputServiceServer()
}

func RegisterBazelOutputServiceServer(s grpc.ServiceRegistrar, srv BazelOutputServiceServer) {
	s.RegisterService(&BazelOutputService_ServiceDesc, srv)
}

func _BazelOutputService_Clean_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CleanRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BazelOutputServiceServer).Clean(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BazelOutputService_Clean_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BazelOutputServiceServer).Clean(ctx, req.(*CleanRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BazelOutputService_StartBuild_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StartBuildRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BazelOutputServiceServer).StartBuild(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BazelOutputService_StartBuild_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BazelOutputServiceServer).StartBuild(ctx, req.(*StartBuildRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BazelOutputService_StageArtifacts_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StageArtifactsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BazelOutputServiceServer).StageArtifacts(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BazelOutputService_StageArtifacts_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BazelOutputServiceServer).StageArtifacts(ctx, req.(*StageArtifactsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BazelOutputService_FinalizeArtifacts_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FinalizeArtifactsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BazelOutputServiceServer).FinalizeArtifacts(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BazelOutputService_FinalizeArtifacts_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BazelOutputServiceServer).FinalizeArtifacts(ctx, req.(*FinalizeArtifactsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BazelOutputService_FinalizeBuild_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FinalizeBuildRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BazelOutputServiceServer).FinalizeBuild(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BazelOutputService_FinalizeBuild_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BazelOutputServiceServer).FinalizeBuild(ctx, req.(*FinalizeBuildRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BazelOutputService_BatchStat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BatchStatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BazelOutputServiceServer).BatchStat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BazelOutputService_BatchStat_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BazelOutputServiceServer).BatchStat(ctx, req.(*BatchStatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// BazelOutputService_ServiceDesc is the grpc.ServiceDesc for BazelOutputService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var BazelOutputService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "bazel_output_service.BazelOutputService",
	HandlerType: (*BazelOutputServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Clean",
			Handler:    _BazelOutputService_Clean_Handler,
		},
		{
			MethodName: "StartBuild",
			Handler:    _BazelOutputService_StartBuild_Handler,
		},
		{
			MethodName: "StageArtifacts",
			Handler:    _BazelOutputService_StageArtifacts_Handler,
		},
		{
			MethodName: "FinalizeArtifacts",
			Handler:    _BazelOutputService_FinalizeArtifacts_Handler,
		},
		{
			MethodName: "FinalizeBuild",
			Handler:    _BazelOutputService_FinalizeBuild_Handler,
		},
		{
			MethodName: "BatchStat",
			Handler:    _BazelOutputService_BatchStat_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/proto/bazeloutputservice/bazel_output_service.proto",
}
