// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v5.27.1
// source: pkg/proto/buildqueuestate/buildqueuestate.proto

package buildqueuestate

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	BuildQueueState_GetOperation_FullMethodName           = "/buildbarn.buildqueuestate.BuildQueueState/GetOperation"
	BuildQueueState_ListOperations_FullMethodName         = "/buildbarn.buildqueuestate.BuildQueueState/ListOperations"
	BuildQueueState_KillOperations_FullMethodName         = "/buildbarn.buildqueuestate.BuildQueueState/KillOperations"
	BuildQueueState_ListPlatformQueues_FullMethodName     = "/buildbarn.buildqueuestate.BuildQueueState/ListPlatformQueues"
	BuildQueueState_ListInvocationChildren_FullMethodName = "/buildbarn.buildqueuestate.BuildQueueState/ListInvocationChildren"
	BuildQueueState_ListQueuedOperations_FullMethodName   = "/buildbarn.buildqueuestate.BuildQueueState/ListQueuedOperations"
	BuildQueueState_ListWorkers_FullMethodName            = "/buildbarn.buildqueuestate.BuildQueueState/ListWorkers"
	BuildQueueState_TerminateWorkers_FullMethodName       = "/buildbarn.buildqueuestate.BuildQueueState/TerminateWorkers"
	BuildQueueState_ListDrains_FullMethodName             = "/buildbarn.buildqueuestate.BuildQueueState/ListDrains"
	BuildQueueState_AddDrain_FullMethodName               = "/buildbarn.buildqueuestate.BuildQueueState/AddDrain"
	BuildQueueState_RemoveDrain_FullMethodName            = "/buildbarn.buildqueuestate.BuildQueueState/RemoveDrain"
)

// BuildQueueStateClient is the client API for BuildQueueState service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type BuildQueueStateClient interface {
	GetOperation(ctx context.Context, in *GetOperationRequest, opts ...grpc.CallOption) (*GetOperationResponse, error)
	ListOperations(ctx context.Context, in *ListOperationsRequest, opts ...grpc.CallOption) (*ListOperationsResponse, error)
	KillOperations(ctx context.Context, in *KillOperationsRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	ListPlatformQueues(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListPlatformQueuesResponse, error)
	ListInvocationChildren(ctx context.Context, in *ListInvocationChildrenRequest, opts ...grpc.CallOption) (*ListInvocationChildrenResponse, error)
	ListQueuedOperations(ctx context.Context, in *ListQueuedOperationsRequest, opts ...grpc.CallOption) (*ListQueuedOperationsResponse, error)
	ListWorkers(ctx context.Context, in *ListWorkersRequest, opts ...grpc.CallOption) (*ListWorkersResponse, error)
	TerminateWorkers(ctx context.Context, in *TerminateWorkersRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	ListDrains(ctx context.Context, in *ListDrainsRequest, opts ...grpc.CallOption) (*ListDrainsResponse, error)
	AddDrain(ctx context.Context, in *AddOrRemoveDrainRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	RemoveDrain(ctx context.Context, in *AddOrRemoveDrainRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type buildQueueStateClient struct {
	cc grpc.ClientConnInterface
}

func NewBuildQueueStateClient(cc grpc.ClientConnInterface) BuildQueueStateClient {
	return &buildQueueStateClient{cc}
}

func (c *buildQueueStateClient) GetOperation(ctx context.Context, in *GetOperationRequest, opts ...grpc.CallOption) (*GetOperationResponse, error) {
	out := new(GetOperationResponse)
	err := c.cc.Invoke(ctx, BuildQueueState_GetOperation_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *buildQueueStateClient) ListOperations(ctx context.Context, in *ListOperationsRequest, opts ...grpc.CallOption) (*ListOperationsResponse, error) {
	out := new(ListOperationsResponse)
	err := c.cc.Invoke(ctx, BuildQueueState_ListOperations_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *buildQueueStateClient) KillOperations(ctx context.Context, in *KillOperationsRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, BuildQueueState_KillOperations_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *buildQueueStateClient) ListPlatformQueues(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ListPlatformQueuesResponse, error) {
	out := new(ListPlatformQueuesResponse)
	err := c.cc.Invoke(ctx, BuildQueueState_ListPlatformQueues_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *buildQueueStateClient) ListInvocationChildren(ctx context.Context, in *ListInvocationChildrenRequest, opts ...grpc.CallOption) (*ListInvocationChildrenResponse, error) {
	out := new(ListInvocationChildrenResponse)
	err := c.cc.Invoke(ctx, BuildQueueState_ListInvocationChildren_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *buildQueueStateClient) ListQueuedOperations(ctx context.Context, in *ListQueuedOperationsRequest, opts ...grpc.CallOption) (*ListQueuedOperationsResponse, error) {
	out := new(ListQueuedOperationsResponse)
	err := c.cc.Invoke(ctx, BuildQueueState_ListQueuedOperations_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *buildQueueStateClient) ListWorkers(ctx context.Context, in *ListWorkersRequest, opts ...grpc.CallOption) (*ListWorkersResponse, error) {
	out := new(ListWorkersResponse)
	err := c.cc.Invoke(ctx, BuildQueueState_ListWorkers_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *buildQueueStateClient) TerminateWorkers(ctx context.Context, in *TerminateWorkersRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, BuildQueueState_TerminateWorkers_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *buildQueueStateClient) ListDrains(ctx context.Context, in *ListDrainsRequest, opts ...grpc.CallOption) (*ListDrainsResponse, error) {
	out := new(ListDrainsResponse)
	err := c.cc.Invoke(ctx, BuildQueueState_ListDrains_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *buildQueueStateClient) AddDrain(ctx context.Context, in *AddOrRemoveDrainRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, BuildQueueState_AddDrain_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *buildQueueStateClient) RemoveDrain(ctx context.Context, in *AddOrRemoveDrainRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, BuildQueueState_RemoveDrain_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BuildQueueStateServer is the server API for BuildQueueState service.
// All implementations should embed UnimplementedBuildQueueStateServer
// for forward compatibility
type BuildQueueStateServer interface {
	GetOperation(context.Context, *GetOperationRequest) (*GetOperationResponse, error)
	ListOperations(context.Context, *ListOperationsRequest) (*ListOperationsResponse, error)
	KillOperations(context.Context, *KillOperationsRequest) (*emptypb.Empty, error)
	ListPlatformQueues(context.Context, *emptypb.Empty) (*ListPlatformQueuesResponse, error)
	ListInvocationChildren(context.Context, *ListInvocationChildrenRequest) (*ListInvocationChildrenResponse, error)
	ListQueuedOperations(context.Context, *ListQueuedOperationsRequest) (*ListQueuedOperationsResponse, error)
	ListWorkers(context.Context, *ListWorkersRequest) (*ListWorkersResponse, error)
	TerminateWorkers(context.Context, *TerminateWorkersRequest) (*emptypb.Empty, error)
	ListDrains(context.Context, *ListDrainsRequest) (*ListDrainsResponse, error)
	AddDrain(context.Context, *AddOrRemoveDrainRequest) (*emptypb.Empty, error)
	RemoveDrain(context.Context, *AddOrRemoveDrainRequest) (*emptypb.Empty, error)
}

// UnimplementedBuildQueueStateServer should be embedded to have forward compatible implementations.
type UnimplementedBuildQueueStateServer struct {
}

func (UnimplementedBuildQueueStateServer) GetOperation(context.Context, *GetOperationRequest) (*GetOperationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetOperation not implemented")
}
func (UnimplementedBuildQueueStateServer) ListOperations(context.Context, *ListOperationsRequest) (*ListOperationsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListOperations not implemented")
}
func (UnimplementedBuildQueueStateServer) KillOperations(context.Context, *KillOperationsRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KillOperations not implemented")
}
func (UnimplementedBuildQueueStateServer) ListPlatformQueues(context.Context, *emptypb.Empty) (*ListPlatformQueuesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListPlatformQueues not implemented")
}
func (UnimplementedBuildQueueStateServer) ListInvocationChildren(context.Context, *ListInvocationChildrenRequest) (*ListInvocationChildrenResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListInvocationChildren not implemented")
}
func (UnimplementedBuildQueueStateServer) ListQueuedOperations(context.Context, *ListQueuedOperationsRequest) (*ListQueuedOperationsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListQueuedOperations not implemented")
}
func (UnimplementedBuildQueueStateServer) ListWorkers(context.Context, *ListWorkersRequest) (*ListWorkersResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListWorkers not implemented")
}
func (UnimplementedBuildQueueStateServer) TerminateWorkers(context.Context, *TerminateWorkersRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TerminateWorkers not implemented")
}
func (UnimplementedBuildQueueStateServer) ListDrains(context.Context, *ListDrainsRequest) (*ListDrainsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListDrains not implemented")
}
func (UnimplementedBuildQueueStateServer) AddDrain(context.Context, *AddOrRemoveDrainRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddDrain not implemented")
}
func (UnimplementedBuildQueueStateServer) RemoveDrain(context.Context, *AddOrRemoveDrainRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RemoveDrain not implemented")
}

// UnsafeBuildQueueStateServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to BuildQueueStateServer will
// result in compilation errors.
type UnsafeBuildQueueStateServer interface {
	mustEmbedUnimplementedBuildQueueStateServer()
}

func RegisterBuildQueueStateServer(s grpc.ServiceRegistrar, srv BuildQueueStateServer) {
	s.RegisterService(&BuildQueueState_ServiceDesc, srv)
}

func _BuildQueueState_GetOperation_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetOperationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BuildQueueStateServer).GetOperation(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BuildQueueState_GetOperation_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BuildQueueStateServer).GetOperation(ctx, req.(*GetOperationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BuildQueueState_ListOperations_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListOperationsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BuildQueueStateServer).ListOperations(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BuildQueueState_ListOperations_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BuildQueueStateServer).ListOperations(ctx, req.(*ListOperationsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BuildQueueState_KillOperations_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(KillOperationsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BuildQueueStateServer).KillOperations(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BuildQueueState_KillOperations_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BuildQueueStateServer).KillOperations(ctx, req.(*KillOperationsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BuildQueueState_ListPlatformQueues_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BuildQueueStateServer).ListPlatformQueues(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BuildQueueState_ListPlatformQueues_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BuildQueueStateServer).ListPlatformQueues(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _BuildQueueState_ListInvocationChildren_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListInvocationChildrenRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BuildQueueStateServer).ListInvocationChildren(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BuildQueueState_ListInvocationChildren_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BuildQueueStateServer).ListInvocationChildren(ctx, req.(*ListInvocationChildrenRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BuildQueueState_ListQueuedOperations_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListQueuedOperationsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BuildQueueStateServer).ListQueuedOperations(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BuildQueueState_ListQueuedOperations_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BuildQueueStateServer).ListQueuedOperations(ctx, req.(*ListQueuedOperationsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BuildQueueState_ListWorkers_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListWorkersRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BuildQueueStateServer).ListWorkers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BuildQueueState_ListWorkers_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BuildQueueStateServer).ListWorkers(ctx, req.(*ListWorkersRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BuildQueueState_TerminateWorkers_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TerminateWorkersRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BuildQueueStateServer).TerminateWorkers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BuildQueueState_TerminateWorkers_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BuildQueueStateServer).TerminateWorkers(ctx, req.(*TerminateWorkersRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BuildQueueState_ListDrains_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListDrainsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BuildQueueStateServer).ListDrains(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BuildQueueState_ListDrains_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BuildQueueStateServer).ListDrains(ctx, req.(*ListDrainsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BuildQueueState_AddDrain_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddOrRemoveDrainRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BuildQueueStateServer).AddDrain(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BuildQueueState_AddDrain_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BuildQueueStateServer).AddDrain(ctx, req.(*AddOrRemoveDrainRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _BuildQueueState_RemoveDrain_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddOrRemoveDrainRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BuildQueueStateServer).RemoveDrain(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BuildQueueState_RemoveDrain_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BuildQueueStateServer).RemoveDrain(ctx, req.(*AddOrRemoveDrainRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// BuildQueueState_ServiceDesc is the grpc.ServiceDesc for BuildQueueState service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var BuildQueueState_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "buildbarn.buildqueuestate.BuildQueueState",
	HandlerType: (*BuildQueueStateServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetOperation",
			Handler:    _BuildQueueState_GetOperation_Handler,
		},
		{
			MethodName: "ListOperations",
			Handler:    _BuildQueueState_ListOperations_Handler,
		},
		{
			MethodName: "KillOperations",
			Handler:    _BuildQueueState_KillOperations_Handler,
		},
		{
			MethodName: "ListPlatformQueues",
			Handler:    _BuildQueueState_ListPlatformQueues_Handler,
		},
		{
			MethodName: "ListInvocationChildren",
			Handler:    _BuildQueueState_ListInvocationChildren_Handler,
		},
		{
			MethodName: "ListQueuedOperations",
			Handler:    _BuildQueueState_ListQueuedOperations_Handler,
		},
		{
			MethodName: "ListWorkers",
			Handler:    _BuildQueueState_ListWorkers_Handler,
		},
		{
			MethodName: "TerminateWorkers",
			Handler:    _BuildQueueState_TerminateWorkers_Handler,
		},
		{
			MethodName: "ListDrains",
			Handler:    _BuildQueueState_ListDrains_Handler,
		},
		{
			MethodName: "AddDrain",
			Handler:    _BuildQueueState_AddDrain_Handler,
		},
		{
			MethodName: "RemoveDrain",
			Handler:    _BuildQueueState_RemoveDrain_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/proto/buildqueuestate/buildqueuestate.proto",
}
