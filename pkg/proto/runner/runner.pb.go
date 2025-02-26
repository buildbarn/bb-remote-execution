// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.5
// 	protoc        v5.29.3
// source: pkg/proto/runner/runner.proto

package runner

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	anypb "google.golang.org/protobuf/types/known/anypb"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type CheckReadinessRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Path          string                 `protobuf:"bytes,1,opt,name=path,proto3" json:"path,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CheckReadinessRequest) Reset() {
	*x = CheckReadinessRequest{}
	mi := &file_pkg_proto_runner_runner_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CheckReadinessRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CheckReadinessRequest) ProtoMessage() {}

func (x *CheckReadinessRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_runner_runner_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CheckReadinessRequest.ProtoReflect.Descriptor instead.
func (*CheckReadinessRequest) Descriptor() ([]byte, []int) {
	return file_pkg_proto_runner_runner_proto_rawDescGZIP(), []int{0}
}

func (x *CheckReadinessRequest) GetPath() string {
	if x != nil {
		return x.Path
	}
	return ""
}

type RunRequest struct {
	state                protoimpl.MessageState `protogen:"open.v1"`
	Arguments            []string               `protobuf:"bytes,1,rep,name=arguments,proto3" json:"arguments,omitempty"`
	EnvironmentVariables map[string]string      `protobuf:"bytes,2,rep,name=environment_variables,json=environmentVariables,proto3" json:"environment_variables,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	WorkingDirectory     string                 `protobuf:"bytes,3,opt,name=working_directory,json=workingDirectory,proto3" json:"working_directory,omitempty"`
	StdoutPath           string                 `protobuf:"bytes,4,opt,name=stdout_path,json=stdoutPath,proto3" json:"stdout_path,omitempty"`
	StderrPath           string                 `protobuf:"bytes,5,opt,name=stderr_path,json=stderrPath,proto3" json:"stderr_path,omitempty"`
	InputRootDirectory   string                 `protobuf:"bytes,6,opt,name=input_root_directory,json=inputRootDirectory,proto3" json:"input_root_directory,omitempty"`
	TemporaryDirectory   string                 `protobuf:"bytes,7,opt,name=temporary_directory,json=temporaryDirectory,proto3" json:"temporary_directory,omitempty"`
	ServerLogsDirectory  string                 `protobuf:"bytes,8,opt,name=server_logs_directory,json=serverLogsDirectory,proto3" json:"server_logs_directory,omitempty"`
	unknownFields        protoimpl.UnknownFields
	sizeCache            protoimpl.SizeCache
}

func (x *RunRequest) Reset() {
	*x = RunRequest{}
	mi := &file_pkg_proto_runner_runner_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RunRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RunRequest) ProtoMessage() {}

func (x *RunRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_runner_runner_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RunRequest.ProtoReflect.Descriptor instead.
func (*RunRequest) Descriptor() ([]byte, []int) {
	return file_pkg_proto_runner_runner_proto_rawDescGZIP(), []int{1}
}

func (x *RunRequest) GetArguments() []string {
	if x != nil {
		return x.Arguments
	}
	return nil
}

func (x *RunRequest) GetEnvironmentVariables() map[string]string {
	if x != nil {
		return x.EnvironmentVariables
	}
	return nil
}

func (x *RunRequest) GetWorkingDirectory() string {
	if x != nil {
		return x.WorkingDirectory
	}
	return ""
}

func (x *RunRequest) GetStdoutPath() string {
	if x != nil {
		return x.StdoutPath
	}
	return ""
}

func (x *RunRequest) GetStderrPath() string {
	if x != nil {
		return x.StderrPath
	}
	return ""
}

func (x *RunRequest) GetInputRootDirectory() string {
	if x != nil {
		return x.InputRootDirectory
	}
	return ""
}

func (x *RunRequest) GetTemporaryDirectory() string {
	if x != nil {
		return x.TemporaryDirectory
	}
	return ""
}

func (x *RunRequest) GetServerLogsDirectory() string {
	if x != nil {
		return x.ServerLogsDirectory
	}
	return ""
}

type RunResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	ExitCode      int64                  `protobuf:"varint,1,opt,name=exit_code,json=exitCode,proto3" json:"exit_code,omitempty"`
	ResourceUsage []*anypb.Any           `protobuf:"bytes,2,rep,name=resource_usage,json=resourceUsage,proto3" json:"resource_usage,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *RunResponse) Reset() {
	*x = RunResponse{}
	mi := &file_pkg_proto_runner_runner_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RunResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RunResponse) ProtoMessage() {}

func (x *RunResponse) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_runner_runner_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RunResponse.ProtoReflect.Descriptor instead.
func (*RunResponse) Descriptor() ([]byte, []int) {
	return file_pkg_proto_runner_runner_proto_rawDescGZIP(), []int{2}
}

func (x *RunResponse) GetExitCode() int64 {
	if x != nil {
		return x.ExitCode
	}
	return 0
}

func (x *RunResponse) GetResourceUsage() []*anypb.Any {
	if x != nil {
		return x.ResourceUsage
	}
	return nil
}

var File_pkg_proto_runner_runner_proto protoreflect.FileDescriptor

var file_pkg_proto_runner_runner_proto_rawDesc = string([]byte{
	0x0a, 0x1d, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x72, 0x75, 0x6e, 0x6e,
	0x65, 0x72, 0x2f, 0x72, 0x75, 0x6e, 0x6e, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x10, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x72, 0x75, 0x6e, 0x6e, 0x65,
	0x72, 0x1a, 0x19, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2f, 0x61, 0x6e, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1b, 0x67, 0x6f,
	0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x65, 0x6d,
	0x70, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x2b, 0x0a, 0x15, 0x43, 0x68, 0x65,
	0x63, 0x6b, 0x52, 0x65, 0x61, 0x64, 0x69, 0x6e, 0x65, 0x73, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x61, 0x74, 0x68, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x04, 0x70, 0x61, 0x74, 0x68, 0x22, 0xe6, 0x03, 0x0a, 0x0a, 0x52, 0x75, 0x6e, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1c, 0x0a, 0x09, 0x61, 0x72, 0x67, 0x75, 0x6d, 0x65, 0x6e,
	0x74, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x09, 0x61, 0x72, 0x67, 0x75, 0x6d, 0x65,
	0x6e, 0x74, 0x73, 0x12, 0x6b, 0x0a, 0x15, 0x65, 0x6e, 0x76, 0x69, 0x72, 0x6f, 0x6e, 0x6d, 0x65,
	0x6e, 0x74, 0x5f, 0x76, 0x61, 0x72, 0x69, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x36, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x72,
	0x75, 0x6e, 0x6e, 0x65, 0x72, 0x2e, 0x52, 0x75, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x2e, 0x45, 0x6e, 0x76, 0x69, 0x72, 0x6f, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x56, 0x61, 0x72, 0x69,
	0x61, 0x62, 0x6c, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x14, 0x65, 0x6e, 0x76, 0x69,
	0x72, 0x6f, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x56, 0x61, 0x72, 0x69, 0x61, 0x62, 0x6c, 0x65, 0x73,
	0x12, 0x2b, 0x0a, 0x11, 0x77, 0x6f, 0x72, 0x6b, 0x69, 0x6e, 0x67, 0x5f, 0x64, 0x69, 0x72, 0x65,
	0x63, 0x74, 0x6f, 0x72, 0x79, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x77, 0x6f, 0x72,
	0x6b, 0x69, 0x6e, 0x67, 0x44, 0x69, 0x72, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x79, 0x12, 0x1f, 0x0a,
	0x0b, 0x73, 0x74, 0x64, 0x6f, 0x75, 0x74, 0x5f, 0x70, 0x61, 0x74, 0x68, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0a, 0x73, 0x74, 0x64, 0x6f, 0x75, 0x74, 0x50, 0x61, 0x74, 0x68, 0x12, 0x1f,
	0x0a, 0x0b, 0x73, 0x74, 0x64, 0x65, 0x72, 0x72, 0x5f, 0x70, 0x61, 0x74, 0x68, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0a, 0x73, 0x74, 0x64, 0x65, 0x72, 0x72, 0x50, 0x61, 0x74, 0x68, 0x12,
	0x30, 0x0a, 0x14, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x5f, 0x72, 0x6f, 0x6f, 0x74, 0x5f, 0x64, 0x69,
	0x72, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x79, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x12, 0x69,
	0x6e, 0x70, 0x75, 0x74, 0x52, 0x6f, 0x6f, 0x74, 0x44, 0x69, 0x72, 0x65, 0x63, 0x74, 0x6f, 0x72,
	0x79, 0x12, 0x2f, 0x0a, 0x13, 0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x72, 0x79, 0x5f, 0x64,
	0x69, 0x72, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x79, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x12,
	0x74, 0x65, 0x6d, 0x70, 0x6f, 0x72, 0x61, 0x72, 0x79, 0x44, 0x69, 0x72, 0x65, 0x63, 0x74, 0x6f,
	0x72, 0x79, 0x12, 0x32, 0x0a, 0x15, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x5f, 0x6c, 0x6f, 0x67,
	0x73, 0x5f, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x79, 0x18, 0x08, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x13, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x4c, 0x6f, 0x67, 0x73, 0x44, 0x69, 0x72,
	0x65, 0x63, 0x74, 0x6f, 0x72, 0x79, 0x1a, 0x47, 0x0a, 0x19, 0x45, 0x6e, 0x76, 0x69, 0x72, 0x6f,
	0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x56, 0x61, 0x72, 0x69, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x45, 0x6e,
	0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22,
	0x67, 0x0a, 0x0b, 0x52, 0x75, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x1b,
	0x0a, 0x09, 0x65, 0x78, 0x69, 0x74, 0x5f, 0x63, 0x6f, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x08, 0x65, 0x78, 0x69, 0x74, 0x43, 0x6f, 0x64, 0x65, 0x12, 0x3b, 0x0a, 0x0e, 0x72,
	0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x75, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x0d, 0x72, 0x65, 0x73, 0x6f, 0x75,
	0x72, 0x63, 0x65, 0x55, 0x73, 0x61, 0x67, 0x65, 0x32, 0x9f, 0x01, 0x0a, 0x06, 0x52, 0x75, 0x6e,
	0x6e, 0x65, 0x72, 0x12, 0x51, 0x0a, 0x0e, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52, 0x65, 0x61, 0x64,
	0x69, 0x6e, 0x65, 0x73, 0x73, 0x12, 0x27, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72,
	0x6e, 0x2e, 0x72, 0x75, 0x6e, 0x6e, 0x65, 0x72, 0x2e, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52, 0x65,
	0x61, 0x64, 0x69, 0x6e, 0x65, 0x73, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x16,
	0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x12, 0x42, 0x0a, 0x03, 0x52, 0x75, 0x6e, 0x12, 0x1c, 0x2e,
	0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x72, 0x75, 0x6e, 0x6e, 0x65, 0x72,
	0x2e, 0x52, 0x75, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1d, 0x2e, 0x62, 0x75,
	0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x72, 0x75, 0x6e, 0x6e, 0x65, 0x72, 0x2e, 0x52,
	0x75, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x3b, 0x5a, 0x39, 0x67, 0x69,
	0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61,
	0x72, 0x6e, 0x2f, 0x62, 0x62, 0x2d, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2d, 0x65, 0x78, 0x65,
	0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x2f, 0x72, 0x75, 0x6e, 0x6e, 0x65, 0x72, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_pkg_proto_runner_runner_proto_rawDescOnce sync.Once
	file_pkg_proto_runner_runner_proto_rawDescData []byte
)

func file_pkg_proto_runner_runner_proto_rawDescGZIP() []byte {
	file_pkg_proto_runner_runner_proto_rawDescOnce.Do(func() {
		file_pkg_proto_runner_runner_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_pkg_proto_runner_runner_proto_rawDesc), len(file_pkg_proto_runner_runner_proto_rawDesc)))
	})
	return file_pkg_proto_runner_runner_proto_rawDescData
}

var file_pkg_proto_runner_runner_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_pkg_proto_runner_runner_proto_goTypes = []any{
	(*CheckReadinessRequest)(nil), // 0: buildbarn.runner.CheckReadinessRequest
	(*RunRequest)(nil),            // 1: buildbarn.runner.RunRequest
	(*RunResponse)(nil),           // 2: buildbarn.runner.RunResponse
	nil,                           // 3: buildbarn.runner.RunRequest.EnvironmentVariablesEntry
	(*anypb.Any)(nil),             // 4: google.protobuf.Any
	(*emptypb.Empty)(nil),         // 5: google.protobuf.Empty
}
var file_pkg_proto_runner_runner_proto_depIdxs = []int32{
	3, // 0: buildbarn.runner.RunRequest.environment_variables:type_name -> buildbarn.runner.RunRequest.EnvironmentVariablesEntry
	4, // 1: buildbarn.runner.RunResponse.resource_usage:type_name -> google.protobuf.Any
	0, // 2: buildbarn.runner.Runner.CheckReadiness:input_type -> buildbarn.runner.CheckReadinessRequest
	1, // 3: buildbarn.runner.Runner.Run:input_type -> buildbarn.runner.RunRequest
	5, // 4: buildbarn.runner.Runner.CheckReadiness:output_type -> google.protobuf.Empty
	2, // 5: buildbarn.runner.Runner.Run:output_type -> buildbarn.runner.RunResponse
	4, // [4:6] is the sub-list for method output_type
	2, // [2:4] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_pkg_proto_runner_runner_proto_init() }
func file_pkg_proto_runner_runner_proto_init() {
	if File_pkg_proto_runner_runner_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_pkg_proto_runner_runner_proto_rawDesc), len(file_pkg_proto_runner_runner_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pkg_proto_runner_runner_proto_goTypes,
		DependencyIndexes: file_pkg_proto_runner_runner_proto_depIdxs,
		MessageInfos:      file_pkg_proto_runner_runner_proto_msgTypes,
	}.Build()
	File_pkg_proto_runner_runner_proto = out.File
	file_pkg_proto_runner_runner_proto_goTypes = nil
	file_pkg_proto_runner_runner_proto_depIdxs = nil
}
