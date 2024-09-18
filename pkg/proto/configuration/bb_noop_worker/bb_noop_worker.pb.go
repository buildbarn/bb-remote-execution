// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        v5.28.1
// source: pkg/proto/configuration/bb_noop_worker/bb_noop_worker.proto

package bb_noop_worker

import (
	v2 "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	blobstore "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	global "github.com/buildbarn/bb-storage/pkg/proto/configuration/global"
	grpc "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ApplicationConfiguration struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Global                    *global.Configuration              `protobuf:"bytes,1,opt,name=global,proto3" json:"global,omitempty"`
	BrowserUrl                string                             `protobuf:"bytes,2,opt,name=browser_url,json=browserUrl,proto3" json:"browser_url,omitempty"`
	Scheduler                 *grpc.ClientConfiguration          `protobuf:"bytes,3,opt,name=scheduler,proto3" json:"scheduler,omitempty"`
	InstanceNamePrefix        string                             `protobuf:"bytes,4,opt,name=instance_name_prefix,json=instanceNamePrefix,proto3" json:"instance_name_prefix,omitempty"`
	Platform                  *v2.Platform                       `protobuf:"bytes,5,opt,name=platform,proto3" json:"platform,omitempty"`
	WorkerId                  map[string]string                  `protobuf:"bytes,6,rep,name=worker_id,json=workerId,proto3" json:"worker_id,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	ContentAddressableStorage *blobstore.BlobAccessConfiguration `protobuf:"bytes,7,opt,name=content_addressable_storage,json=contentAddressableStorage,proto3" json:"content_addressable_storage,omitempty"`
	MaximumMessageSizeBytes   int64                              `protobuf:"varint,8,opt,name=maximum_message_size_bytes,json=maximumMessageSizeBytes,proto3" json:"maximum_message_size_bytes,omitempty"`
}

func (x *ApplicationConfiguration) Reset() {
	*x = ApplicationConfiguration{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ApplicationConfiguration) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ApplicationConfiguration) ProtoMessage() {}

func (x *ApplicationConfiguration) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ApplicationConfiguration.ProtoReflect.Descriptor instead.
func (*ApplicationConfiguration) Descriptor() ([]byte, []int) {
	return file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_rawDescGZIP(), []int{0}
}

func (x *ApplicationConfiguration) GetGlobal() *global.Configuration {
	if x != nil {
		return x.Global
	}
	return nil
}

func (x *ApplicationConfiguration) GetBrowserUrl() string {
	if x != nil {
		return x.BrowserUrl
	}
	return ""
}

func (x *ApplicationConfiguration) GetScheduler() *grpc.ClientConfiguration {
	if x != nil {
		return x.Scheduler
	}
	return nil
}

func (x *ApplicationConfiguration) GetInstanceNamePrefix() string {
	if x != nil {
		return x.InstanceNamePrefix
	}
	return ""
}

func (x *ApplicationConfiguration) GetPlatform() *v2.Platform {
	if x != nil {
		return x.Platform
	}
	return nil
}

func (x *ApplicationConfiguration) GetWorkerId() map[string]string {
	if x != nil {
		return x.WorkerId
	}
	return nil
}

func (x *ApplicationConfiguration) GetContentAddressableStorage() *blobstore.BlobAccessConfiguration {
	if x != nil {
		return x.ContentAddressableStorage
	}
	return nil
}

func (x *ApplicationConfiguration) GetMaximumMessageSizeBytes() int64 {
	if x != nil {
		return x.MaximumMessageSizeBytes
	}
	return 0
}

var File_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto protoreflect.FileDescriptor

var file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_rawDesc = []byte{
	0x0a, 0x3b, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x66,
	0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x62, 0x62, 0x5f, 0x6e, 0x6f, 0x6f,
	0x70, 0x5f, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2f, 0x62, 0x62, 0x5f, 0x6e, 0x6f, 0x6f, 0x70,
	0x5f, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x26, 0x62,
	0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x62, 0x62, 0x5f, 0x6e, 0x6f, 0x6f, 0x70, 0x5f, 0x77,
	0x6f, 0x72, 0x6b, 0x65, 0x72, 0x1a, 0x36, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x2f, 0x62, 0x61, 0x7a,
	0x65, 0x6c, 0x2f, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2f, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74,
	0x69, 0x6f, 0x6e, 0x2f, 0x76, 0x32, 0x2f, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x5f, 0x65, 0x78,
	0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x31, 0x70,
	0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x62, 0x6c, 0x6f, 0x62, 0x73, 0x74, 0x6f, 0x72, 0x65,
	0x2f, 0x62, 0x6c, 0x6f, 0x62, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x1a, 0x2b, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x66,
	0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c,
	0x2f, 0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x27, 0x70,
	0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x67, 0x72, 0x70, 0x63, 0x2f, 0x67, 0x72, 0x70, 0x63,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xaf, 0x05, 0x0a, 0x18, 0x41, 0x70, 0x70, 0x6c, 0x69,
	0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x12, 0x45, 0x0a, 0x06, 0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x2d, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e,
	0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x67, 0x6c,
	0x6f, 0x62, 0x61, 0x6c, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x52, 0x06, 0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x12, 0x1f, 0x0a, 0x0b, 0x62, 0x72,
	0x6f, 0x77, 0x73, 0x65, 0x72, 0x5f, 0x75, 0x72, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0a, 0x62, 0x72, 0x6f, 0x77, 0x73, 0x65, 0x72, 0x55, 0x72, 0x6c, 0x12, 0x4f, 0x0a, 0x09, 0x73,
	0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x31,
	0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x69,
	0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x43, 0x6c,
	0x69, 0x65, 0x6e, 0x74, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x52, 0x09, 0x73, 0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x72, 0x12, 0x30, 0x0a, 0x14,
	0x69, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x5f, 0x70, 0x72,
	0x65, 0x66, 0x69, 0x78, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x12, 0x69, 0x6e, 0x73, 0x74,
	0x61, 0x6e, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x50, 0x72, 0x65, 0x66, 0x69, 0x78, 0x12, 0x45,
	0x0a, 0x08, 0x70, 0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x29, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x2e, 0x62, 0x61, 0x7a, 0x65, 0x6c, 0x2e, 0x72,
	0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x2e,
	0x76, 0x32, 0x2e, 0x50, 0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x52, 0x08, 0x70, 0x6c, 0x61,
	0x74, 0x66, 0x6f, 0x72, 0x6d, 0x12, 0x6b, 0x0a, 0x09, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x5f,
	0x69, 0x64, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x4e, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64,
	0x62, 0x61, 0x72, 0x6e, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x2e, 0x62, 0x62, 0x5f, 0x6e, 0x6f, 0x6f, 0x70, 0x5f, 0x77, 0x6f, 0x72, 0x6b, 0x65,
	0x72, 0x2e, 0x41, 0x70, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x43, 0x6f, 0x6e,
	0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x57, 0x6f, 0x72, 0x6b, 0x65,
	0x72, 0x49, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x08, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72,
	0x49, 0x64, 0x12, 0x7a, 0x0a, 0x1b, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x5f, 0x61, 0x64,
	0x64, 0x72, 0x65, 0x73, 0x73, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67,
	0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x3a, 0x2e, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62,
	0x61, 0x72, 0x6e, 0x2e, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x2e, 0x62, 0x6c, 0x6f, 0x62, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x42, 0x6c, 0x6f, 0x62,
	0x41, 0x63, 0x63, 0x65, 0x73, 0x73, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x52, 0x19, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x41, 0x64, 0x64, 0x72,
	0x65, 0x73, 0x73, 0x61, 0x62, 0x6c, 0x65, 0x53, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x12, 0x3b,
	0x0a, 0x1a, 0x6d, 0x61, 0x78, 0x69, 0x6d, 0x75, 0x6d, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x5f, 0x62, 0x79, 0x74, 0x65, 0x73, 0x18, 0x08, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x17, 0x6d, 0x61, 0x78, 0x69, 0x6d, 0x75, 0x6d, 0x4d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x53, 0x69, 0x7a, 0x65, 0x42, 0x79, 0x74, 0x65, 0x73, 0x1a, 0x3b, 0x0a, 0x0d, 0x57,
	0x6f, 0x72, 0x6b, 0x65, 0x72, 0x49, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03,
	0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14,
	0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x42, 0x51, 0x5a, 0x4f, 0x67, 0x69, 0x74, 0x68,
	0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x62, 0x61, 0x72, 0x6e,
	0x2f, 0x62, 0x62, 0x2d, 0x72, 0x65, 0x6d, 0x6f, 0x74, 0x65, 0x2d, 0x65, 0x78, 0x65, 0x63, 0x75,
	0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63,
	0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x62, 0x62, 0x5f,
	0x6e, 0x6f, 0x6f, 0x70, 0x5f, 0x77, 0x6f, 0x72, 0x6b, 0x65, 0x72, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_rawDescOnce sync.Once
	file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_rawDescData = file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_rawDesc
)

func file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_rawDescGZIP() []byte {
	file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_rawDescOnce.Do(func() {
		file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_rawDescData)
	})
	return file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_rawDescData
}

var file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_goTypes = []interface{}{
	(*ApplicationConfiguration)(nil),          // 0: buildbarn.configuration.bb_noop_worker.ApplicationConfiguration
	nil,                                       // 1: buildbarn.configuration.bb_noop_worker.ApplicationConfiguration.WorkerIdEntry
	(*global.Configuration)(nil),              // 2: buildbarn.configuration.global.Configuration
	(*grpc.ClientConfiguration)(nil),          // 3: buildbarn.configuration.grpc.ClientConfiguration
	(*v2.Platform)(nil),                       // 4: build.bazel.remote.execution.v2.Platform
	(*blobstore.BlobAccessConfiguration)(nil), // 5: buildbarn.configuration.blobstore.BlobAccessConfiguration
}
var file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_depIdxs = []int32{
	2, // 0: buildbarn.configuration.bb_noop_worker.ApplicationConfiguration.global:type_name -> buildbarn.configuration.global.Configuration
	3, // 1: buildbarn.configuration.bb_noop_worker.ApplicationConfiguration.scheduler:type_name -> buildbarn.configuration.grpc.ClientConfiguration
	4, // 2: buildbarn.configuration.bb_noop_worker.ApplicationConfiguration.platform:type_name -> build.bazel.remote.execution.v2.Platform
	1, // 3: buildbarn.configuration.bb_noop_worker.ApplicationConfiguration.worker_id:type_name -> buildbarn.configuration.bb_noop_worker.ApplicationConfiguration.WorkerIdEntry
	5, // 4: buildbarn.configuration.bb_noop_worker.ApplicationConfiguration.content_addressable_storage:type_name -> buildbarn.configuration.blobstore.BlobAccessConfiguration
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_init() }
func file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_init() {
	if File_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ApplicationConfiguration); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_goTypes,
		DependencyIndexes: file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_depIdxs,
		MessageInfos:      file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_msgTypes,
	}.Build()
	File_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto = out.File
	file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_rawDesc = nil
	file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_goTypes = nil
	file_pkg_proto_configuration_bb_noop_worker_bb_noop_worker_proto_depIdxs = nil
}
