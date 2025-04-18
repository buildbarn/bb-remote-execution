// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        v5.29.3
// source: pkg/proto/outputpathpersistency/outputpathpersistency.proto

package outputpathpersistency

import (
	v2 "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
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

type RootDirectory struct {
	state               protoimpl.MessageState `protogen:"open.v1"`
	InitialCreationTime *timestamppb.Timestamp `protobuf:"bytes,1,opt,name=initial_creation_time,json=initialCreationTime,proto3" json:"initial_creation_time,omitempty"`
	Contents            *Directory             `protobuf:"bytes,2,opt,name=contents,proto3" json:"contents,omitempty"`
	unknownFields       protoimpl.UnknownFields
	sizeCache           protoimpl.SizeCache
}

func (x *RootDirectory) Reset() {
	*x = RootDirectory{}
	mi := &file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RootDirectory) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RootDirectory) ProtoMessage() {}

func (x *RootDirectory) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RootDirectory.ProtoReflect.Descriptor instead.
func (*RootDirectory) Descriptor() ([]byte, []int) {
	return file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDescGZIP(), []int{0}
}

func (x *RootDirectory) GetInitialCreationTime() *timestamppb.Timestamp {
	if x != nil {
		return x.InitialCreationTime
	}
	return nil
}

func (x *RootDirectory) GetContents() *Directory {
	if x != nil {
		return x.Contents
	}
	return nil
}

type Directory struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Files         []*v2.FileNode         `protobuf:"bytes,1,rep,name=files,proto3" json:"files,omitempty"`
	Directories   []*DirectoryNode       `protobuf:"bytes,2,rep,name=directories,proto3" json:"directories,omitempty"`
	Symlinks      []*v2.SymlinkNode      `protobuf:"bytes,3,rep,name=symlinks,proto3" json:"symlinks,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Directory) Reset() {
	*x = Directory{}
	mi := &file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Directory) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Directory) ProtoMessage() {}

func (x *Directory) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Directory.ProtoReflect.Descriptor instead.
func (*Directory) Descriptor() ([]byte, []int) {
	return file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDescGZIP(), []int{1}
}

func (x *Directory) GetFiles() []*v2.FileNode {
	if x != nil {
		return x.Files
	}
	return nil
}

func (x *Directory) GetDirectories() []*DirectoryNode {
	if x != nil {
		return x.Directories
	}
	return nil
}

func (x *Directory) GetSymlinks() []*v2.SymlinkNode {
	if x != nil {
		return x.Symlinks
	}
	return nil
}

type DirectoryNode struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Name          string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	FileRegion    *FileRegion            `protobuf:"bytes,2,opt,name=file_region,json=fileRegion,proto3" json:"file_region,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *DirectoryNode) Reset() {
	*x = DirectoryNode{}
	mi := &file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DirectoryNode) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DirectoryNode) ProtoMessage() {}

func (x *DirectoryNode) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DirectoryNode.ProtoReflect.Descriptor instead.
func (*DirectoryNode) Descriptor() ([]byte, []int) {
	return file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDescGZIP(), []int{2}
}

func (x *DirectoryNode) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *DirectoryNode) GetFileRegion() *FileRegion {
	if x != nil {
		return x.FileRegion
	}
	return nil
}

type FileRegion struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	OffsetBytes   int64                  `protobuf:"varint,1,opt,name=offset_bytes,json=offsetBytes,proto3" json:"offset_bytes,omitempty"`
	SizeBytes     int32                  `protobuf:"varint,2,opt,name=size_bytes,json=sizeBytes,proto3" json:"size_bytes,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FileRegion) Reset() {
	*x = FileRegion{}
	mi := &file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FileRegion) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FileRegion) ProtoMessage() {}

func (x *FileRegion) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FileRegion.ProtoReflect.Descriptor instead.
func (*FileRegion) Descriptor() ([]byte, []int) {
	return file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDescGZIP(), []int{3}
}

func (x *FileRegion) GetOffsetBytes() int64 {
	if x != nil {
		return x.OffsetBytes
	}
	return 0
}

func (x *FileRegion) GetSizeBytes() int32 {
	if x != nil {
		return x.SizeBytes
	}
	return 0
}

var File_pkg_proto_outputpathpersistency_outputpathpersistency_proto protoreflect.FileDescriptor

const file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDesc = "" +
	"\n" +
	";pkg/proto/outputpathpersistency/outputpathpersistency.proto\x12\x1fbuildbarn.outputpathpersistency\x1a6build/bazel/remote/execution/v2/remote_execution.proto\x1a\x1fgoogle/protobuf/timestamp.proto\"\xa7\x01\n" +
	"\rRootDirectory\x12N\n" +
	"\x15initial_creation_time\x18\x01 \x01(\v2\x1a.google.protobuf.TimestampR\x13initialCreationTime\x12F\n" +
	"\bcontents\x18\x02 \x01(\v2*.buildbarn.outputpathpersistency.DirectoryR\bcontents\"\xe8\x01\n" +
	"\tDirectory\x12?\n" +
	"\x05files\x18\x01 \x03(\v2).build.bazel.remote.execution.v2.FileNodeR\x05files\x12P\n" +
	"\vdirectories\x18\x02 \x03(\v2..buildbarn.outputpathpersistency.DirectoryNodeR\vdirectories\x12H\n" +
	"\bsymlinks\x18\x03 \x03(\v2,.build.bazel.remote.execution.v2.SymlinkNodeR\bsymlinks\"q\n" +
	"\rDirectoryNode\x12\x12\n" +
	"\x04name\x18\x01 \x01(\tR\x04name\x12L\n" +
	"\vfile_region\x18\x02 \x01(\v2+.buildbarn.outputpathpersistency.FileRegionR\n" +
	"fileRegion\"N\n" +
	"\n" +
	"FileRegion\x12!\n" +
	"\foffset_bytes\x18\x01 \x01(\x03R\voffsetBytes\x12\x1d\n" +
	"\n" +
	"size_bytes\x18\x02 \x01(\x05R\tsizeBytesBJZHgithub.com/buildbarn/bb-remote-execution/pkg/proto/outputpathpersistencyb\x06proto3"

var (
	file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDescOnce sync.Once
	file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDescData []byte
)

func file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDescGZIP() []byte {
	file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDescOnce.Do(func() {
		file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDesc), len(file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDesc)))
	})
	return file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDescData
}

var file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_goTypes = []any{
	(*RootDirectory)(nil),         // 0: buildbarn.outputpathpersistency.RootDirectory
	(*Directory)(nil),             // 1: buildbarn.outputpathpersistency.Directory
	(*DirectoryNode)(nil),         // 2: buildbarn.outputpathpersistency.DirectoryNode
	(*FileRegion)(nil),            // 3: buildbarn.outputpathpersistency.FileRegion
	(*timestamppb.Timestamp)(nil), // 4: google.protobuf.Timestamp
	(*v2.FileNode)(nil),           // 5: build.bazel.remote.execution.v2.FileNode
	(*v2.SymlinkNode)(nil),        // 6: build.bazel.remote.execution.v2.SymlinkNode
}
var file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_depIdxs = []int32{
	4, // 0: buildbarn.outputpathpersistency.RootDirectory.initial_creation_time:type_name -> google.protobuf.Timestamp
	1, // 1: buildbarn.outputpathpersistency.RootDirectory.contents:type_name -> buildbarn.outputpathpersistency.Directory
	5, // 2: buildbarn.outputpathpersistency.Directory.files:type_name -> build.bazel.remote.execution.v2.FileNode
	2, // 3: buildbarn.outputpathpersistency.Directory.directories:type_name -> buildbarn.outputpathpersistency.DirectoryNode
	6, // 4: buildbarn.outputpathpersistency.Directory.symlinks:type_name -> build.bazel.remote.execution.v2.SymlinkNode
	3, // 5: buildbarn.outputpathpersistency.DirectoryNode.file_region:type_name -> buildbarn.outputpathpersistency.FileRegion
	6, // [6:6] is the sub-list for method output_type
	6, // [6:6] is the sub-list for method input_type
	6, // [6:6] is the sub-list for extension type_name
	6, // [6:6] is the sub-list for extension extendee
	0, // [0:6] is the sub-list for field type_name
}

func init() { file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_init() }
func file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_init() {
	if File_pkg_proto_outputpathpersistency_outputpathpersistency_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDesc), len(file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_goTypes,
		DependencyIndexes: file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_depIdxs,
		MessageInfos:      file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_msgTypes,
	}.Build()
	File_pkg_proto_outputpathpersistency_outputpathpersistency_proto = out.File
	file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_goTypes = nil
	file_pkg_proto_outputpathpersistency_outputpathpersistency_proto_depIdxs = nil
}
