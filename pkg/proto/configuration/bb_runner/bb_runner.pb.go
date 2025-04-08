// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        v5.29.3
// source: pkg/proto/configuration/bb_runner/bb_runner.proto

package bb_runner

import (
	credentials "github.com/buildbarn/bb-remote-execution/pkg/proto/configuration/credentials"
	global "github.com/buildbarn/bb-storage/pkg/proto/configuration/global"
	grpc "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
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

type ApplicationConfiguration struct {
	state                          protoimpl.MessageState                    `protogen:"open.v1"`
	BuildDirectoryPath             string                                    `protobuf:"bytes,1,opt,name=build_directory_path,json=buildDirectoryPath,proto3" json:"build_directory_path,omitempty"`
	GrpcServers                    []*grpc.ServerConfiguration               `protobuf:"bytes,2,rep,name=grpc_servers,json=grpcServers,proto3" json:"grpc_servers,omitempty"`
	CleanTemporaryDirectories      []string                                  `protobuf:"bytes,3,rep,name=clean_temporary_directories,json=cleanTemporaryDirectories,proto3" json:"clean_temporary_directories,omitempty"`
	Global                         *global.Configuration                     `protobuf:"bytes,4,opt,name=global,proto3" json:"global,omitempty"`
	SetTmpdirEnvironmentVariable   bool                                      `protobuf:"varint,5,opt,name=set_tmpdir_environment_variable,json=setTmpdirEnvironmentVariable,proto3" json:"set_tmpdir_environment_variable,omitempty"`
	TemporaryDirectoryInstaller    *grpc.ClientConfiguration                 `protobuf:"bytes,6,opt,name=temporary_directory_installer,json=temporaryDirectoryInstaller,proto3" json:"temporary_directory_installer,omitempty"`
	ChrootIntoInputRoot            bool                                      `protobuf:"varint,7,opt,name=chroot_into_input_root,json=chrootIntoInputRoot,proto3" json:"chroot_into_input_root,omitempty"`
	CleanProcessTable              bool                                      `protobuf:"varint,8,opt,name=clean_process_table,json=cleanProcessTable,proto3" json:"clean_process_table,omitempty"`
	ReadinessCheckingPathnames     []string                                  `protobuf:"bytes,10,rep,name=readiness_checking_pathnames,json=readinessCheckingPathnames,proto3" json:"readiness_checking_pathnames,omitempty"`
	RunCommandsAs                  *credentials.UNIXCredentialsConfiguration `protobuf:"bytes,11,opt,name=run_commands_as,json=runCommandsAs,proto3" json:"run_commands_as,omitempty"`
	SymlinkTemporaryDirectories    []string                                  `protobuf:"bytes,12,rep,name=symlink_temporary_directories,json=symlinkTemporaryDirectories,proto3" json:"symlink_temporary_directories,omitempty"`
	RunCommandCleaner              []string                                  `protobuf:"bytes,13,rep,name=run_command_cleaner,json=runCommandCleaner,proto3" json:"run_command_cleaner,omitempty"`
	AppleXcodeDeveloperDirectories map[string]string                         `protobuf:"bytes,14,rep,name=apple_xcode_developer_directories,json=appleXcodeDeveloperDirectories,proto3" json:"apple_xcode_developer_directories,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
	unknownFields                  protoimpl.UnknownFields
	sizeCache                      protoimpl.SizeCache
}

func (x *ApplicationConfiguration) Reset() {
	*x = ApplicationConfiguration{}
	mi := &file_pkg_proto_configuration_bb_runner_bb_runner_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ApplicationConfiguration) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ApplicationConfiguration) ProtoMessage() {}

func (x *ApplicationConfiguration) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_proto_configuration_bb_runner_bb_runner_proto_msgTypes[0]
	if x != nil {
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
	return file_pkg_proto_configuration_bb_runner_bb_runner_proto_rawDescGZIP(), []int{0}
}

func (x *ApplicationConfiguration) GetBuildDirectoryPath() string {
	if x != nil {
		return x.BuildDirectoryPath
	}
	return ""
}

func (x *ApplicationConfiguration) GetGrpcServers() []*grpc.ServerConfiguration {
	if x != nil {
		return x.GrpcServers
	}
	return nil
}

func (x *ApplicationConfiguration) GetCleanTemporaryDirectories() []string {
	if x != nil {
		return x.CleanTemporaryDirectories
	}
	return nil
}

func (x *ApplicationConfiguration) GetGlobal() *global.Configuration {
	if x != nil {
		return x.Global
	}
	return nil
}

func (x *ApplicationConfiguration) GetSetTmpdirEnvironmentVariable() bool {
	if x != nil {
		return x.SetTmpdirEnvironmentVariable
	}
	return false
}

func (x *ApplicationConfiguration) GetTemporaryDirectoryInstaller() *grpc.ClientConfiguration {
	if x != nil {
		return x.TemporaryDirectoryInstaller
	}
	return nil
}

func (x *ApplicationConfiguration) GetChrootIntoInputRoot() bool {
	if x != nil {
		return x.ChrootIntoInputRoot
	}
	return false
}

func (x *ApplicationConfiguration) GetCleanProcessTable() bool {
	if x != nil {
		return x.CleanProcessTable
	}
	return false
}

func (x *ApplicationConfiguration) GetReadinessCheckingPathnames() []string {
	if x != nil {
		return x.ReadinessCheckingPathnames
	}
	return nil
}

func (x *ApplicationConfiguration) GetRunCommandsAs() *credentials.UNIXCredentialsConfiguration {
	if x != nil {
		return x.RunCommandsAs
	}
	return nil
}

func (x *ApplicationConfiguration) GetSymlinkTemporaryDirectories() []string {
	if x != nil {
		return x.SymlinkTemporaryDirectories
	}
	return nil
}

func (x *ApplicationConfiguration) GetRunCommandCleaner() []string {
	if x != nil {
		return x.RunCommandCleaner
	}
	return nil
}

func (x *ApplicationConfiguration) GetAppleXcodeDeveloperDirectories() map[string]string {
	if x != nil {
		return x.AppleXcodeDeveloperDirectories
	}
	return nil
}

var File_pkg_proto_configuration_bb_runner_bb_runner_proto protoreflect.FileDescriptor

const file_pkg_proto_configuration_bb_runner_bb_runner_proto_rawDesc = "" +
	"\n" +
	"1pkg/proto/configuration/bb_runner/bb_runner.proto\x12!buildbarn.configuration.bb_runner\x1a5pkg/proto/configuration/credentials/credentials.proto\x1a+pkg/proto/configuration/global/global.proto\x1a'pkg/proto/configuration/grpc/grpc.proto\"\xf3\b\n" +
	"\x18ApplicationConfiguration\x120\n" +
	"\x14build_directory_path\x18\x01 \x01(\tR\x12buildDirectoryPath\x12T\n" +
	"\fgrpc_servers\x18\x02 \x03(\v21.buildbarn.configuration.grpc.ServerConfigurationR\vgrpcServers\x12>\n" +
	"\x1bclean_temporary_directories\x18\x03 \x03(\tR\x19cleanTemporaryDirectories\x12E\n" +
	"\x06global\x18\x04 \x01(\v2-.buildbarn.configuration.global.ConfigurationR\x06global\x12E\n" +
	"\x1fset_tmpdir_environment_variable\x18\x05 \x01(\bR\x1csetTmpdirEnvironmentVariable\x12u\n" +
	"\x1dtemporary_directory_installer\x18\x06 \x01(\v21.buildbarn.configuration.grpc.ClientConfigurationR\x1btemporaryDirectoryInstaller\x123\n" +
	"\x16chroot_into_input_root\x18\a \x01(\bR\x13chrootIntoInputRoot\x12.\n" +
	"\x13clean_process_table\x18\b \x01(\bR\x11cleanProcessTable\x12@\n" +
	"\x1creadiness_checking_pathnames\x18\n" +
	" \x03(\tR\x1areadinessCheckingPathnames\x12i\n" +
	"\x0frun_commands_as\x18\v \x01(\v2A.buildbarn.configuration.credentials.UNIXCredentialsConfigurationR\rrunCommandsAs\x12B\n" +
	"\x1dsymlink_temporary_directories\x18\f \x03(\tR\x1bsymlinkTemporaryDirectories\x12.\n" +
	"\x13run_command_cleaner\x18\r \x03(\tR\x11runCommandCleaner\x12\xaa\x01\n" +
	"!apple_xcode_developer_directories\x18\x0e \x03(\v2_.buildbarn.configuration.bb_runner.ApplicationConfiguration.AppleXcodeDeveloperDirectoriesEntryR\x1eappleXcodeDeveloperDirectories\x1aQ\n" +
	"#AppleXcodeDeveloperDirectoriesEntry\x12\x10\n" +
	"\x03key\x18\x01 \x01(\tR\x03key\x12\x14\n" +
	"\x05value\x18\x02 \x01(\tR\x05value:\x028\x01J\x04\b\t\x10\n" +
	"BLZJgithub.com/buildbarn/bb-remote-execution/pkg/proto/configuration/bb_runnerb\x06proto3"

var (
	file_pkg_proto_configuration_bb_runner_bb_runner_proto_rawDescOnce sync.Once
	file_pkg_proto_configuration_bb_runner_bb_runner_proto_rawDescData []byte
)

func file_pkg_proto_configuration_bb_runner_bb_runner_proto_rawDescGZIP() []byte {
	file_pkg_proto_configuration_bb_runner_bb_runner_proto_rawDescOnce.Do(func() {
		file_pkg_proto_configuration_bb_runner_bb_runner_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_pkg_proto_configuration_bb_runner_bb_runner_proto_rawDesc), len(file_pkg_proto_configuration_bb_runner_bb_runner_proto_rawDesc)))
	})
	return file_pkg_proto_configuration_bb_runner_bb_runner_proto_rawDescData
}

var file_pkg_proto_configuration_bb_runner_bb_runner_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_pkg_proto_configuration_bb_runner_bb_runner_proto_goTypes = []any{
	(*ApplicationConfiguration)(nil),                 // 0: buildbarn.configuration.bb_runner.ApplicationConfiguration
	nil,                                              // 1: buildbarn.configuration.bb_runner.ApplicationConfiguration.AppleXcodeDeveloperDirectoriesEntry
	(*grpc.ServerConfiguration)(nil),                 // 2: buildbarn.configuration.grpc.ServerConfiguration
	(*global.Configuration)(nil),                     // 3: buildbarn.configuration.global.Configuration
	(*grpc.ClientConfiguration)(nil),                 // 4: buildbarn.configuration.grpc.ClientConfiguration
	(*credentials.UNIXCredentialsConfiguration)(nil), // 5: buildbarn.configuration.credentials.UNIXCredentialsConfiguration
}
var file_pkg_proto_configuration_bb_runner_bb_runner_proto_depIdxs = []int32{
	2, // 0: buildbarn.configuration.bb_runner.ApplicationConfiguration.grpc_servers:type_name -> buildbarn.configuration.grpc.ServerConfiguration
	3, // 1: buildbarn.configuration.bb_runner.ApplicationConfiguration.global:type_name -> buildbarn.configuration.global.Configuration
	4, // 2: buildbarn.configuration.bb_runner.ApplicationConfiguration.temporary_directory_installer:type_name -> buildbarn.configuration.grpc.ClientConfiguration
	5, // 3: buildbarn.configuration.bb_runner.ApplicationConfiguration.run_commands_as:type_name -> buildbarn.configuration.credentials.UNIXCredentialsConfiguration
	1, // 4: buildbarn.configuration.bb_runner.ApplicationConfiguration.apple_xcode_developer_directories:type_name -> buildbarn.configuration.bb_runner.ApplicationConfiguration.AppleXcodeDeveloperDirectoriesEntry
	5, // [5:5] is the sub-list for method output_type
	5, // [5:5] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_pkg_proto_configuration_bb_runner_bb_runner_proto_init() }
func file_pkg_proto_configuration_bb_runner_bb_runner_proto_init() {
	if File_pkg_proto_configuration_bb_runner_bb_runner_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_pkg_proto_configuration_bb_runner_bb_runner_proto_rawDesc), len(file_pkg_proto_configuration_bb_runner_bb_runner_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pkg_proto_configuration_bb_runner_bb_runner_proto_goTypes,
		DependencyIndexes: file_pkg_proto_configuration_bb_runner_bb_runner_proto_depIdxs,
		MessageInfos:      file_pkg_proto_configuration_bb_runner_bb_runner_proto_msgTypes,
	}.Build()
	File_pkg_proto_configuration_bb_runner_bb_runner_proto = out.File
	file_pkg_proto_configuration_bb_runner_bb_runner_proto_goTypes = nil
	file_pkg_proto_configuration_bb_runner_bb_runner_proto_depIdxs = nil
}
