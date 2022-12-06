workspace(name = "com_github_buildbarn_bb_remote_execution")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "59d5b42ac315e7eadffa944e86e90c2990110a1c8075f1cd145f487e999d22b3",
    strip_prefix = "rules_docker-0.17.0",
    urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.17.0/rules_docker-v0.17.0.tar.gz"],
)

http_archive(
    name = "io_bazel_rules_go",
    patches = ["//:patches/io_bazel_rules_go/tags-manual.diff"],
    sha256 = "16e9fca53ed6bd4ff4ad76facc9b7b651a89db1689a2877d6fd7b82aa824e366",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.34.0/rules_go-v0.34.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.34.0/rules_go-v0.34.0.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    patches = ["//:patches/bazel_gazelle/dont-flatten-srcs.diff"],
    sha256 = "5982e5463f171da99e3bdaeff8c0f48283a7a5f396ec5282910b9e8a49c0dd7e",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.25.0/bazel-gazelle-v0.25.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.25.0/bazel-gazelle-v0.25.0.tar.gz",
    ],
)

# gazelle:repository_macro go_dependencies.bzl%go_dependencies
load(":go_dependencies.bzl", "go_dependencies")

go_dependencies()

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains(version = "1.19")

load("@io_bazel_rules_docker//repositories:repositories.bzl", container_repositories = "repositories")

container_repositories()

load("@io_bazel_rules_docker//repositories:deps.bzl", container_deps = "deps")

container_deps()

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

container_pull(
    name = "busybox",
    digest = "sha256:a2490cec4484ee6c1068ba3a05f89934010c85242f736280b35343483b2264b6",  # 1.31.1-uclibc
    registry = "docker.io",
    repository = "library/busybox",
)

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")

gazelle_dependencies()

load("@io_bazel_rules_docker//go:image.bzl", _go_image_repos = "repositories")

_go_image_repos()

load("@com_github_bazelbuild_remote_apis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "bazel_remote_apis_imports",
    go = True,
)

http_archive(
    name = "com_google_protobuf",
    sha256 = "ba0650be1b169d24908eeddbe6107f011d8df0da5b1a5a4449a913b10e578faf",
    strip_prefix = "protobuf-3.19.4",
    urls = ["https://github.com/protocolbuffers/protobuf/releases/download/v3.19.4/protobuf-all-3.19.4.tar.gz"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")

http_file(
    name = "com_github_krallin_tini_tini_static_amd64",
    downloaded_file_path = "tini",
    executable = True,
    sha256 = "eadb9d6e2dc960655481d78a92d2c8bc021861045987ccd3e27c7eae5af0cf33",
    urls = ["https://github.com/krallin/tini/releases/download/v0.18.0/tini-static-amd64"],
)

http_archive(
    name = "com_grail_bazel_toolchain",
    canonical_id = "0.7.2",
    sha256 = "f7aa8e59c9d3cafde6edb372d9bd25fb4ee7293ab20b916d867cd0baaa642529",
    strip_prefix = "bazel-toolchain-0.7.2",
    url = "https://github.com/grailbio/bazel-toolchain/archive/0.7.2.tar.gz",
)

load("@com_grail_bazel_toolchain//toolchain:rules.bzl", "llvm_toolchain")

llvm_toolchain(
    name = "llvm_toolchain",
    llvm_version = "14.0.0",
)

http_archive(
    name = "io_bazel_rules_jsonnet",
    sha256 = "d20270872ba8d4c108edecc9581e2bb7f320afab71f8caa2f6394b5202e8a2c3",
    strip_prefix = "rules_jsonnet-0.4.0",
    urls = ["https://github.com/bazelbuild/rules_jsonnet/archive/0.4.0.tar.gz"],
)

load("@io_bazel_rules_jsonnet//jsonnet:jsonnet.bzl", "jsonnet_repositories")

jsonnet_repositories()

load("@google_jsonnet_go//bazel:repositories.bzl", "jsonnet_go_repositories")

jsonnet_go_repositories()

load("@google_jsonnet_go//bazel:deps.bzl", "jsonnet_go_dependencies")

jsonnet_go_dependencies()

http_archive(
    name = "com_github_twbs_bootstrap",
    build_file_content = """exports_files(["css/bootstrap.min.css", "js/bootstrap.min.js"])""",
    sha256 = "395342b2974e3350560e65752d36aab6573652b11cc6cb5ef79a2e5e83ad64b1",
    strip_prefix = "bootstrap-5.1.0-dist",
    urls = ["https://github.com/twbs/bootstrap/releases/download/v5.1.0/bootstrap-5.1.0-dist.zip"],
)

http_archive(
    name = "build_bazel_rules_nodejs",
    sha256 = "8f5f192ba02319254aaf2cdcca00ec12eaafeb979a80a1e946773c520ae0a2c9",
    urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/3.7.0/rules_nodejs-3.7.0.tar.gz"],
)

load("@build_bazel_rules_nodejs//:index.bzl", "node_repositories", "npm_install")

node_repositories(
    node_version = "16.4.1",
)

npm_install(
    name = "npm",
    package_json = "@com_github_buildbarn_bb_storage//:package.json",
    package_lock_json = "@com_github_buildbarn_bb_storage//:package-lock.json",
    symlink_node_modules = False,
)

http_archive(
    name = "rules_antlr",
    patches = ["@com_github_buildbarn_go_xdr//:patches/rules_antlr/antlr-4.10.diff"],
    sha256 = "26e6a83c665cf6c1093b628b3a749071322f0f70305d12ede30909695ed85591",
    strip_prefix = "rules_antlr-0.5.0",
    urls = ["https://github.com/marcohu/rules_antlr/archive/0.5.0.tar.gz"],
)

load("@rules_antlr//antlr:repositories.bzl", "rules_antlr_dependencies")

rules_antlr_dependencies("4.10")

http_archive(
    name = "io_opentelemetry_proto",
    build_file_content = """
proto_library(
    name = "common_proto",
    srcs = ["opentelemetry/proto/common/v1/common.proto"],
    visibility = ["//visibility:public"],
)
""",
    sha256 = "464bc2b348e674a1a03142e403cbccb01be8655b6de0f8bfe733ea31fcd421be",
    strip_prefix = "opentelemetry-proto-0.19.0",
    urls = ["https://github.com/open-telemetry/opentelemetry-proto/archive/refs/tags/v0.19.0.tar.gz"],
)
