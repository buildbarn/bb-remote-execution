workspace(name = "com_github_buildbarn_bb_remote_execution")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_pkg",
    sha256 = "8f9ee2dc10c1ae514ee599a8b42ed99fa262b757058f65ad3c384289ff70c4b8",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.9.1/rules_pkg-0.9.1.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.9.1/rules_pkg-0.9.1.tar.gz",
    ],
)

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "b1e80761a8a8243d03ebca8845e9cc1ba6c82ce7c5179ce2b295cd36f7e394bf",
    urls = ["https://github.com/bazelbuild/rules_docker/releases/download/v0.25.0/rules_docker-v0.25.0.tar.gz"],
)

http_archive(
    name = "io_bazel_rules_go",
    patches = ["//:patches/io_bazel_rules_go/tags-manual.diff"],
    sha256 = "91585017debb61982f7054c9688857a2ad1fd823fc3f9cb05048b0025c47d023",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.42.0/rules_go-v0.42.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.42.0/rules_go-v0.42.0.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    patches = ["//:patches/bazel_gazelle/dont-flatten-srcs.diff"],
    sha256 = "b7387f72efb59f876e4daae42f1d3912d0d45563eac7cb23d1de0b094ab588cf",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.34.0/bazel-gazelle-v0.34.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.34.0/bazel-gazelle-v0.34.0.tar.gz",
    ],
)

load("@bazel_gazelle//:deps.bzl", "go_repository")

# Override the version of gomock to one that includes support for
# generating mocks for function types. We can't do this through go.mod,
# as it causes almost all of our package dependencies to be downgraded.
go_repository(
    name = "com_github_golang_mock",
    importpath = "github.com/golang/mock",
    patches = [
        "@com_github_buildbarn_bb_storage//:patches/com_github_golang_mock/mocks-for-funcs.diff",
        "//:patches/com_github_golang_mock/generics.diff",
    ],
    replace = "github.com/golang/mock",
    sum = "h1:DxRM2MRFDKF8JGaT1ZSsCZ9KxoOki+rrOoB011jIEDc=",
    version = "v1.6.1-0.20220512030613-73266f9366fc",
)

# gazelle:repository_macro go_dependencies.bzl%go_dependencies
load(":go_dependencies.bzl", "go_dependencies")

go_dependencies()

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains(version = "1.21.5")

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
    integrity = "sha256-j/URpk/EbueS0/5JpaG8rW99xQ37uloosOW5ecF/mHE=",
    strip_prefix = "protobuf-25.2",
    urls = ["https://github.com/protocolbuffers/protobuf/releases/download/v25.2/protobuf-25.2.tar.gz"],
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "googleapis",
    sha256 = "361e26593b881e70286a28065859c941e25b96f9c48ba91127293d0a881152d6",
    strip_prefix = "googleapis-a3770599794a8d319286df96f03343b6cd0e7f4f",
    urls = ["https://github.com/googleapis/googleapis/archive/a3770599794a8d319286df96f03343b6cd0e7f4f.zip"],
)

load("@googleapis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "com_google_googleapis_imports",
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")

http_file(
    name = "com_github_krallin_tini_tini_static_amd64",
    downloaded_file_path = "tini",
    executable = True,
    sha256 = "eadb9d6e2dc960655481d78a92d2c8bc021861045987ccd3e27c7eae5af0cf33",
    urls = ["https://github.com/krallin/tini/releases/download/v0.18.0/tini-static-amd64"],
)

http_archive(
    name = "toolchains_llvm",
    canonical_id = "0.10.3",
    sha256 = "b7cd301ef7b0ece28d20d3e778697a5e3b81828393150bed04838c0c52963a01",
    strip_prefix = "toolchains_llvm-0.10.3",
    url = "https://github.com/grailbio/bazel-toolchain/releases/download/0.10.3/toolchains_llvm-0.10.3.tar.gz",
)

load("@toolchains_llvm//toolchain:rules.bzl", "llvm_toolchain")

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
    name = "aspect_rules_js",
    sha256 = "a949d56fed8fa0a8dd82a0a660acc949253a05b2b0c52a07e4034e27f11218f6",
    strip_prefix = "rules_js-1.33.1",
    url = "https://github.com/aspect-build/rules_js/releases/download/v1.33.1/rules_js-v1.33.1.tar.gz",
)

load("@aspect_rules_js//js:repositories.bzl", "rules_js_dependencies")

rules_js_dependencies()

load("@rules_nodejs//nodejs:repositories.bzl", "DEFAULT_NODE_VERSION", "nodejs_register_toolchains")

nodejs_register_toolchains(
    name = "nodejs",
    node_version = DEFAULT_NODE_VERSION,
)

load("@aspect_rules_js//npm:npm_import.bzl", "npm_translate_lock")

npm_translate_lock(
    name = "npm",
    pnpm_lock = "@com_github_buildbarn_bb_storage//:pnpm-lock.yaml",
)

load("@npm//:repositories.bzl", "npm_repositories")

npm_repositories()

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
