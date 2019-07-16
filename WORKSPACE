workspace(name = "com_github_buildbarn_bb_remote_execution")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "bazel_gomock",
    sha256 = "788038400b9f24c079046769eb93e22a3906fc7c16eb3c7beea099e2e46f220f",
    strip_prefix = "bazel_gomock-d5cc12f6eca65d5b6b99f88b5551c37a3a47a65b",
    urls = ["https://github.com/mickael-carl/bazel_gomock/archive/d5cc12f6eca65d5b6b99f88b5551c37a3a47a65b.tar.gz"],
)

http_archive(
    name = "bazel_toolchains",
    sha256 = "28cb3666da80fbc62d4c46814f5468dd5d0b59f9064c0b933eee3140d706d330",
    strip_prefix = "bazel-toolchains-0.27.1",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-toolchains/archive/0.27.1.tar.gz",
        "https://github.com/bazelbuild/bazel-toolchains/archive/0.27.1.tar.gz",
    ],
)

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "87fc6a2b128147a0a3039a2fd0b53cc1f2ed5adb8716f50756544a572999ae9a",
    strip_prefix = "rules_docker-0.8.1",
    urls = ["https://github.com/bazelbuild/rules_docker/archive/v0.8.1.tar.gz"],
)

http_archive(
    name = "io_bazel_rules_go",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/rules_go/releases/download/0.18.6/rules_go-0.18.6.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/0.18.6/rules_go-0.18.6.tar.gz",
    ],
    sha256 = "f04d2373bcaf8aa09bccb08a98a57e721306c8f6043a2a0ee610fd6853dcde3d",
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "3c681998538231a2d24d0c07ed5a7658cb72bfb5fd4bf9911157c0e9ac6a2687",
    urls = ["https://github.com/bazelbuild/bazel-gazelle/releases/download/0.17.0/bazel-gazelle-0.17.0.tar.gz"],
)

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "com_github_buildbarn_bb_storage",
    commit = "e8c6266e6632d88220e12ea62dc4b6931372ce55",
    remote = "https://github.com/buildbarn/bb-storage.git",
)

load("@io_bazel_rules_docker//repositories:repositories.bzl", container_repositories = "repositories")

container_repositories()

load("@io_bazel_rules_docker//container:container.bzl", "container_pull")

container_pull(
    name = "rbe_ubuntu16_04_base",
    digest = "sha256:94d7d8552902d228c32c8c148cc13f0effc2b4837757a6e95b73fdc5c5e4b07b",
    registry = "launcher.gcr.io",
    repository = "google/rbe-ubuntu16-04",
)

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")

gazelle_dependencies()

load("@com_github_buildbarn_bb_storage//:go_dependencies.bzl", "bb_storage_go_dependencies")

bb_storage_go_dependencies()
