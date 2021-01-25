# Buildbarn Remote Execution [![Build status](https://github.com/buildbarn/bb-remote-execution/workflows/master/badge.svg)](https://github.com/buildbarn/bb-remote-execution/actions) [![PkgGoDev](https://pkg.go.dev/badge/github.com/buildbarn/bb-remote-execution)](https://pkg.go.dev/github.com/buildbarn/bb-remote-execution) [![Go Report Card](https://goreportcard.com/badge/github.com/buildbarn/bb-remote-execution)](https://goreportcard.com/report/github.com/buildbarn/bb-remote-execution)

Translations: [Chinese](https://github.com/buildbarn/bb-remote-execution/blob/master/doc/zh_CN/README.md)

This repository provides tools that can be used in combination with
[the Buildbarn storage daemon](https://github.com/buildbarn/bb-storage)
to add support for remote execution, allowing you to create
[a build farm](https://en.wikipedia.org/wiki/Compile_farm) that can be
called into using tools such as [Bazel](https://bazel.build/),
[BuildStream](https://wiki.gnome.org/Projects/BuildStream) and
[recc](https://gitlab.com/bloomberg/recc).

This repository provides three programs:

- `bb_scheduler`: A service that receives requests from
  [`bb_storage`](https://github.com/buildbarn/bb-storage) to queue build
  actions that need to be run.
- `bb_worker`: A service that requests build actions from `bb_scheduler`
  and orchestrates their execution. This includes downloading the build
  action's input files and uploading its output files.
- `bb_runner`: A service that executes the command associated with the
  build action.

Most setups will run a single instance of `bb_scheduler` and a large
number of pairs of `bb_worker`/`bb_runner` processes. Older versions of
Buildbarn integrated the functionality of `bb_worker` and `bb_runner`
into a single process. These processes were decomposed to accomplish the
following:

- To make it possible to use privilege separation. Privilege separation
  is used to prevent build actions from overwriting input files. This
  allows `bb_worker` to cache these files across build actions,
  exposing it to the build action through hardlinking.
- To make execution pluggable. `bb_worker` communicates with `bb_runner`
  using [a simple gRPC-based protocol](https://github.com/buildbarn/bb-remote-execution/blob/master/pkg/proto/runner/runner.proto).
  One could, for example, implement a custom runner process that
  executes build actions using [QEMU user-mode emulation](https://www.qemu.org/).
- To work around [a race condition](https://github.com/golang/go/issues/22315)
  that effectively prevents multi-threaded processes from writing
  executables to disk and spawning them. Through this decomposition,
  `bb_worker` writes executables to disk, while `bb_runner` spawns them.

This repository provides container images for each of these components.
For `bb_runner`, it provides one image without a userland, and one that
installs the `bb_runner` into another container on startup. The former
is sufficient for [BuildStream](https://buildstream.build), while the
latter can, for example, be used in combination with Google RBE's
[Ubuntu 16.04 image](https://console.cloud.google.com/marketplace/details/google/rbe-ubuntu16-04).
The advantage of using the Ubuntu 16.04 image is that the Bazel project
provides [ready-to-use toolchain definitions](https://github.com/bazelbuild/bazel-toolchains)
for them.

Please refer to [the Buildbarn Deployments repository](https://github.com/buildbarn/bb-deployments)
for examples on how to set up these tools.
