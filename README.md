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
For `bb_runner`, it provides two images: `bb_runner_bare` and `bb_runner_installer`.
`bb_runner_bare` has no userland/linux install, it just has the `bb_runner`
executable. Typically the actions that will run on a runner do expect some
userland to be installed.

It would be nice if you could just use any image of your choosing as the image
that your build actions will run on. Like using
[Ubuntu 16.04 image](https://console.cloud.google.com/marketplace/details/google/rbe-ubuntu16-04),
to take advantage of the fact that bazel project provides [ready-to-use toolchain 
definitions](https://github.com/bazelbuild/bazel-toolchains) for them.

What makes that tricky is that that image will not have `bb_runner` installed.
This is where `bb_runner_installer` image comes in. It doesn't actually
install anything, but it provides the `bb_runner` executable through its
filesystem. You have to configure your orchestration of choice to mount this
filesystem from `bb_runner_installer` into the image of your choice that you
want to run on. This way you can use a vanilla image and just run the bb_runner
executable from Buildbarn's provided container. There's a few tricks to check
if the volume is already available, you can see an example of how to do this
in the [docker-compose example](https://github.com/buildbarn/bb-deployments/blob/e404c1a519355353d0e2cdfd447126fe07095594/docker-compose/docker-compose.yml#L89).

Please refer to [the Buildbarn Deployments repository](https://github.com/buildbarn/bb-deployments)
for examples on how to set up these tools.
