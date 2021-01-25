*注：本文档是从2021-01-25的英语版本翻译而来的*

# Buildbarn 远端执行

本仓库里的工具可以与[Buildbarn storage daemon](https://github.com/buildbarn/bb-storage) 共同组成一个[构建集群](https://en.wikipedia.org/wiki/Compile_farm) 来为 [Bazel](https://bazel.build/), [BuildStream](https://wiki.gnome.org/Projects/BuildStream) and [recc](https://gitlab.com/bloomberg/recc)等工具提供远端执行服务。

本仓库提供3个应用程序：

- `bb_scheduler`: 这个服务会接收[`bb_storage`](https://github.com/buildbarn/bb-storage)的请求，并将需要运行的Build Action入队
- `bb_worker`: 这个服务从`bb_scheduler` 获取Build Action并且编排他们的执行。其中包含了下载Build Action的输入文件和上传Build Action的输出文件。
- `bb_runner`: 这个服务执行Build Action所关联的命令。

大多数的部署会跑一个`bb_scheduler`的实例以及大量成对的`bb_worker`/`bb_runner`进程。老版本的Buildbarn将`bb_worker` 和`bb_runner`的功能集成在一个单进程中。这些进程组合在一起完成以下功能：

- 权限隔离： 防止Build Action对输入文件进行改写，使得`bb_worker`可以在Build Action间缓存这些文件，并通过硬链接暴露给Build Action
- 执行可插拔：`bb_worker`通过 [gRPC协议](https://github.com/buildbarn/bb-remote-execution/blob/master/pkg/proto/runner/runner.proto)与`bb_runner`通信。例如一个集成了用[QEMU](https://www.qemu.org/)执行Build Action的Runner进程的云服务
- 解决[并发竞争的问题](https://github.com/golang/go/issues/22315) ： 有效防止多线程的进程向磁盘写可执行程序并且执行他们。Buildbarn提供的解决方案是 `bb_worker`将可执行程序写到磁盘，`bb_runner` 来执行他们。



本仓库为每个组件提供了容器镜像。对于`bb_runner`来说，提供了一个不包含用户区的容器镜像，也提供了在容器启动时安装`bb_runner`的方案。前者对于[BuildStream](https://buildstream.build/)来说已经足够了，而后者可以用来与Google RBE's [Ubuntu 16.04 容器镜像](https://console.cloud.google.com/marketplace/details/google/rbe-ubuntu16-04)一同组合使用。用 Ubuntu 16.04 容器镜像的优点是Bazel提供了拿来即用的[工具链定义](https://github.com/bazelbuild/bazel-toolchains)。

请参考[Buildbarn Deployments 仓库](https://github.com/buildbarn/bb-deployments) 中关于如何设置这些工具的例子。

