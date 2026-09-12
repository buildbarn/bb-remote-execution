# Persistent workers

This document describes how Buildbarn implements support for
[Bazel's persistent worker protocol](https://bazel.build/remote/persistent),
covering both the design and the way the implementation is tested. It
addresses [issue #112](https://github.com/buildbarn/bb-remote-execution/issues/112).

## Table of contents

- [Background](#background)
- [Design](#design)
  - [Overview](#overview)
  - [`bb_scheduler`: routing](#bb_scheduler-routing)
  - [`bb_worker`: extracting the request](#bb_worker-extracting-the-request)
  - [`bb_runner`: running the tool](#bb_runner-running-the-tool)
  - [The symlink farm](#the-symlink-farm)
  - [The tool's own inputs](#the-tools-own-inputs)
  - [Harvesting outputs](#harvesting-outputs)
  - [Worker pool management](#worker-pool-management)
  - [Interaction with other features](#interaction-with-other-features)
- [Configuration](#configuration)
- [Observability](#observability)
- [Testing plan](#testing-plan)
- [Known limitations](#known-limitations)

The client/server contract this implementation follows is written down
separately, in
[Remote persistent workers: a specification](remote_persistent_workers_specification.md).
That document is about what any remote execution service has to do; this
one is about how Buildbarn does it.

## Background

Many build tools have a high startup cost. A JVM based compiler such as
`javac` needs to start a virtual machine, load and JIT compile its own
class files and warm up its caches before it can compile a single source
file. Paying that cost for every build action is wasteful.

Bazel solves this locally by keeping tool processes alive in between
actions. Instead of running the tool once per action, it launches the
tool once and then exchanges `WorkRequest`/`WorkResponse` messages with
it over its standard input and output. This is called the
[persistent worker protocol](https://bazel.build/remote/persistent).

The protocol is deliberately simple:

- The tool is launched with the *worker startup arguments*: every
  command line argument that is not a "flag file". It is expected to
  remain alive until its standard input is closed.
- For every action, a `WorkRequest` is written to the tool's standard
  input. It contains the remaining arguments (the contents of the flag
  files) and the list of input files with their digests.
- The tool replies with a `WorkResponse` containing an exit code and the
  output that would otherwise have been written to standard output and
  error.

Two encodings exist. By default messages are length delimited Protobuf
messages (Java's `writeDelimitedTo()`). Tools that declare the
`requires-worker-protocol=json` execution requirement instead use
newline delimited JSON, using the canonical Protobuf JSON mapping.

When `--experimental_remote_mark_tool_inputs` is passed, Bazel makes the
same information available to a *remote* execution service:

- A `persistentWorkerKey` REv2 platform property is added to the action.
  Its value is a hexadecimal fingerprint over the tool's own input
  files, the worker startup arguments and the environment. Two actions
  that share this key may be executed by the same tool process.
- Tool inputs are marked with a `bazel_tool_input` node property, so
  that a remote worker can tell the tool apart from the action's
  sources.

Everything else — deciding whether to reuse a process, keeping it alive,
tracking which inputs changed — is up to the remote execution service.
That is what this implementation adds.

## Design

### Overview

Persistent worker support touches all three Buildbarn daemons, but the
bulk of the work happens in `bb_runner`:

```
                 Action with 'persistentWorkerKey' platform property
                                      |
                                      v
+---------------------------------------------------------------------+
| bb_scheduler                                                        |
|   platform.NewStrippingKeyExtractor()                               |
|     -> removes 'persistentWorkerKey' from the *platform key*, so    |
|        that the action still matches a regular worker.              |
|   invocation.NewPlatformPropertyKeyExtractor()                      |
|     -> groups actions by 'persistentWorkerKey', so that worker      |
|        invocation stickiness keeps them on the same worker.         |
+---------------------------------------------------------------------+
                                      |
                                      v
+---------------------------------------------------------------------+
| bb_worker                                                           |
|   builder.PersistentWorkerExtractor                                 |
|     -> reads the platform properties,                               |
|     -> walks the input root Merkle tree to build WorkRequest.inputs,|
|     -> attaches a PersistentWorker message to the RunRequest.       |
+---------------------------------------------------------------------+
                                      | gRPC (runner.proto)
                                      v
+---------------------------------------------------------------------+
| bb_runner                                                           |
|   runner.PersistentWorkerRunner                                     |
|     -> splits argv into startup arguments and flag files,           |
|     -> expands '@flagfile' arguments,                               |
|     -> acquires a process from runner.PersistentWorkerPool,         |
|     -> repopulates its execution root with a symlink farm,          |
|     -> exchanges WorkRequest/WorkResponse,                          |
|     -> moves the outputs back into the input root,                  |
|     -> writes WorkResponse.output to the action's stderr file.      |
+---------------------------------------------------------------------+
```

The division of labour between `bb_worker` and `bb_runner` follows the
existing split of responsibilities. `bb_worker` has the Merkle tree of
the input root in hand and already knows every file's digest, so it can
produce `WorkRequest.inputs` without hashing anything. `bb_runner` owns
process creation and privilege separation, so it owns the pool.

### `bb_scheduler`: routing

`InMemoryBuildQueue` places every action in a queue that is keyed by
`platform.Key`, which is the REv2 instance name prefix plus the
JSON serialisation of the `Platform` message. Workers announce the
platform they provide, and an action is only ever assigned to a worker
whose key is *exactly* equal.

`persistentWorkerKey` is a fingerprint of a tool, not a property of an
execution environment. A worker cannot know its value ahead of time, so
if it were part of the platform key, no worker would ever match and
every persistent worker action would stay queued until it timed out.

`platform.NewStrippingKeyExtractor()` (see
`pkg/scheduler/platform/stripping_key_extractor.go`) wraps another
`platform.KeyExtractor` and removes a configured list of property names
from a *copy* of the action before the key is computed. The action that
is handed to the worker is left untouched, so `bb_worker` can still read
the property.

Stripping alone is enough to make persistent workers work. To make them
*effective*, actions that share a key should preferably run on the same
worker, so that a tool process that is already warm gets reused.
`InMemoryBuildQueue` already implements this through
`worker_invocation_stickiness_limits`, which keeps a worker on the same
invocation for a configurable amount of time. All that was missing was
a way to derive an invocation from the tool:
`invocation.NewPlatformPropertyKeyExtractor()` (see
`pkg/scheduler/invocation/platform_property_key_extractor.go`) returns
an invocation key based on the value of a single platform property.
Actions that don't carry the property are grouped together as if it had
an empty value.

To support this, `invocation.KeyExtractor.ExtractKey()` now also
receives the digest function and the `Action` message, matching the
signature of `platform.KeyExtractor.ExtractKey()`. The existing
extractors ignore the new parameters.

### `bb_worker`: extracting the request

`builder.PersistentWorkerExtractor` (see
`pkg/builder/persistent_worker_extractor.go`) runs just before the
action transitions into the `Running` state. It:

1. Reads `persistentWorkerKey` from `Action.platform`. If it is absent,
   nothing happens and the action is executed as a regular process.
   Platform properties are read from the `Action` message only, which is
   consistent with `platform.ActionKeyExtractor` and REv2.2 semantics.
2. Reads the optional `persistentWorkerProtocol` property, which selects
   between the `proto` (default) and `json` encodings. Bazel sets it
   from the tool's `requires-worker-protocol` execution requirement,
   whose only valid values are `json` and `proto`; a tool that does not
   declare it uses `proto`.
3. Recursively walks the input root through `cas.DirectoryFetcher`,
   producing one `blaze.worker.Input` per file. `Input.path` is relative
   to the input root and `Input.digest` is the ASCII hexadecimal
   representation of the file's REv2 digest hash, which is what Bazel
   sends to local workers.
4. Attaches the result to `RunRequest.persistent_worker`.

Walking the input root costs one `GetDirectory()` call per directory.
Because `bb_worker` uses a `CachingDirectoryFetcher`, these are usually
served from memory, and the input root was just materialised anyway. To
bound the cost for pathological actions, `maximum_input_file_count`
caps the number of inputs. Actions that exceed it fall back to regular
process execution rather than being sent an incomplete input list — an
incomplete list would cause tools that do incremental compilation to
produce *wrong* results, whereas running the tool one-shot is merely
slower. Tools that support the worker protocol are always able to run
one-shot, because Bazel itself falls back to it when
`--strategy=worker` is not in effect.

`RunRequest.persistent_worker` is a new field. Runners that do not
implement it must reject requests that set it, because silently running
the tool as a regular process would make it interpret the `@flagfile`
arguments literally. `bb_runner` only accepts the field when persistent
workers are explicitly enabled in its configuration.

### `bb_runner`: running the tool

`runner.NewPersistentWorkerRunner()` (see
`pkg/runner/persistent_worker_runner.go`) decorates another
`RunnerServer`. Requests without `persistent_worker` are forwarded
unmodified. For the rest it performs the following steps.

**Argument splitting.** `SplitPersistentWorkerArguments()` divides argv
into the arguments used to launch the tool and the arguments that
describe the work. An argument is a flag file when it starts with `@`,
`-flagfile=` or `--flagfile=`, which mirrors Bazel's
`(?:@|--?flagfile=)(.+)` pattern. Both lists must be non-empty:
without a startup argument there is no tool to launch, and without a
flag file the request would be indistinguishable from a regular action.

`--persistent_worker` is then appended to the startup arguments. This
step is easy to overlook and impossible to skip. Bazel does *not*
rewrite the command line it sends to a remote execution service: what
arrives is the command line that would be used to run the action as a
regular process — flag files included, `--persistent_worker` absent —
precisely so that a service which ignores the platform property still
executes the action correctly. Reconstructing the worker command line is
therefore the service's job, and it is the same reconstruction
`WorkerParser.splitSpawnArgsIntoWorkerArgsAndFlagFiles()` performs
locally. Without the flag, the tool would parse an empty command line,
never read its standard input, and the exchange would fail. The flag is
appended unconditionally, matching Bazel, which also means it is part of
the pool key.

**Flag file expansion.** `ExpandFlagFileArguments()` reads the flag
files from the input root and replaces each argument with the lines it
contains. This matches Bazel's `WorkerSpawnRunner`:

- `--flagfile=x` and `-flagfile=x` are *not* expanded; the argument is
  passed to the tool verbatim.
- `@x` is expanded, unless it starts with `@@` (an escaped literal) or
  looks like an external repository label (`@repo//pkg:target`).
- Expansion is recursive, and the files are split on `\n` with a
  trailing `\r` and a trailing empty line removed, matching Guava's
  `Splitter.on('\n')` behaviour that Bazel relies on.
- Paths are resolved relative to the action's `working_directory`, which
  is what REv2 says command line arguments are relative to and what an
  unexpanded `--flagfile=` would resolve against. Resolution starts at
  the input root, so `..` can still be used to reach files above the
  working directory, while paths that escape the input root are
  rejected.
- Recursion depth and file size are bounded, so that a cyclic or
  enormous flag file cannot hang or exhaust `bb_runner`.

**Process acquisition.** The pool is keyed by more than just the
client-provided key. `newPersistentWorkerKey()` hashes the tool key, the
protocol, the working directory, the startup arguments and the full
environment. A worker process is therefore never reused for an action
that expects a different environment, even if a client computed
`persistentWorkerKey` incorrectly or maliciously. The tool key is also
retained verbatim in the key struct, so that a hash collision alone
cannot cause reuse.

**Execution.** The execution root is repopulated (see below), the
process is started if it isn't running yet, and a single `WorkRequest`
is written. `request_id` is always `0`, because only one request is in
flight at a time; the protocol reserves that value for non-multiplexed
("singleplex") workers. The response is validated: a non-zero
`request_id` or a `was_cancelled` response is a protocol violation and
causes the process to be discarded. The size of a single `WorkResponse`
is bounded at 64 MiB, for both encodings: the message is decoded into
memory in its entirety, and one tool that emits an unbounded amount of
output must not be able to take down the actions that run alongside it.

**Output harvesting.** Files that the action created inside the
execution root are moved into the input root, so that `bb_worker` can
upload them (see below). This happens even when the tool reported a
non-zero exit code, because clients are still interested in whatever
outputs were produced.

**Output.** `WorkResponse.output` is written to the file that
`bb_worker` uses to capture standard error, and the standard output file
is created empty. This matches what Bazel does with the output of a
persistent worker, and it means the output shows up in the REv2
`ActionResult` exactly as it would for a regular action. Anything the
tool writes to its own standard error is *not* part of any single action
and is instead appended to a per-process `stderr` log file that
operators can inspect. The process's standard *output* is the protocol
channel itself; a tool that writes anything else to it corrupts the
message stream, and the action fails. This is a requirement the
persistent worker protocol places on the tool, not something the runner
can paper over.

**Failure handling.** Any error after the process has been acquired
marks it unhealthy, which causes it to be killed and its directory to be
removed when it is released. This includes I/O errors, protocol
violations and the execution timeout expiring: a tool that did not
answer within the action's timeout cannot be reused, because the
response to the abandoned request would be read as the response to the
next one.

### The symlink farm

This is the central problem that makes remote persistent workers harder
than local ones.

A process cannot change the working directory it was launched with, but
Buildbarn gives every build action a *fresh* build directory, and the
input root lives inside it. A tool process that outlives an action would
therefore keep pointing at a directory that has been deleted.

The solution is the same one Bazel's symlinked sandbox uses. Every
persistent worker process gets a directory of its own, holding a stable
execution root:

```
<persistent worker directory>/
  0/                      <- one directory per worker process
    root/                 <- execution root; the process's working directory
    tmp/                  <- TMPDIR for this process
    stderr                <- diagnostics not tied to a single action
  1/
    ...
```

Before every action, `root/` is emptied and repopulated with one
symbolic link per child of the current action's input root, each
pointing at the corresponding path in the build directory. Because the
links are created for the *children* of the input root rather than for
the input root itself, the tool observes a directory that looks exactly
like the input root, at a path that never changes.

The tool itself is the one thing that must **not** arrive through a
symbolic link. See [the tool's own inputs](#the-tools-own-inputs).

A non-empty `working_directory` needs a little more care: the process is
launched with `root/<working_directory>` as its working directory, and
paths in the flag files are relative to *that*, while `WorkRequest`
input paths remain relative to the input root. Each component of the
working directory is therefore created as a real directory, and the
siblings at every level are symlinked, so that both `../..`-style
references to the input root and references relative to the working
directory resolve correctly.

Those real directories are *reused* across actions rather than removed
and recreated. A process whose working directory has been unlinked would
otherwise keep resolving relative paths against the removed inode and
never see the next action's inputs. Only their contents are replaced.

Symbolic links are cheap to create and, unlike hard links, work for
directories and across the FUSE/NFSv4 based virtual file systems that
`bb_worker` can use for the build directory. The persistent worker
directory does *not* need to be on the same file system as the build
directory; see [harvesting outputs](#harvesting-outputs).

### The tool's own inputs

A symbolic link is the wrong way to expose the tool, and this is not a
detail: it is the difference between the feature working and not
working at all.

`bb_worker` names each build directory after the digest of the action
that runs in it, and removes it as soon as that action completes. A tool
reached through `root/<something> -> <build dir>/<digest>/root/...` is
therefore launched from a path that stops existing the moment the first
action finishes. The process survives — the kernel keeps the inode alive
— but anything that *re-derives* a path from the tool's own install
location does not. Every JVM does exactly that at startup, to compute
`java.home`, which makes this fatal for Bazel's `JavaBuilder`: on the
second action `javac` compares its `--system` directory against
`java.home` using `Files.isSameFile`, the removed directory raises an
`IOException`, and the action fails with an `IllegalArgumentException`.

This is what the `bazel_tool_input` node property is for. `bb_worker`
collects the paths that carry it while it walks the input root, and
sends them to the runner in `PersistentWorker.tool_input_paths`. A
directory whose subtree consists of nothing but tool inputs is collapsed
into a single path, so a JDK costs one entry rather than thousands.

The runner materializes those paths inside the worker's own execution
root, once, before the process is started: a hard link where the two
directories share a file system, a copy where they do not. Directories
that merely lead to tool inputs become real directories whose *other*
children are still symlinked per action, so a directory holding both the
tool and the action's sources behaves correctly. Nothing re-materializes
on later actions, which is safe because the worker's pooling key covers
both the tool's contents (through Bazel's `persistentWorkerKey`) and its
layout (through the tool input paths themselves).

### Harvesting outputs

**Symlink in, move out.** The farm only solves half of the problem.
Outputs that the tool writes to a path that traverses a symbolic link
land in the input root automatically, because the link resolves there.
Outputs written to a path that does *not* — a file created directly in
the execution root, or anywhere inside one of the real directories that
make up the working directory — land in the worker's own directory,
where `bb_worker` would never look for them. After every action the
execution root is therefore walked, and every entry that is not one of
our symbolic links is moved into the corresponding location in the input
root. Two kinds of entry are descended into rather than moved: the real
working directory components, so that the running process keeps its
working directory, and the directories that lead to tool inputs. The
materialized tool is skipped outright — moving it into the input root
would unlink the executable of the very process that is expected to
serve the next action.

`rename()` is attempted first. It fails with `EXDEV` when the two
directories are not backed by the same file system, which is the norm
rather than the exception in production: deployments place the
persistent worker directory on local disk while the build directory is
provided by a FUSE or NFSv4 mount. The same error is reported when the
two directories are backed by different `filesystem.Directory`
implementations, which cannot rename between each other either. A
recursive copy is used as the fallback in both cases.

This mirrors what Bazel's symlinked sandbox does: symlink the inputs in,
move the outputs out. It also has a useful side effect: because the
entries are *moved*, the execution root is left holding only symbolic
links and the tool, so a stale output from a previous action can never
be mistaken for an output of the next one.

### Worker pool management

`runner.PersistentWorkerPool` (see
`pkg/runner/persistent_worker_pool.go`) owns the processes.

- **Reuse is LIFO.** The most recently released process for a key is
  handed out first, as its caches are the warmest.
- **Eviction is LRU.** When the pool is at `maximum_worker_count` and a
  process for a new key is needed, the idle process that has gone
  unused for the longest is terminated. Processes that are executing an
  action are never terminated, so the maximum is a soft limit; it should
  be set to at least the runner's concurrency.
- **Idle processes expire.** A reaper goroutine, started through
  `PersistentWorkerPool.Run()`, terminates processes that have been idle
  for longer than `idle_timeout`. Because expiry is evaluated on a timer
  of the same period, a process may live for up to twice that long.
- **Startup is clean.** The persistent worker directory is emptied when
  the pool is created, so that a crashed `bb_runner` does not leave
  state behind. The directory must therefore not be shared between
  `bb_runner` instances.
- **Shutdown is clean.** Cancelling the context passed to `Run()`
  terminates all idle processes. Termination closes the process's
  standard input (the protocol's graceful shutdown signal), kills it
  without waiting — an unresponsive tool must not be able to block
  shutdown — reaps it, and removes its directory.
- **Processes outlive their action's context.** They are deliberately
  created with `context.Background()`, and their standard input and
  output are `os.Pipe()`s rather than the pipes `exec.Cmd` creates, so
  that `Wait()` never blocks on a copying goroutine.

### Interaction with other features

**Process table cleaner.** `clean_process_table` kills every process
owned by the runner's user that was created after `bb_runner` started.
That would kill idle persistent workers. `bb_runner` now excludes live
worker PIDs through `PersistentWorkerPool.ContainsProcessID()`.

**`chroot_into_input_root`.** Rejected at startup when combined with
persistent workers. A chrooted process is confined to a single input
root, which is exactly the thing a persistent worker has to outlive.

**`set_tmpdir_environment_variable`.** Honoured, but pointed at the
worker's own `tmp/` directory rather than the action's, since the
process outlives the action. That directory is removed when the process
is terminated.

**Action timeouts.** Enforced by `bb_worker` as usual. The runner stops
waiting for the response when the context is cancelled and the process
is discarded.

**Runners without persistent worker support.** `runner.LocalRunner`
rejects any request carrying `persistent_worker` with
`InvalidArgument`. Silently running the tool as a regular process would
hand it literal `@flagfile` arguments, which a tool built as a
persistent worker does not expand, so it would compute the wrong result
without anyone noticing. Failing loudly is the only safe behaviour, and
it also catches the misconfiguration where `bb_worker` has persistent
workers enabled but the runner it talks to does not.

**In-flight deduplication, caching, size classes.** Unaffected: from the
scheduler's and `bb_worker`'s point of view, this is an ordinary action
that happens to be executed differently.

## Configuration

`bb_scheduler`:

```jsonnet
actionRouter: {
  simple: {
    platformKeyExtractor: {
      stripping: {
        propertyNames: ['persistentWorkerKey', 'persistentWorkerProtocol'],
        base: { action: {} },
      },
    },
    invocationKeyExtractors: [
      { toolInvocationId: {} },
      { platformPropertyName: 'persistentWorkerKey' },
    ],
    initialSizeClassAnalyzer: { /* ... */ },
  },
},
```

and, on the platform queue that should have worker affinity:

```jsonnet
workerInvocationStickinessLimits: ['0s', '300s'],
```

The list has one entry per invocation key extractor: no stickiness at
the Bazel invocation level, five minutes of stickiness at the tool
level.

`bb_worker`:

```jsonnet
runners: [{
  endpoint: { address: 'unix:///worker/runner' },
  concurrency: 8,
  persistentWorkers: {
    maximumInputFileCount: 10000,
  },
  // ...
}],
```

`bb_runner`:

```jsonnet
{
  buildDirectoryPath: '/worker/build',
  persistentWorkers: {
    directoryPath: '/worker/persistent',
    maximumWorkerCount: 32,
    idleTimeout: '900s',
  },
  // ...
}
```

`directoryPath` must not be shared between `bb_runner` instances. It
does not need to be on the same file system as `buildDirectoryPath`,
though placing it there is faster: the tool is then hard linked into
each worker's execution root instead of copied, and outputs are moved
with `rename()` instead of being copied out. Enabling it in
`bb_worker` without enabling it in `bb_runner` causes every persistent
worker action to fail, as the runner rejects a `RunRequest` it cannot
honour.

Finally, Bazel needs to be invoked with
`--experimental_remote_mark_tool_inputs`.

## Observability

| Metric | Labels | Meaning |
| --- | --- | --- |
| `buildbarn_builder_persistent_worker_extractor_actions_total` | `result` = `Used`, `TooManyInputFiles` | Actions that requested a persistent worker, and whether the request was honoured. |
| `buildbarn_runner_persistent_worker_pool_operations_total` | `result` = `Reused`, `Created` | Whether an existing process could be reused. The ratio is the primary indicator of how well stickiness is working. |
| `buildbarn_runner_persistent_worker_pool_terminations_total` | `reason` = `Evicted`, `IdleTimeout`, `Failed` | Why processes were terminated. A high `Evicted` rate means `maximum_worker_count` is too low. |
| `buildbarn_runner_persistent_worker_pool_processes` | `state` = `Idle`, `Busy` | Current number of processes. |

Per-process diagnostics that are not attributable to a single action end
up in `<directory_path>/<id>/stderr`.

## Testing plan

The implementation is covered at four levels. Everything below runs
under `bazel test //...`; the `internal/mock` package is generated by
Bazel, so plain `go test` is not sufficient.

### 1. Pure unit tests

| Area | Test | What it pins down |
| --- | --- | --- |
| Argument splitting | `TestSplitPersistentWorkerArguments` (`pkg/runner`) | Both flag file spellings, `@@`-escaped arguments, empty arguments not being flag files, and the two "must be non-empty" errors. |
| Flag file expansion | `TestExpandFlagFileArguments` (`pkg/runner`) | Simple and nested expansion, `\r\n` line endings, empty files, blank lines and a missing trailing newline, `--flagfile=`/`@@`/`@repo//pkg` *not* being expanded, non-existent files, paths escaping the input root, cyclic includes, and resolution relative to a nested working directory including `..` back into the input root. Uses a real temporary directory rather than a mock, so that path resolution is exercised for real. |
| Wire protocol | `TestPersistentWorkerProtocolProto`, `TestPersistentWorkerProtocolJSON`, `TestPersistentWorkerProtocolUnsupported` (`pkg/runner`) | That the Protobuf encoding is byte-for-byte `writeDelimitedTo()`, that responses larger than protodelim's 4 MiB default limit can be read, that truncated input is an error, that JSON messages are newline separated with no insignificant whitespace, that `bytes` fields are base64 encoded, that unknown fields and inter-message whitespace are tolerated, and that what we write round-trips through `protojson`. Both encodings have an `OversizedWorkResponse` subtest that feeds in a message beyond the 64 MiB bound and asserts it is rejected rather than allocated. |
| Message size bound | `TestResettableLimitReader` (`pkg/runner`) | That a message of exactly the limit is accepted, that one byte more is an error, and — the subtle one — that the budget is per message rather than for the lifetime of the reader, so a long-lived worker is not cut off after its first large response. |

### 2. Mock-driven unit tests

| Area | Test | What it pins down |
| --- | --- | --- |
| `bb_worker` extraction | `TestPersistentWorkerExtractor` (`pkg/builder`) | That actions without the property never touch storage; that unrelated properties are ignored; that a nested input root yields correctly ordered, input-root-relative paths with hexadecimal digests; that symlink entries are skipped; that both protocol values and an invalid one behave correctly; that the input file limit falls back to regular execution at the boundary and one past it; and that malformed digests, invalid filenames and storage failures produce the expected status codes and messages. |
| Scheduler platform key | `TestStrippingKeyExtractor` (`pkg/scheduler/platform`) | That nothing is stripped when nothing matches; that stripping one or several properties yields the platform key a plain worker announces; that stripping *every* property equals an action with no platform at all; that two different `persistentWorkerKey` values collapse onto one key — the property that makes routing work; that the caller's `Action` is not mutated; and that errors from the underlying extractor propagate. |
| Scheduler invocation key | `TestPlatformPropertyInvocationKeyExtractor` (`pkg/scheduler/invocation`) | That the key reflects the property value, that a missing property and a missing platform produce the same key, and that distinct tools produce distinct keys — the property that makes stickiness work. |

### 3. End-to-end tests against a real worker process

`pkg/runner/persistent_worker_runner_test.go` makes the test binary
double as a persistent worker. `TestMain` checks for the
`BB_RUNNER_TEST_FAKE_PERSISTENT_WORKER` environment variable and, when
it is set, speaks the persistent worker protocol on standard input and
output instead of running tests. The fake worker reports its process ID,
how many requests it has served, the arguments it was launched with, the
`WorkRequest` it received, its working directory and the contents of a
marker file from the execution root, encoded as JSON in
`WorkResponse.output`. It supports options to crash, to hang and to
return a non-zero exit code.

This gives real process creation, real pipes and real symlink farms
without depending on an external tool being installed.

| Test | What it pins down |
| --- | --- |
| `TestPersistentWorkerRunnerPassthrough` | Requests without `persistent_worker` reach the underlying runner unmodified. |
| `TestPersistentWorkerRunnerReuse` | The heart of the feature: two actions with the same key, run from **two different input roots**, are served by the **same process ID**, and the second one sees the second input root's `marker.txt` through the symlink farm. A third action with a different key gets a different process ID. |
| `TestPersistentWorkerRunnerWorkingDirectory` | Reuse for an action with a nested `working_directory`. The process must keep the same working directory across actions — the directories along that path are preserved rather than recreated — while both the marker file in the working directory and the one in the input root above it reflect the new action. This is the case that a naive "empty the execution root" implementation gets wrong. |
| `TestPersistentWorkerRunnerOutputFiles` | The harvest step. The tool writes an output into the execution root, both at its top level and inside a nested working directory, using paths that do not traverse a symbolic link. Both must end up in the action's input root, and the execution root must be left holding nothing but symbolic links, so the next action cannot observe a stale output. |
| `TestPersistentWorkerRunnerJSONProtocol` | The same flow over newline delimited JSON. |
| `TestPersistentWorkerRunnerNonZeroExitCode` | A tool that fails yields `RunResponse.exit_code` rather than a gRPC error, and its output lands in the action's stderr file. |
| `TestPersistentWorkerRunnerCrash` | A process that dies mid-request produces an error, is not returned to the pool, and a subsequent action gets a freshly started process. |
| `TestPersistentWorkerRunnerTimeout` | A hanging tool causes the action's context deadline to be reported, and the process is discarded rather than reused. |
| `TestPersistentWorkerRunnerInvalidRequests` | An empty key, an argument list without a flag file, and a non-existent input root are all rejected with `InvalidArgument`. |
| `TestLocalRunnerRun/PersistentWorkerNotSupported` | A runner without persistent worker support rejects an action that requests one with `InvalidArgument`, rather than running the tool with unexpanded `@flagfile` arguments. |
| `TestPersistentWorkerPoolEviction` | With `maximum_worker_count` set to 1, alternating between two keys terminates and recreates processes instead of growing the pool. |
| `TestPersistentWorkerPoolIdleTimeout` | Driven by a mock `clock.Clock` and a mock timer: advancing time past the idle timeout and firing the reaper terminates the idle process, and the next action starts a new one. |
| `TestPersistentWorkerRunnerToolInputs` | The regression test for tool materialization. A tool stored in the input root is launched, the **whole build directory of the first action is deleted**, and a second action must still be served by the *same process ID* and still be able to read a file next to its own executable. Fails with `EOF` — the process dies — if the tool is left as a symbolic link. |
| `TestPersistentWorkerRunnerToolInputsPartialDirectory` | A directory holding both a tool input and a regular input: the tool survives the removal of the first input root, while the regular input tracks the action that is currently running. |
| `TestPersistentWorkerRunnerToolInputsInvalid` | The input root itself, a path escaping the input root, and a path containing the action's working directory are all rejected with `InvalidArgument`. |
| `TestParseToolInputPaths` | Tree construction: nesting, a path underneath an already-materialized ancestor, the reverse order, and every rejected path. |
| `TestMaterializeToolInputs` | Files, symbolic links and nested directories are all reproduced in the execution root, nothing outside the tool is, and the result outlives the removal of the input root. |
| `TestRefreshSymlinkFarmToolInputs` | Across two input roots: the tool stays a real file while its non-tool siblings are repointed. |
| `TestIsCrossDevice` | `EXDEV` is recognised bare and wrapped; `ENOENT` and `nil` are not. |
| `TestMoveIntoInputRootCrossDevice` | With `rename()` forced to fail with `EXDEV`: regular files, the executable bit, nested directories, symbolic links and a pre-existing destination are all handled by the copy fallback. |

### 4. Whole-repository checks

- `bazel test //...` — the full suite, including the pre-existing
  `pkg/builder`, `pkg/runner` and `pkg/scheduler` tests, which the
  changed `NewLocalBuildExecutor()` and
  `invocation.KeyExtractor.ExtractKey()` signatures touch.
- `bazel build //...` for every platform in CI, so that the new code
  compiles on Windows and the BSDs too.
- `bazel run @com_github_buildbarn_bb_storage//tools:reformat` followed
  by `git diff --exit-code`, which regenerates the `.pb.go` files, the
  `BUILD.bazel` files and reformats the `.proto` files.

### Manual verification

Automated tests cannot cover a real Bazel client, so the following is
worth doing once against a deployment:

1. Run a Java build with `--experimental_remote_mark_tool_inputs` and
   `--remote_executor` pointing at the cluster. Confirm that
   `buildbarn_runner_persistent_worker_pool_operations_total{result="Reused"}`
   grows.
2. Confirm that compiler diagnostics still appear in Bazel's output and
   in `bb_browser`, which verifies the `WorkResponse.output` to stderr
   mapping.
3. Confirm that a build of more than a handful of Java targets
   *completes*. Tool materialization is only exercised from the second
   action onwards on a given process, so a one-action smoke test passes
   even when it is broken.
3. Run the same build without the flag and confirm nothing changes
   functionally.
4. Kill a tool process by hand and confirm that the next action
   succeeds.
5. Point a client at the cluster that sets `persistentWorkerKey` without
   the runner having persistent workers enabled, and confirm the action
   fails loudly rather than running the tool with literal `@flagfile`
   arguments.

## Known limitations

- **Multiplex workers are not supported.** Only one request is in flight
  per process (`request_id` is always `0`). Bazel's multiplex workers
  would allow a single process to serve several actions concurrently.
  The pool would need to hand the same process to several callers and
  demultiplex responses by request ID.
- **`--worker_extra_flag` is not supported.** Bazel appends these
  per-mnemonic flags to a worker's command line locally, but they are a
  property of the client's own command line and are not communicated to
  a remote execution service. The upstream design proposal lists this as
  an open question.
- **Cancellation is not sent.** When an action times out, the process is
  killed rather than sent a `cancel` request. Killing is always correct;
  sending `cancel` first would let more processes be reused.
- **Symlinks in the input root are not reported as inputs.** Only
  regular files appear in `WorkRequest.inputs`. Tools that key their
  incremental state on a symlink's target would not notice a change to
  it. Bazel's own workers do not do this.
- **A client that marks no tool inputs gets a worker that can only
  serve one action.** `bazel_tool_input` node properties are what tells
  the server which files make up the tool, and therefore which files
  need a home that outlives the action. Without them the tool is
  reached through a symbolic link into a build directory that is
  removed as soon as the first action completes, and any tool that
  resolves a path relative to its own executable — every JVM — fails on
  the second action. Bazel always emits the properties alongside
  `persistentWorkerKey`, so this only affects other clients.
- **Files the tool writes into its own installation directory are not
  harvested, and persist across actions.** The tool's directory belongs
  to the worker process rather than to any single action. This matches
  what Bazel's local workers do, where the tool likewise lives in a
  directory that outlives the action.
- **Tool materialization is not shared between worker processes.** Two
  processes with the same key each get their own copy. Where the
  persistent worker directory and the build directory share a file
  system this is free, as hard links are used; where they do not, the
  tool is copied once per process.
- **The worker's `tmp/` directory is never cleaned between actions.**
  It is created when the process starts and removed when it is
  terminated. Emptying it per action would be wrong for the tools this
  feature exists for — a worker is explicitly allowed to keep state
  across requests, and some keep it on disk — but it does mean a
  long-lived process that leaks temporary files grows without bound.
  `idle_timeout` and `maximum_worker_count` are the backstop; operators
  running such a tool should tighten them.
- **A single `WorkResponse` may not exceed 64 MiB.** A tool that emits
  more output than that for one action fails that action and is
  discarded. The limit exists because the message is decoded into
  memory in full.
- **The process table cleaner only protects the worker processes
  themselves.** Children that a persistent worker spawns are still
  eligible for cleanup. In practice the cleaner only runs while the
  runner is idle.
- **`maximum_worker_count` is a soft limit.** Processes that are
  executing an action are never evicted, so the pool can temporarily
  exceed the configured maximum by up to the runner's concurrency.
