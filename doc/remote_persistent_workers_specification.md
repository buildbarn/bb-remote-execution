# Remote persistent workers: a specification

*Status: draft, intended for submission to
[bazelbuild/remote-apis](https://github.com/bazelbuild/remote-apis).*

Bazel can execute certain build actions through *persistent workers*:
long running tool processes that receive one unit of work at a time over
their standard input, so that a JVM stays warm and incremental state is
retained between actions. Since Bazel 5,
`--experimental_remote_mark_tool_inputs` also annotates *remote* actions
so that a remote execution service can do the same.

The annotations were introduced by the design proposal
[Remote Persistent Workers](https://github.com/bazelbuild/proposals/blob/main/designs/2021-03-06-remote-persistent-workers.md),
which explicitly leaves the protocol unspecified:

> The persistent worker protocol is not formally specified.

That is the gap this document tries to close. Everything below was
derived from Bazel's own implementation — `RemoteExecutionService`,
`PlatformProperties`, `Spawns`, `WorkerParser` and `WorkerSpawnRunner` —
and from a working implementation of the server side in
[Buildbarn](https://github.com/buildbarn/bb-remote-execution) (see
[doc/persistent_workers.md](persistent_workers.md)). Its purpose is to
state the contract precisely enough that two independent
implementations agree, and to record the places where the current
contract is ambiguous.

## 1. What the client sends

A client that wants an action to be eligible for persistent worker
execution does three things, and nothing else. In particular, **the
`Command` message is not modified**: `Command.arguments` is exactly the
command line that would be used to run the action as a regular process.

### 1.1 `persistentWorkerKey` platform property

The client sets a platform property named `persistentWorkerKey` to a
non-empty, opaque string. Bazel derives it from the names and digests
of the tool inputs and runfiles, the action's environment variables, the
rewritten command line, the action mnemonic, and a few internal flags.

The value has no structure a server may rely on. Its only guaranteed
property is the one that matters: **two actions carrying the same key
may be executed by the same worker process; two actions carrying
different keys may not.**

The property is set on the platform of *both* the `Command` and the
`Action` message. Servers implementing REv2.2 or later should read it
from `Action.platform`.

### 1.2 `persistentWorkerProtocol` platform property

Optional. Selects the encoding used on the worker's standard input and
output. Bazel copies it verbatim from the tool's
`requires-worker-protocol` execution requirement, which it validates
against exactly two values:

| Value | Meaning |
| --- | --- |
| `proto` | Length delimited `blaze.worker.WorkRequest` / `WorkResponse` messages. |
| `json` | The canonical Protobuf JSON mapping of those messages, newline separated. |

An absent property means `proto`. Servers should reject other values
rather than guess.

> **Ambiguity.** Bazel's own `PlatformProperties` javadoc documents the
> supported values as "json, protobuf". No Bazel code path can produce
> `protobuf`, because `Spawns.getWorkerProtocolFormat()` rejects
> anything other than `json` and `proto`. The specification should
> settle on `json` and `proto` and Bazel's comment should be corrected.

### 1.3 `bazel_tool_input` node properties

Files in the input root that are tool inputs carry a `NodeProperty`
named `bazel_tool_input` with an empty value.

This is informational: it lets a server compute a worker key of its own,
or decide where to place tool files. A server that takes the key from
`persistentWorkerKey` — as this specification assumes — does not need to
read it. It notably does **not** partition the input root: tool inputs
remain ordinary inputs and are reported to the worker like any other
file (§3.2).

### 1.4 Backwards compatibility

A server that does not understand any of the above ignores it and
executes the action as a regular process. This is always correct: the
`Command` message describes a complete, self-contained action. This
property is the reason the client must not rewrite the command line, and
it is worth preserving in any future extension.

## 2. Reconstructing the worker command line

A server that honours `persistentWorkerKey` must transform
`Command.arguments` into the command line of a worker process. The
transformation mirrors
`WorkerParser.splitSpawnArgsIntoWorkerArgsAndFlagFiles()`.

1. **Partition the arguments.** An argument is a *flag file argument*
   when it matches `(?:@|--?flagfile=)(.+)`, i.e. it starts with `@`,
   `-flagfile=` or `--flagfile=` and is longer than that prefix. Note
   that `@@`-escaped arguments *do* match. All other arguments are
   *startup arguments*, in their original order.
2. **Require both to be non-empty.** An action with no startup argument
   has no tool to launch; an action with no flag file argument carries
   no work.
3. **Append `--persistent_worker`** to the startup arguments. This is
   the argument that makes the tool read `WorkRequest` messages from its
   standard input instead of acting on its command line. Bazel appends
   it unconditionally when launching a worker locally, and servers
   should do the same, including when the argument is already present.

   This step is the one most easily missed, precisely because §1
   requires the client to leave the command line alone. Without it the
   tool runs one-shot against an empty command line.
4. **Expand the flag file arguments** into `WorkRequest.arguments`, as
   described in §3.1.

Bazel additionally appends any `--worker_extra_flag` values that match
the action's mnemonic. Those are a property of the client's own command
line and are not transmitted, so a server cannot reproduce them. The
design proposal lists this as an open question.

## 3. The `WorkRequest` message

### 3.1 `arguments`

Produced by expanding each flag file argument, in order, using the rules
of `WorkerSpawnRunner.expandArgument()`:

- An argument starting with `@` is replaced by the lines of the file it
  names, each of which is expanded recursively — **unless** it starts
  with `@@` (an escaped literal) or matches `^@.*//.*` (an external
  repository label). Those are passed through verbatim.
- `--flagfile=` and `-flagfile=` arguments are passed through verbatim.
  The tool is expected to read them itself.
- Files are split on `\n`. A trailing `\r` is stripped from each line
  and a single trailing newline does not produce an empty argument, so
  that the result matches Guava's `CharSource.readLines()`, which Bazel
  uses.

Path resolution deserves a note. Bazel resolves these paths against the
exec root, which is also the working directory of a locally executed
spawn, so the two readings coincide and it never had to choose. Under
REv2, `Command.working_directory` may be non-empty, and the two readings
differ. Resolving against the working directory is the reading that
keeps `@x` and an unexpanded `--flagfile=x` referring to the same file,
and it is what REv2 says argument paths are relative to. Servers should
resolve against the working directory, permit `..` to reach the input
root above it, and reject paths that escape the input root or traverse a
symbolic link out of it.

Implementations should bound the recursion depth and the size of a flag
file. Bazel bounds neither, and crashes on a self-referential flag file.

### 3.2 `inputs`

One entry per **file** in the input root — the complete input root, not
only the non-tool inputs.

- `path` is relative to the input root, using `/` separators.
- `digest` is the *ASCII hexadecimal* representation of the file's
  content hash, encoded as UTF-8 bytes. It is not the raw hash, and it
  is not an REv2 `Digest` message. Bazel produces it with
  `ByteString.copyFromUtf8(HashCode.fromBytes(digest).toString())`,
  which is lowercase.

The worker uses this list to invalidate whatever it has cached from
previous requests, so an incomplete list causes silently incorrect
output rather than an error. A server that cannot afford to enumerate a
very large input root must fall back to regular process execution rather
than send a truncated list.

Directories and symbolic links are not reported; Bazel's own workers do
not consult them.

### 3.3 `request_id`, `cancel`, `verbosity`, `sandbox_dir`

`request_id` is `0` for a singleplex worker — one request in flight per
process — and Bazel requires that value. Multiplex workers, `cancel`
requests and `sandbox_dir` are out of scope for this document; there is
currently no platform property by which a client can state that a tool
supports them remotely. The proposal mentions `persistentWorkerMultiplex`
as a candidate.

## 4. Executing the request

### 4.1 The worker's file system view

A worker process outlives the action that started it, but each action
has its own input root. The process cannot change the directory it was
launched in, so the server must give it a *stable* path whose contents
change per action.

The workable technique is Bazel's own symlinked sandbox: give each
worker process a private execution root, and before every action
repopulate it with one symbolic link per child of the current input
root. When `Command.working_directory` is non-empty, the components
along that path must be real directories — created once and reused, not
recreated, or the running process's working directory is unlinked — with
the siblings at each level symlinked.

After the action completes, anything in the execution root that is not
one of those symbolic links is an output the tool wrote through a path
that did not traverse a link, and must be moved into the input root
before the server collects `Command.output_paths`.

A server is not obliged to use this technique, but it is obliged to
provide its observable result:

- the tool's working directory contains the current action's input root,
  at a path that does not change between actions;
- paths in `WorkRequest.inputs` resolve relative to the input root;
- outputs the tool writes are collected as they would be for a regular
  action;
- no output of a previous action is visible.

### 4.2 The exchange

Write one `WorkRequest`, read one `WorkResponse`. The worker's standard
input and output are the protocol channel and carry nothing else; a tool
that writes anything else to its standard output corrupts the stream.
Standard error is not part of any single action.

`WorkResponse.output` is the action's output as the user should see it.
Servers should surface it as the action's standard error, which is what
Bazel does. `WorkResponse.exit_code` is the action's exit code; a
non-zero value is a failed action, not a failed worker.

A response whose `request_id` does not match the request, or whose
`was_cancelled` is set without a cancellation having been sent, is a
protocol violation.

Implementations should bound the size of a single `WorkResponse`. The
`proto` encoding is length prefixed and therefore easy to bound; the
`json` encoding is not.

### 4.3 Worker lifetime

The following are implementation decisions, but a specification should
state the constraints they operate under:

- A worker process must be discarded, not reused, after any I/O error,
  protocol violation, or action timeout. The response to an abandoned
  request would otherwise be read as the response to the next one.
- A worker must not be reused for an action whose environment or startup
  command line differs, even if `persistentWorkerKey` matches. The key
  is client-computed and a server cannot verify it.
- Servers are free to terminate idle workers at any time. Clients must
  not assume state survives between actions; the protocol is an
  optimisation, not a contract about caching.
- A worker that is executing an action must not be terminated for
  capacity reasons.

## 5. Summary of the contract

| | Client | Server |
| --- | --- | --- |
| `persistentWorkerKey` | sets it on `Action`/`Command` platform | routes so that equal keys can share a process |
| `persistentWorkerProtocol` | sets `json`, or omits for `proto` | selects the encoding, rejects other values |
| `bazel_tool_input` | marks tool inputs | may ignore |
| `Command.arguments` | leaves unmodified | splits, appends `--persistent_worker` |
| flag files | are inputs | expands into `WorkRequest.arguments` |
| input root | is complete | reports every file in `WorkRequest.inputs` |
| working directory | `Command.working_directory` | keeps it stable across actions |
| outputs | `Command.output_paths` | collects as for a regular action |

## 6. Open questions

1. `protobuf` versus `proto` as the value of `persistentWorkerProtocol`
   (§1.2).
2. Whether `--worker_extra_flag` should be transmitted, and how (§2).
3. Whether multiplex workers, cancellation and `sandbox_dir` should be
   specified, and which platform properties would carry them (§3.3).
4. Whether flag file paths resolve against the input root or against
   `Command.working_directory` (§3.1). Bazel has never had to choose;
   REv2 servers do.
5. Whether these properties should keep their `bazel_`/camelCase
   spelling once formally adopted, or be renamed as part of moving them
   into the REv2 specification proper.
