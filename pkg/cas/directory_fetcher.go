package cas

import (
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// DirectoryFetcher is responsible for fetching Directory messages from
// the Content Addressable Storage (CAS). These describe the layout of a
// single directory in a build action's input root.
type DirectoryFetcher Fetcher[remoteexecution.Directory]
