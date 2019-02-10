package environment

import (
	"context"
	"log"
	"path"

	"github.com/buildbarn/bb-remote-execution/pkg/proto/runner"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

type actionDigestSubdirectoryManager struct {
	base               Manager
	subdirectoryFormat util.DigestKeyFormat
}

// NewActionDigestSubdirectoryManager is an adapter for Manager that
// causes build actions to be executed inside a subdirectory within the
// build directory, as opposed to inside the build directory itself. The
// subdirectory is named after the action digest of the build action.
//
// This adapter can be used to add concurrency to a single worker. When
// executing build actions in parallel, every build action needs its own
// build directory. Instead of picking random pathnames or using a
// counter, using the action digest has the advantage of improving
// determinism in case absolute paths end up in build output.
func NewActionDigestSubdirectoryManager(base Manager, subdirectoryFormat util.DigestKeyFormat) Manager {
	return &actionDigestSubdirectoryManager{
		base:               base,
		subdirectoryFormat: subdirectoryFormat,
	}
}

func (em *actionDigestSubdirectoryManager) Acquire(actionDigest *util.Digest, platformProperties map[string]string) (ManagedEnvironment, error) {
	// Allocate underlying environment.
	environment, err := em.base.Acquire(actionDigest, platformProperties)
	if err != nil {
		return nil, err
	}

	// Create build directory within.
	buildDirectory := environment.GetBuildDirectory()
	subdirectoryName := actionDigest.GetKey(em.subdirectoryFormat)
	if err := buildDirectory.Mkdir(subdirectoryName, 0777); err != nil {
		environment.Release()
		return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to create build subdirectory %#v", subdirectoryName)
	}
	subdirectory, err := buildDirectory.Enter(subdirectoryName)
	if err != nil {
		if err := buildDirectory.Remove(subdirectoryName); err != nil {
			log.Print("Failed to remove action digest build directory upon failure to enter: ", err)
		}
		environment.Release()
		return nil, util.StatusWrapfWithCode(err, codes.Internal, "Failed to enter build subdirectory %#v", subdirectoryName)
	}

	return &actionDigestSubdirectoryEnvironment{
		base:             environment,
		subdirectory:     subdirectory,
		subdirectoryName: subdirectoryName,
	}, nil
}

type actionDigestSubdirectoryEnvironment struct {
	base             ManagedEnvironment
	subdirectory     filesystem.Directory
	subdirectoryName string
}

func (e *actionDigestSubdirectoryEnvironment) GetBuildDirectory() filesystem.Directory {
	return e.subdirectory
}

func (e *actionDigestSubdirectoryEnvironment) Run(ctx context.Context, request *runner.RunRequest) (*runner.RunResponse, error) {
	// Prepend subdirectory name to working directory and log files of build action.
	newRequest := *request
	newRequest.WorkingDirectory = path.Join(e.subdirectoryName, newRequest.WorkingDirectory)
	newRequest.StdoutPath = path.Join(e.subdirectoryName, newRequest.StdoutPath)
	newRequest.StderrPath = path.Join(e.subdirectoryName, newRequest.StderrPath)
	return e.base.Run(ctx, &newRequest)
}

func (e *actionDigestSubdirectoryEnvironment) Release() {
	// Remove subdirectory prior to releasing the environment.
	if err := e.subdirectory.Close(); err != nil {
		log.Printf("Failed to close build subdirectory %s: %s", e.subdirectoryName, err)
	}
	if err := e.base.GetBuildDirectory().RemoveAll(e.subdirectoryName); err != nil {
		log.Printf("Failed to remove build subdirectory %s: %s", e.subdirectoryName, err)
	}
	e.base.Release()
}
