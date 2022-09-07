package cleaner

import (
	"context"
	"os/exec"

	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
)

// NewCommandRunningCleaner creates a new Cleaner that executes a command on the
// system and expects it to succeed.
func NewCommandRunningCleaner(command string, args []string) Cleaner {
	return func(ctx context.Context) error {
		if err := exec.CommandContext(ctx, command, args...).Run(); err != nil {
			return util.StatusWrapWithCode(err, codes.Internal, "Failed to run cleaning command")
		}
		return nil
	}
}
