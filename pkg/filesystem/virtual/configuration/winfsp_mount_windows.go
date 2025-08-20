//go:build windows
// +build windows

package configuration

import (
	"context"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual/winfsp"
	"github.com/buildbarn/bb-storage/pkg/program"
)

func (m *winfspMount) Expose(terminationGroup program.Group, rootDirectory virtual.Directory) error {
	winfspFs := winfsp.NewFileSystem(rootDirectory, m.caseSensitive)
	fs, err := winfspFs.Mount(m.fsName, m.mountPath)
	if err != nil {
		return err
	}
	// Automatically unmount upon shutdown.
	terminationGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		<-ctx.Done()
		fs.Unmount()
		return nil
	})
	return nil
}
