package builder

import (
	"io"
	"os"
	"sort"
	"syscall"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/kballard/go-shellquote"
)

// mkdirEmittingDirectory is an implementation of
// ParentPopulatingDirectory that does little more than track which
// directories are created. For each leaf directory created, it is
// capable of emitting an "mkdir -p" shell command.
type mkdirEmittingDirectory struct {
	children map[path.Component]*mkdirEmittingDirectory
}

func (d *mkdirEmittingDirectory) Close() error {
	return nil
}

func (d *mkdirEmittingDirectory) EnterParentPopulatableDirectory(name path.Component) (ParentPopulatableDirectory, error) {
	if child, ok := d.children[name]; ok {
		return child, nil
	}
	return nil, syscall.ENOENT
}

func (d *mkdirEmittingDirectory) Mkdir(name path.Component, perm os.FileMode) error {
	if _, ok := d.children[name]; ok {
		return syscall.EEXIST
	}
	d.children[name] = &mkdirEmittingDirectory{
		children: map[path.Component]*mkdirEmittingDirectory{},
	}
	return nil
}

func (d *mkdirEmittingDirectory) emitCommands(trace *path.Trace, w io.StringWriter) error {
	if len(d.children) > 0 {
		// This directory has children, so there's no need to
		// emit an 'mkdir -p' call for this directory.
		l := make(componentsList, 0, len(d.children))
		for name := range d.children {
			l = append(l, name)
		}
		sort.Sort(l)
		for _, name := range l {
			if err := d.children[name].emitCommands(trace.Append(name), w); err != nil {
				return err
			}
		}
	} else if trace != nil {
		// This directory has no children and it's not the root
		// directory. Emit a single 'mkdir -p' call.
		if _, err := w.WriteString("mkdir -p "); err != nil {
			return err
		}
		if _, err := w.WriteString(shellquote.Join(trace.String())); err != nil {
			return err
		}
		if _, err := w.WriteString("\n"); err != nil {
			return err
		}
	}
	return nil
}

// ConvertCommandToShellScript writes a POSIX shell script to a
// StringWriter that causes a process to be launched in the way encoded
// in a Command message.
//
// Because input roots do not explicitly store parent directories of
// outputs, and actions generally assume that they exist, the resulting
// shell script may contain one or more "mkdir -p" calls to create those
// directories prior to execution.
func ConvertCommandToShellScript(command *remoteexecution.Command, w io.StringWriter) error {
	// Preamble.
	if _, err := w.WriteString("#!/bin/sh\nset -e\n"); err != nil {
		return err
	}

	// Create parent directories of outputs.
	outputHierarchy, err := NewOutputHierarchy(command)
	if err != nil {
		return err
	}
	d := mkdirEmittingDirectory{
		children: map[path.Component]*mkdirEmittingDirectory{},
	}
	if err := outputHierarchy.CreateParentDirectories(&d); err != nil {
		return err
	}
	if err := d.emitCommands(nil, w); err != nil {
		return err
	}

	// Switch to the right working directory.
	workingDirectory, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	if err := path.Resolve(command.WorkingDirectory, scopeWalker); err != nil {
		return util.StatusWrap(err, "Failed to resolve working directory")
	}
	if _, err := w.WriteString("cd "); err != nil {
		return err
	}
	if _, err := w.WriteString(shellquote.Join(workingDirectory.String())); err != nil {
		return err
	}
	if _, err := w.WriteString("\n"); err != nil {
		return err
	}

	// Set environment variables.
	for _, environmentVariable := range command.EnvironmentVariables {
		if _, err := w.WriteString("export "); err != nil {
			return err
		}
		if _, err := w.WriteString(environmentVariable.Name); err != nil {
			return err
		}
		if _, err := w.WriteString("="); err != nil {
			return err
		}
		if _, err := w.WriteString(shellquote.Join(environmentVariable.Value)); err != nil {
			return err
		}
		if _, err := w.WriteString("\n"); err != nil {
			return err
		}
	}

	// Execute the command.
	if _, err := w.WriteString("exec"); err != nil {
		return err
	}
	for _, argument := range command.Arguments {
		if _, err := w.WriteString(" "); err != nil {
			return err
		}
		if _, err := w.WriteString(shellquote.Join(argument)); err != nil {
			return err
		}
	}
	if _, err := w.WriteString("\n"); err != nil {
		return err
	}
	return nil
}
