package cas

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// copyCachedFile copies a file between directories, yielding an independent file
// with its own hard link count. It is the fallback for hardlinking when the
// source has reached the filesystem's maximum link count (1023 on NTFS).
func copyCachedFile(srcDirectory filesystem.Directory, srcName path.Component, dstDirectory filesystem.Directory, dstName path.Component) error {
	r, err := srcDirectory.OpenRead(srcName)
	if err != nil {
		return err
	}
	defer r.Close()

	// CreateExcl mirrors hardlink semantics: the destination must not exist.
	w, err := dstDirectory.OpenWrite(dstName, filesystem.CreateExcl(0o777))
	if err != nil {
		return err
	}

	buf := make([]byte, 1<<16)
	for offset := int64(0); ; {
		n, readErr := r.ReadAt(buf, offset)
		if n > 0 {
			if _, err := w.WriteAt(buf[:n], offset); err != nil {
				w.Close()
				return err
			}
			offset += int64(n)
		}
		if readErr == io.EOF {
			return w.Close()
		}
		if readErr != nil {
			w.Close()
			return readErr
		}
	}
}
