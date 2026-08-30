package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// writeFileAtomic writes data to path with the given permissions by writing
// to a temp file in the same directory and renaming it into place, so a
// concurrent reader (or a crash mid-write) never observes a partial file.
// The parent directory is created (mode 0700) if it doesn't already exist.
func writeFileAtomic(path string, data []byte, perm os.FileMode) (err error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("config: mkdir %s: %w", dir, err)
	}

	tmp, err := os.CreateTemp(dir, ".tmp-"+filepath.Base(path)+"-*")
	if err != nil {
		return fmt.Errorf("config: create temp file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	defer func() {
		// Best-effort cleanup; no-op once the rename below succeeds.
		if removeErr := os.Remove(tmpName); removeErr != nil && err == nil && !os.IsNotExist(removeErr) {
			err = fmt.Errorf("config: cleanup temp file %s: %w", tmpName, removeErr)
		}
	}()

	if _, err = tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("config: write temp file %s: %w", tmpName, err)
	}
	if err = tmp.Chmod(perm); err != nil {
		tmp.Close()
		return fmt.Errorf("config: chmod temp file %s: %w", tmpName, err)
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("config: sync temp file %s: %w", tmpName, err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("config: close temp file %s: %w", tmpName, err)
	}
	if err = os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("config: rename %s to %s: %w", tmpName, path, err)
	}
	return nil
}
