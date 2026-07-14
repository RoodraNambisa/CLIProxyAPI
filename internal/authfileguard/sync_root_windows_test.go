//go:build windows

package authfileguard

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSyncRootDirectoryAfterRootRename(t *testing.T) {
	baseDir := t.TempDir()
	originalDir := filepath.Join(baseDir, "original")
	if errMkdir := os.Mkdir(originalDir, 0o700); errMkdir != nil {
		t.Fatalf("mkdir original root: %v", errMkdir)
	}
	root, errRoot := os.OpenRoot(originalDir)
	if errRoot != nil {
		t.Fatalf("open original root: %v", errRoot)
	}
	defer func() { _ = root.Close() }()
	movedDir := filepath.Join(baseDir, "moved")
	if errRename := os.Rename(originalDir, movedDir); errRename != nil {
		t.Fatalf("rename root: %v", errRename)
	}
	if errSync := SyncRootDirectory(root, "."); errSync != nil {
		t.Fatalf("sync renamed root: %v", errSync)
	}
}

func TestSyncRootDirectoryThroughRootSymlink(t *testing.T) {
	baseDir := t.TempDir()
	targetDir := filepath.Join(baseDir, "target")
	if errMkdir := os.Mkdir(targetDir, 0o700); errMkdir != nil {
		t.Fatalf("mkdir target root: %v", errMkdir)
	}
	linkDir := filepath.Join(baseDir, "link")
	if errLink := os.Symlink(targetDir, linkDir); errLink != nil {
		t.Skipf("directory symlink unavailable: %v", errLink)
	}
	root, errRoot := os.OpenRoot(linkDir)
	if errRoot != nil {
		t.Fatalf("open linked root: %v", errRoot)
	}
	defer func() { _ = root.Close() }()
	if errSync := SyncRootDirectory(root, "."); errSync != nil {
		t.Fatalf("sync linked root: %v", errSync)
	}
}
