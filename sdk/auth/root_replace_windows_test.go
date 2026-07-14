//go:build windows

package auth

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReplaceRootFileReplacesExistingTarget(t *testing.T) {
	dir := t.TempDir()
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer closeFileTokenRoot(root)
	if errWrite := root.WriteFile("old.json", []byte("new"), 0o600); errWrite != nil {
		t.Fatalf("write replacement: %v", errWrite)
	}
	if errWrite := root.WriteFile("auth.json", []byte("old"), 0o600); errWrite != nil {
		t.Fatalf("write target: %v", errWrite)
	}
	if errReplace := replaceRootFile(root, "old.json", "auth.json"); errReplace != nil {
		t.Fatalf("replaceRootFile() error = %v", errReplace)
	}
	if data, errRead := root.ReadFile("auth.json"); errRead != nil || string(data) != "new" {
		t.Fatalf("replaced data = %q, %v", data, errRead)
	}
}

func TestReplaceRootFileKeepsRenamedParentIdentity(t *testing.T) {
	baseDir := t.TempDir()
	parentDir := filepath.Join(baseDir, "nested")
	if errMkdir := os.Mkdir(parentDir, 0o700); errMkdir != nil {
		t.Fatalf("create parent: %v", errMkdir)
	}
	root, errRoot := os.OpenRoot(parentDir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer closeFileTokenRoot(root)
	if errWrite := root.WriteFile("replacement.json", []byte("new"), 0o600); errWrite != nil {
		t.Fatalf("write replacement: %v", errWrite)
	}
	if errWrite := root.WriteFile("auth.json", []byte("old"), 0o600); errWrite != nil {
		t.Fatalf("write target: %v", errWrite)
	}
	movedParent := parentDir + "-old"
	if errRename := os.Rename(parentDir, movedParent); errRename != nil {
		t.Fatalf("rename parent: %v", errRename)
	}
	if errMkdir := os.Mkdir(parentDir, 0o700); errMkdir != nil {
		t.Fatalf("recreate lexical parent: %v", errMkdir)
	}
	if errWrite := os.WriteFile(filepath.Join(parentDir, "auth.json"), []byte("unrelated"), 0o600); errWrite != nil {
		t.Fatalf("write unrelated target: %v", errWrite)
	}
	if errReplace := replaceRootFile(root, "replacement.json", "auth.json"); errReplace != nil {
		t.Fatalf("replaceRootFile() error = %v", errReplace)
	}
	if data, errRead := os.ReadFile(filepath.Join(movedParent, "auth.json")); errRead != nil || string(data) != "new" {
		t.Fatalf("pinned target = %q, %v", data, errRead)
	}
	if data, errRead := os.ReadFile(filepath.Join(parentDir, "auth.json")); errRead != nil || string(data) != "unrelated" {
		t.Fatalf("replacement parent target = %q, %v", data, errRead)
	}
}
