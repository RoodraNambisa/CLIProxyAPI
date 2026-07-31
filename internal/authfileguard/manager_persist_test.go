package authfileguard

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestManagerOwnedPersistenceContext(t *testing.T) {
	if ManagerOwnedPersistence(nil) || ManagerOwnedPersistence(context.Background()) {
		t.Fatal("unmarked context reported manager ownership")
	}
	if !ManagerOwnedPersistence(WithManagerOwnedPersistence(t.Context())) {
		t.Fatal("marked context did not report manager ownership")
	}
}

func TestManagerPersistedGenerationIsPathAndHashSpecific(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chatgpt-web.json")
	MarkManagerPersistedGeneration(path, "generation-a")

	if ConsumeManagerPersistedGeneration(path, "generation-b") {
		t.Fatal("different file generation consumed the marker")
	}
	if ConsumeManagerPersistedGeneration(path, "generation-a") {
		t.Fatal("superseded file generation left a stale marker")
	}
}

func TestManagerPersistedGenerationMatchesEquivalentPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "chatgpt-web.json")
	MarkManagerPersistedGeneration(path, "generation-a")

	equivalent := filepath.Join(dir, ".", "chatgpt-web.json")
	if !ConsumeManagerPersistedGeneration(equivalent, "generation-a") {
		t.Fatal("equivalent file path did not consume the marker")
	}
}

func TestManagerPersistedGenerationMismatchDoesNotClearAnotherPath(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first.json")
	second := filepath.Join(dir, "second.json")
	MarkManagerPersistedGeneration(first, "generation-a")
	MarkManagerPersistedGeneration(second, "generation-b")

	if ConsumeManagerPersistedGeneration(first, "external-generation") {
		t.Fatal("external generation matched the first manager marker")
	}
	if !ConsumeManagerPersistedGeneration(second, "generation-b") {
		t.Fatal("observing the first path cleared the second path marker")
	}
}

func TestManagerPersistedGenerationConsumesAllSymlinkAliases(t *testing.T) {
	root := t.TempDir()
	realDir := filepath.Join(root, "real")
	if errMkdir := os.Mkdir(realDir, 0o700); errMkdir != nil {
		t.Fatalf("create real directory: %v", errMkdir)
	}
	aliasDir := filepath.Join(root, "alias")
	if errSymlink := os.Symlink(realDir, aliasDir); errSymlink != nil {
		t.Skipf("create directory symlink: %v", errSymlink)
	}
	aliasPath := filepath.Join(aliasDir, "chatgpt-web.json")
	realPath := filepath.Join(realDir, "chatgpt-web.json")
	MarkManagerPersistedGeneration(aliasPath, "generation-a")

	if !ConsumeManagerPersistedGeneration(realPath, "generation-a") {
		t.Fatal("real path did not consume the generation marked through its symlink alias")
	}
	if ConsumeManagerPersistedGeneration(aliasPath, "generation-a") {
		t.Fatal("consuming the real path left a stale symlink alias marker")
	}
}

func TestClearManagerPersistedGenerationClearsAllAliases(t *testing.T) {
	root := t.TempDir()
	realDir := filepath.Join(root, "real")
	if errMkdir := os.Mkdir(realDir, 0o700); errMkdir != nil {
		t.Fatalf("create real directory: %v", errMkdir)
	}
	aliasDir := filepath.Join(root, "alias")
	if errSymlink := os.Symlink(realDir, aliasDir); errSymlink != nil {
		t.Skipf("create directory symlink: %v", errSymlink)
	}
	aliasPath := filepath.Join(aliasDir, "chatgpt-web.json")
	realPath := filepath.Join(realDir, "chatgpt-web.json")
	MarkManagerPersistedGeneration(aliasPath, "generation-a")

	ClearManagerPersistedGeneration(realPath)

	if ConsumeManagerPersistedGeneration(aliasPath, "generation-a") {
		t.Fatal("clearing the real path left a stale symlink alias marker")
	}
}
